import { CloseCode, HandshakeSection, Protocol, PROTOCOL_CODE_MASK, PROTOCOL_MODIFIER_MASK, ProtocolModifier, type InferState, type InferInput, type NormalizeRoomType, type ExtractRoomMessages, type ExtractRoomClientMessages, type ExtractMessageType, type ExtractResponseType } from '@colyseus/shared-types';
import { decode, Decoder, encode, Iterator, Schema } from '@colyseus/schema';

import { RoomInput } from './input/RoomInput.ts';
import type { InputHandle, InputOptions } from './input/InputHandle.ts';
export { type InputHandle, type InputOptions } from './input/InputHandle.ts';

import { Packr, unpack, RESERVE_START_SPACE } from 'msgpackr';

import { Connection } from './Connection.ts';
import { getSerializer, Serializer } from './serializer/Serializer.ts';

// The unused imports here are important for better `.d.ts` file generation
// (Later merged with `dts-bundle-generator`)
import { createNanoEvents } from './core/nanoevents.ts';
import { createSignal } from './core/signal.ts';

import { SchemaConstructor, SchemaSerializer } from './serializer/SchemaSerializer.ts';

import { NULL_CLOCK, type RoomClock } from './RoomClock.ts';

import { type ReconnectionOptions, createReconnection, enqueueMessage } from './Reconnection.ts';
import {
    type OnReply,
    type RequestOptions,
    RequestAbortedError,
    RequestClosedError,
    RequestTimeoutError,
    toRequestError,
} from './RoomRequest.ts';

import { now } from './core/utils.ts';

// Infer serializer type based on State: SchemaSerializer for Schema types, Serializer otherwise
export type InferSerializer<State> = [State] extends [Schema]
    ? SchemaSerializer<State>
    : Serializer<State>;

export class Room<
    T = any,
    State = InferState<T, never>,
> {
    public roomId: string;
    public sessionId: string;
    public reconnectionToken: string;

    public name: string;
    public connection: Connection;

    // Public signals
    public onStateChange = createSignal<(state: State) => void>();
    public onError = createSignal<(code: number, message?: string) => void>();
    public onLeave = createSignal<(code: number, reason?: string) => void>();

    public onReconnect = createSignal<() => void>();
    public onDrop = createSignal<(code: number, reason?: string) => void>();

    protected onJoin = createSignal();

    public serializerId: string;
    public serializer: InferSerializer<State>;

    // reconnection logic
    public reconnection: ReconnectionOptions = createReconnection();

    protected joinedAtTime: number = 0;

    /**
     * Server-time + RTT estimator, driven by the {@link ProtocolModifier.TIMED}
     * prefix that servers emit when `defineInput()` was called.
     *
     * PRESENCE CONTRACT: `clock` is never `undefined` and always satisfies
     * {@link RoomClock} — every member, including `renderNow()`, is callable
     * with no optional chaining and no fallback.
     *
     * - Defaults to a shared frozen {@link NULL_CLOCK} so `room.clock.serverNow()`
     *   always works. The shim returns the client's own `performance.now()` and
     *   reports `0` for RTT. Rooms that never call `defineInput()` keep this
     *   stub for the whole session — zero allocation cost for chat / lobby /
     *   turn-based rooms.
     * - After handshake on an input room, a default {@link RoomClockImpl} is
     *   instantiated. Users can swap their own implementation in via
     *   `room.clock = new MyClock()` between `await joinOrCreate(...)` and
     *   the first state message (any state-message arrival is on a future
     *   microtask, so a synchronous swap is race-free). A custom clock with no
     *   slew state of its own satisfies the `renderNow` guarantee by aliasing:
     *   `renderNow() { return this.serverNow(); }`.
     */
    public clock: RoomClock = NULL_CLOCK;

    protected onMessageHandlers = createNanoEvents();

    protected packr: Packr;
    protected sharedBuffer: Uint8Array;

    #lastPingTime: number = 0;
    #pingCallback?: (ms: number) => void = undefined;

    /**
     * Default time (ms) a `room.request()` / `room.send(..., callback)` waits
     * for a reply before rejecting. Class-level default — tune globally via
     * `Room.defaultRequestTimeout = ms`; override per-call with the `timeout` option.
     */
    static defaultRequestTimeout = 10000;

    /** Monotonic id correlating a {@link Protocol.ROOM_REQUEST} with its reply. @internal */
    #nextRequestId: number = 0;

    /**
     * Bumped on every transport transition: `connect()`, reconnect
     * (`onopen`) and `onclose`. A pending entry snapshots the epoch it was
     * SENT on; a ROOM_RESPONSE whose epoch differs from the current one is a
     * late frame from a dead socket and is dropped — after a reconnect the
     * monotonic id counter may have wrapped (or a fresh process restarted it
     * at 0), so the raw requestId alone could collide and resolve a request
     * that belongs to the new connection.
     * @internal
     */
    #connectionEpoch = 0;

    /**
     * In-flight round-trips awaiting a {@link Protocol.ROOM_RESPONSE}, keyed by the
     * monotonic request id. Each entry snapshots {@link #connectionEpoch} and
     * carries:
     *
     * - `onReply` — called exactly once with `(status, payload)` when the
     *   server answers on the SAME epoch;
     * - `onClose` (optional) — invoked ONLY on a transport-level disconnect
     *   with a {@link RequestClosedError}. A local timeout/abort removes the
     *   entry through {@link cancelRequest} WITHOUT firing this (the request
     *   promise rejects with its own specific error); its absence drops the
     *   registration silently (the predict layer's TTL path).
     *
     * Both reply and close go through the single settlement guards, so the
     * registry entry, the timeout, and the abort listener are released by a
     * single code path — a late double-event can never settle twice.
     * @internal
     */
    #pending = new Map<number, {
        epoch: number;
        onReply: OnReply;
        onClose?: (error: RequestClosedError) => void;
    }>();

    /**
     * Per-room input state — schema ctor, server-advertised stamp/rate flags,
     * and the cached {@link InputHandle} — all grouped in {@link RoomInput}.
     * Lazily created only when the room declares input (an `INPUT_*` handshake
     * section arrives) or `input()` is called; stays `undefined` for chat /
     * lobby / turn-based rooms — zero allocation.
     * @internal
     */
    #input?: RoomInput;

    /**
     * Seq of the newest {@link ProtocolModifier.UNRELIABLE} patch applied, so a
     * reordered datagram can be dropped rather than write a stale value. Wraps
     * at 65536; `0` is the post-`ROOM_STATE` baseline. Stays `0` forever on
     * rooms whose state declares no `@unreliable` field.
     * @internal
     */
    #lastUnreliableSeq = 0;

    constructor(name: string, rootSchema?: SchemaConstructor<State>) {
        this.name = name;

        this.packr = new Packr();
        this.sharedBuffer = new Uint8Array(8192);

        if (rootSchema) {
            const serializer: SchemaSerializer = new (getSerializer("schema"));
            this.serializer = serializer;

            const state: State = new rootSchema();
            serializer.state = state;
            serializer.decoder = new Decoder(state as Schema);
        }

        this.onLeave(() => {
            this.removeAllListeners();
            this.destroy();
        });
    }

    public connect(endpoint: string, options?: any, headers?: any) {
        this.connection = new Connection(options.protocol);

        // New transport — every frame in flight belongs to the old one.
        // Rejecting below also invalidates them; the epoch bump makes any
        // ROOM_RESPONSE that still drains out of the dying socket unmatchable.
        this.bumpConnectionEpoch();

        this.connection.events.onmessage = this.onMessageCallback.bind(this);
        this.connection.events.onopen = () => {
            // (Re)connected transport: late replies that arrive BEFORE this
            // event would carry the prior epoch and are dropped.
            this.bumpConnectionEpoch();
        };
        this.connection.events.onclose = (e: CloseEvent) => {
            // The in-flight requests can't be answered on a closed socket.
            // Bump first so a response buffered behind the close in the same
            // task can't resolve a (theoretical) post-close registration.
            this.bumpConnectionEpoch();
            this.rejectAllPending(new RequestClosedError(
                "connection closed before a response was received.",
                e?.code,
            ));

            if (this.joinedAtTime === 0) {
                console.warn?.(`Room connection was closed unexpectedly (${e.code}): ${e.reason}`);
                this.onError.invoke(e.code, e.reason);
                return;
            }

            if (
                e.code === CloseCode.NO_STATUS_RECEIVED ||
                e.code === CloseCode.ABNORMAL_CLOSURE ||
                e.code === CloseCode.GOING_AWAY ||
                e.code === CloseCode.MAY_TRY_RECONNECT
            ) {
                this.onDrop.invoke(e.code, e.reason);
                this.handleReconnection(e.code, e.reason);

            } else {
                this.onLeave.invoke(e.code, e.reason);
            }
        };

        this.connection.events.onerror = (e: CloseEvent) => {
            this.onError.invoke(e.code, e.reason);
        };

        /**
         * if local serializer has state, it means we don't need to receive the
         * handshake from the server
         */
        const skipHandshake = (this.serializer?.getState() !== undefined);

        if (options.protocol === "h3") {
            // FIXME: refactor this.
            const url = new URL(endpoint);
            this.connection.connect(url.origin, { ...options, skipHandshake });

        } else {
            this.connection.connect(`${endpoint}${skipHandshake ? "&skipHandshake=1" : ""}`, headers);
        }

    }

    public leave(consented: boolean = true): Promise<number> {
        // Lifecycle boundary: a leaving room settles no more requests. Reject
        // up front (rather than waiting for the socket's onclose) so callers
        // observing `leave()` completion never race a still-pending promise.
        this.rejectAllPending(new RequestClosedError("room left before a response was received."));

        return new Promise((resolve) => {
            this.onLeave((code) => resolve(code));

            if (this.connection) {
                if (consented) {
                    this.sharedBuffer[0] = Protocol.LEAVE_ROOM;
                    this.connection.send(this.sharedBuffer.subarray(0, 1));

                } else {
                    this.connection.close();
                }

            } else {
                this.onLeave.invoke(CloseCode.CONSENTED);
            }
        });
    }

    public onMessage<MessageType extends keyof ExtractRoomClientMessages<NormalizeRoomType<T>>>(
        message: MessageType,
        callback: (payload: ExtractRoomClientMessages<NormalizeRoomType<T>>[MessageType]) => void
    ): () => void
    public onMessage<Payload = any>(type: "*", callback: (messageType: string | number, payload: Payload) => void): () => void
    // Fallback overload: only available when no typed client messages are defined
    public onMessage<Payload = any>(
        type: [keyof ExtractRoomClientMessages<NormalizeRoomType<T>>] extends [never] ? (string | number) : never,
        callback: (payload: Payload) => void
    ): () => void
    public onMessage(type: '*' | string | number, callback: (...args: any[]) => void) {
        return this.onMessageHandlers.on(this.getMessageHandlerKey(type), callback);
    }

    public ping(callback: (ms: number) => void) {
        // skip if connection is not open
        if (!this.connection?.isOpen) {
            return;
        }

        this.#lastPingTime = now();
        this.#pingCallback = callback;
        this.sharedBuffer[0] = Protocol.PING;
        this.connection.send(this.sharedBuffer.subarray(0, 1));
    }

    public send<MessageType extends keyof ExtractRoomMessages<NormalizeRoomType<T>>>(
        messageType: MessageType,
        payload?: ExtractMessageType<ExtractRoomMessages<NormalizeRoomType<T>>[MessageType]>
    ): void
    // Request overload: passing a callback turns this into a request/response —
    // the callback receives the value the server handler returns (or an Error).
    public send<MessageType extends keyof ExtractRoomMessages<NormalizeRoomType<T>>>(
        messageType: MessageType,
        payload: ExtractMessageType<ExtractRoomMessages<NormalizeRoomType<T>>[MessageType]>,
        callback: (response: ExtractResponseType<ExtractRoomMessages<NormalizeRoomType<T>>[MessageType]>, error?: Error) => void
    ): void
    // Fallback overload: only available when no typed messages are defined
    public send<Payload = any>(
        messageType: [keyof ExtractRoomMessages<NormalizeRoomType<T>>] extends [never] ? (string | number) : never,
        payload?: Payload
    ): void
    // Fallback request overload
    public send<Payload = any, Response = any>(
        messageType: [keyof ExtractRoomMessages<NormalizeRoomType<T>>] extends [never] ? (string | number) : never,
        payload: Payload,
        callback: (response: Response, error?: Error) => void
    ): void
    public send(messageType: string | number, payload?: any, callback?: (response: any, error?: Error) => void): void {
        // Request/response form: defer to `request()` and adapt to a
        // (response, error) callback.
        if (callback !== undefined) {
            this.request(messageType as any, payload).then(
                (response) => callback(response, undefined),
                (error) => callback(undefined, error),
            );
            return;
        }

        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_DATA;

        if (typeof(messageType) === "string") {
            encode.string(this.sharedBuffer, messageType, it);

        } else {
            encode.number(this.sharedBuffer, messageType, it);
        }
        const headerLength = it.offset;

        let data: Uint8Array;
        if (payload !== undefined) {
            // Reserve `headerLength` writable bytes at the front of msgpackr's
            // output and prepend the protocol header into them.
            data = this.packr.pack(payload, RESERVE_START_SPACE | headerLength);
            data.set(this.sharedBuffer.subarray(0, headerLength), 0);
        } else {
            data = this.sharedBuffer.subarray(0, headerLength);
        }

        // If connection is not open, buffer the message
        if (!this.connection.isOpen) {
            enqueueMessage(this, new Uint8Array(data));
        } else {
            this.connection.send(data);
        }
    }

    /**
     * Send a message and await the server's reply. The server answers by
     * returning a value from its matching `onMessage(type, ...)` handler.
     *
     * The promise rejects with a discriminated {@link RequestError} so callers
     * can distinguish every failure mode via `error.kind`:
     *
     * - `"timeout"` ({@link RequestTimeoutError}) — no reply within
     *   `options.timeout` (default {@link Room.defaultRequestTimeout});
     * - `"aborted"` ({@link RequestAbortedError}) — `options.signal` aborted
     *   (or {@link cancelRequest} ran);
     * - `"closed"` ({@link RequestClosedError}) — the transport closed (leave,
     *   drop, dispose) before a reply;
     * - `"rejected"` / `"error"` ({@link RequestRejectedError} /
     *   {@link RequestFailedError}) — a deliberate `ctx.reject(reason)` (read
     *   `.reason`) vs. a handler fault (throw / no handler);
     * - `"busy"` / `"duplicate"` ({@link RequestBusyError} /
     *   {@link RequestDuplicateError}) — the server's pending cap or duplicate
     *   policy refused the request before the handler ran.
     *
     * @example
     * ```typescript
     * const profile = await room.request("get-profile", { id: 42 });
     * ```
     */
    public request<MessageType extends keyof ExtractRoomMessages<NormalizeRoomType<T>>>(
        messageType: MessageType,
        payload?: ExtractMessageType<ExtractRoomMessages<NormalizeRoomType<T>>[MessageType]>,
        options?: RequestOptions
    ): Promise<ExtractResponseType<ExtractRoomMessages<NormalizeRoomType<T>>[MessageType]>>
    public request<Payload = any, Response = any>(
        messageType: [keyof ExtractRoomMessages<NormalizeRoomType<T>>] extends [never] ? (string | number) : never,
        payload?: Payload,
        options?: RequestOptions
    ): Promise<Response>
    public request(messageType: string | number, payload?: any, options?: RequestOptions): Promise<any> {
        if (!this.connection.isOpen) {
            return Promise.reject(new RequestClosedError(
                `cannot send request "${messageType}": connection is not open.`,
            ));
        }

        // An already-aborted signal fails fast: nothing is transmitted and no
        // pending registration is created.
        if (options?.signal?.aborted) {
            return Promise.reject(new RequestAbortedError(messageType, (options.signal as any).reason));
        }

        // `timeout === false` (or Infinity) disables the timer entirely — the
        // caller then owns cancellation through `signal` / cancelRequest().
        const timeoutOption = options?.timeout ?? Room.defaultRequestTimeout;
        const timeoutMs = (timeoutOption === false || timeoutOption === Infinity) ? undefined : timeoutOption;

        // request = sendRequest + fail-fast-offline (above) + promise + timer +
        // abort wiring. ALL terminal paths (reply / timeout / abort / close)
        // converge on the single `settled` guard, so exactly one outcome wins.
        return new Promise((resolve, reject) => {
            let timer: ReturnType<typeof setTimeout> | undefined;
            let requestId: number;

            const cleanup = () => {
                if (timer !== undefined) { clearTimeout(timer); timer = undefined; }
                options?.signal?.removeEventListener("abort", onAbort);
            };

            // Local abandon (timeout / abort signal): unregister + notify the
            // server, but DON'T fire the entry's `onClose` — that callback is
            // reserved for transport-level closes. This promise owns the
            // rejection with the specific error; a fired onClose would race it
            // and win with a generic RequestClosedError.
            const abandon = (error: Error, notifyServer: boolean) => {
                if (!this.#pending.has(requestId)) { return; }
                this.#removePending(requestId, notifyServer);
                cleanup();
                reject(error);
            };

            const onAbort = () => abandon(
                new RequestAbortedError(messageType, (options!.signal as any).reason),
                true,
            );

            requestId = this.sendRequest(
                messageType, payload,
                { mode: options?.mode, requestId: options?.requestId },
                (status, replyPayload) => {
                    // dispatch deleted the entry before firing; onReply fires
                    // at most once per registration.
                    cleanup();
                    const error = toRequestError(status, replyPayload);
                    if (error !== undefined) { reject(error); } else { resolve(replyPayload); }
                },
                (error) => { cleanup(); reject(error); },
            );

            // An unreliable send that couldn't be transmitted registers no
            // waitable round-trip: fail fast rather than leave it to time out.
            if (requestId === -1) {
                reject(new RequestClosedError(
                    `cannot send request "${messageType}": connection is not open.`,
                ));
                return;
            }

            options?.signal?.addEventListener("abort", onAbort, { once: true });

            if (timeoutMs !== undefined) {
                timer = setTimeout(() => {
                    abandon(new RequestTimeoutError(messageType, timeoutMs), true);
                }, timeoutMs);
            }
        });
    }

    /** Next correlation id (uint32) for a round-trip. @internal */
    #mintRequestId(): number {
        const id = this.#nextRequestId;
        this.#nextRequestId = (this.#nextRequestId + 1) >>> 0; // keep within uint32
        return id;
    }

    /** Encode a {@link Protocol.ROOM_REQUEST} frame (shared by `request` + `sendRequest`).
     *  The returned buffer aliases `sharedBuffer` for the payload-less case, so
     *  it must be transmitted (or copied) before the next encode. @internal */
    #encodeRequestFrame(requestId: number, messageType: string | number, payload: any): Uint8Array {
        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_REQUEST;
        encode.number(this.sharedBuffer, requestId, it);

        if (typeof(messageType) === "string") {
            encode.string(this.sharedBuffer, messageType, it);
        } else {
            encode.number(this.sharedBuffer, messageType, it);
        }
        const headerLength = it.offset;

        if (payload !== undefined) {
            const data = this.packr.pack(payload, RESERVE_START_SPACE | headerLength);
            data.set(this.sharedBuffer.subarray(0, headerLength), 0);
            return data;
        }
        return this.sharedBuffer.subarray(0, headerLength);
    }

    /** Encode a {@link Protocol.ROOM_REQUEST_CANCEL} frame: header-only, shares
     *  the scratch buffer, so it is copied/sent inline. @internal */
    #encodeCancelFrame(requestId: number): Uint8Array {
        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_REQUEST_CANCEL;
        encode.number(this.sharedBuffer, requestId, it);
        return new Uint8Array(this.sharedBuffer.subarray(0, it.offset));
    }

    /**
     * Low-level round-trip primitive: register `onReply` (called once with the
     * decoded `(status, payload)` when the server replies on the same
     * connection epoch) and transmit a {@link Protocol.ROOM_REQUEST} (`mode`
     * picks the channel; `requestId` optionally pins the correlation id for
     * server-side idempotency). {@link request} wraps it with a promise,
     * timeout, and abort support. `onClose` (optional) is invoked ONLY on a
     * transport-level disconnect with a {@link RequestClosedError} — local
     * cancels (timeout/abort) settle their own promises and never fire it.
     * Omit `onClose` to drop the registration silently. Returns the request
     * id, or `-1` if an unreliable send couldn't be transmitted (offline).
     * @internal
     */
    protected sendRequest(
        messageType: string | number,
        payload: any,
        opts: { mode?: "reliable" | "unreliable", requestId?: number },
        onReply: OnReply,
        onClose?: (error: RequestClosedError) => void,
    ): number {
        const requestId = opts.requestId ?? this.#mintRequestId();

        const data = this.#encodeRequestFrame(requestId, messageType, payload);
        if (opts.mode === "unreliable") {
            if (!this.connection.isOpen) { return -1; }
            // Sent once — no unreliable channel falls back to reliable inside the
            // transport.
            this.connection.sendUnreliable(data);
        } else if (this.connection.isOpen) {
            this.connection.send(data);
        } else {
            // Reliable + offline: buffer so it flushes on (re)connect. The
            // entry snapshots the CURRENT epoch; see #connectionEpoch.
            enqueueMessage(this, new Uint8Array(data));
        }

        this.#pending.set(requestId, { epoch: this.#connectionEpoch, onReply, onClose });
        return requestId;
    }

    /**
     * Abandon a pending round-trip through a LOCAL decision (timeout, abort
     * signal, or the predict layer's TTL path): remove the registration and,
     * when `notifyServer` is set and the transport is open, send a
     * {@link Protocol.ROOM_REQUEST_CANCEL} so the server frees its pending
     * slot / aborts the handler's signal. Unlike a transport close this does
     * NOT invoke the entry's `onClose` — a local cancel is the caller's own
     * decision and the {@link request} promise rejects with the specific
     * timeout/abort error, not a generic closed error. Idempotent. @internal
     */
    protected cancelRequest(id: number, notifyServer: boolean = false): void {
        this.#removePending(id, notifyServer);
    }

    /** Shared unregister + optional CANCEL frame. Does NOT fire `onClose`
     *  (transport-close-only); callers settle the local promise themselves.
     *  @internal */
    #removePending(id: number, notifyServer: boolean): void {
        const entry = this.#pending.get(id);
        if (entry === undefined) { return; }
        this.#pending.delete(id);

        if (notifyServer && this.connection?.isOpen) {
            // Best effort — a lost CANCEL only costs the server a slot until
            // the handler settles.
            this.connection.send(this.#encodeCancelFrame(id));
        }
    }

    /** Bump the connection epoch — invalidates every in-flight registration's
     *  ability to match a reply (used together with {@link rejectAllPending}).
     *  Protected (rather than private) as a transport-lifecycle seam shared
     *  with the reconnect path and the test harness. @internal */
    protected bumpConnectionEpoch(): void {
        this.#connectionEpoch = (this.#connectionEpoch + 1) >>> 0;
    }

    /** Reject every pending request with `error` (transport close / leave /
     *  dispose). Request entries reject; entries without an `onClose`
     *  (the predict layer) drop silently. @internal */
    protected rejectAllPending(error: RequestClosedError) {
        if (this.#pending.size === 0) { return; }
        for (const entry of this.#pending.values()) { entry.onClose?.(error); }
        this.#pending.clear();
    }

    public sendUnreliable<T = any>(type: string | number, message?: T): void {
        // If connection is not open, skip
        if (!this.connection.isOpen) { return; }

        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_DATA;

        if (typeof(type) === "string") {
            encode.string(this.sharedBuffer, type, it);

        } else {
            encode.number(this.sharedBuffer, type, it);
        }
        const headerLength = it.offset;

        let data: Uint8Array;
        if (message !== undefined) {
            data = this.packr.pack(message, RESERVE_START_SPACE | headerLength);
            data.set(this.sharedBuffer.subarray(0, headerLength), 0);
        } else {
            data = this.sharedBuffer.subarray(0, headerLength);
        }

        this.connection.sendUnreliable(data);
    }

    public sendBytes(type: string | number, bytes: Uint8Array) {
        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_DATA_BYTES;

        if (typeof(type) === "string") {
            encode.string(this.sharedBuffer, type, it);

        } else {
            encode.number(this.sharedBuffer, type, it);
        }
        const headerLength = it.offset;

        // grow the scratch buffer if needed, preserving the header bytes
        if (headerLength + bytes.byteLength > this.sharedBuffer.byteLength) {
            const newBuffer = new Uint8Array(headerLength + bytes.byteLength);
            newBuffer.set(this.sharedBuffer.subarray(0, headerLength));
            this.sharedBuffer = newBuffer;
        }

        this.sharedBuffer.set(bytes, headerLength);

        // If connection is not open, buffer the message
        if (!this.connection.isOpen) {
            enqueueMessage(this, this.sharedBuffer.subarray(0, headerLength + bytes.byteLength));
        } else {
            this.connection.send(this.sharedBuffer.subarray(0, headerLength + bytes.byteLength));
        }

    }

    /**
     * Get the per-room input handle. Lazily created on first call and cached;
     * subsequent calls return the same handle (options on later calls are
     * ignored — a warning fires once if they differ from the handle's config).
     *
     * Schema discovery, in order:
     * 1. `options.type` — explicit constructor (overrides everything).
     * 2. Server-sent reflection from the JOIN handshake — populated when the
     *    server room called `defineInput()`. The synthesized class has the
     *    same fields as the server's input schema; `instanceof YourInput`
     *    won't pass on it.
     *
     * Throws if neither source has produced a constructor.
     *
     * Inputs are always delta-encoded; every `send()` transmits one input
     * (a body-less frame when nothing changed). For rollback netcode, prefer
     * `{ mode: "unreliable", historySize: 4 }`: tiny per-tick payloads,
     * redundancy across drops, idempotent under reordering.
     *
     * @example
     * ```typescript
     * const room = await client.joinOrCreate<typeof FpsRoom>("fps");
     * const input = room.input({ mode: "unreliable" });   // type from server
     * // each simulation tick:
     * input.data.seq++;
     * input.data.vx = vx;
     * input.data.vy = vy;
     * input.send();
     * ```
     */
    public input<
        I = ([InferInput<T>] extends [never] ? any : InferInput<T>),
    >(options?: InputOptions<I>): InputHandle<I> {
        return (this.#input ??= new RoomInput(this)).handle(options);
    }

    public get state (): State {
        return this.serializer.getState();
    }

    public removeAllListeners() {
        this.onJoin.clear();
        this.onStateChange.clear();
        this.onError.clear();
        this.onLeave.clear();
        this.onReconnect.clear();
        this.onDrop.clear();
        this.onMessageHandlers.events = {};

        if (this.serializer instanceof SchemaSerializer) {
            // Remove callback references
            this.serializer.decoder.root.callbacks = {};
        }
    }

    protected onMessageCallback(event: MessageEvent) {
        this.#dispatchFrame(new Uint8Array(event.data));
    }

    /** Decode + dispatch a single protocol frame. */
    #dispatchFrame(buffer: Uint8Array) {
        const it: Iterator = { offset: 1 };
        // Strip modifier bits (e.g. ProtocolModifier.TIMED). Consume any
        // modifier-attached prefix bytes here so the dispatch tree below
        // stays modifier-agnostic.
        const rawByte = buffer[0];
        const code = rawByte & PROTOCOL_CODE_MASK;
        if (rawByte & ProtocolModifier.TIMED) {
            // [uint32 sNow][uint32 inputSeq]  — sNow = ms since room start
            // (clock.elapsedTime); inputSeq = last PROCESSED input.
            //
            // Routing: the INPUT ack goes to the input handle (it owns the
            // round-trip — what you sent, what's acked); it returns an RTT
            // sample which, with sNow, feeds the time-only clock. `decode.*`
            // advance `it.offset`; read in declared byte order.
            const sNow = decode.uint32(buffer as Buffer, it);
            const inputSeq = decode.uint32(buffer as Buffer, it);
            const rttSample = this.#input ? this.#input.ackInput(inputSeq) : -1;
            this.clock.sample(sNow, rttSample);
        }

        if (code === Protocol.JOIN_ROOM) {
            const reconnectionToken = decode.utf8Read(buffer as Buffer, it, buffer[it.offset++]);
            this.serializerId = decode.utf8Read(buffer as Buffer, it, buffer[it.offset++]);

            // Instantiate serializer if not locally available.
            if (!this.serializer) {
                const serializer = getSerializer(this.serializerId);
                this.serializer = new serializer();
            }

            // State reflection is length-prefixed (varint). The schema decoder
            // runs `while (offset < bytes.byteLength)` so without a boundary
            // it would read past the state reflection into the trailing
            // tagged-section bytes — see Protocol.ts for the wire layout.
            const stateReflectionLen = decode.number(buffer as Buffer, it);
            if (stateReflectionLen > 0 && this.serializer.handshake) {
                const stateReflectionEnd = it.offset + stateReflectionLen;
                this.serializer.handshake(buffer.subarray(0, stateReflectionEnd), it);
                it.offset = stateReflectionEnd;
            }

            // Parse trailing tagged sections (forward-compatible: unknown tags
            // are skipped via length). See HandshakeSection in shared-types.
            while (it.offset < buffer.byteLength) {
                const tag = buffer[it.offset++];
                const sectionLen = decode.number(buffer as Buffer, it);
                const sectionEnd = it.offset + sectionLen;

                if (tag === HandshakeSection.INPUT_REFLECTION) {
                    (this.#input ??= new RoomInput(this)).applyReflection(buffer, it, sectionEnd);

                } else if (tag === HandshakeSection.INPUT_OPTIONS) {
                    (this.#input ??= new RoomInput(this)).applyOptions(buffer, it);
                }

                it.offset = sectionEnd;
            }

            // Hand the snapshot cadence to the clock (sections decode in any
            // order, so do it once the loop has both the clock and patchRate) —
            // interpolation reads it to tell an idle delta-encoded gap from the
            // normal patch interval.
            const patchRate = this.#input?.patchRate;
            if (patchRate !== undefined) {
                this.clock.setPatchInterval?.(patchRate);
            }

            if (this.joinedAtTime === 0) {
                this.joinedAtTime = Date.now();
                this.onJoin.invoke();

            } else {
                console.info(`[Colyseus reconnection]: ${String.fromCodePoint(0x2705)} reconnection successful!`); // ✅
                this.reconnection.isReconnecting = false;
                this.onReconnect.invoke();
            }

            this.reconnectionToken = `${this.roomId}:${reconnectionToken}`;

            // Acknowledge JOIN_ROOM.
            this.sharedBuffer[0] = Protocol.JOIN_ROOM;
            this.connection.send(this.sharedBuffer.subarray(0, 1));

            // Send any enqueued messages that were buffered while disconnected
            if (this.reconnection.enqueuedMessages.length > 0) {
                for (const message of this.reconnection.enqueuedMessages) {
                    this.connection.send(message.data);
                }
                // Clear the buffer after sending
                this.reconnection.enqueuedMessages = [];
            }

        } else if (code === Protocol.ERROR) {
            const code = decode.number(buffer as Buffer, it);
            const message = decode.string(buffer as Buffer, it);

            this.onError.invoke(code, message);

        } else if (code === Protocol.LEAVE_ROOM) {
            this.leave();

        } else if (code === Protocol.ROOM_STATE) {
            // Full state re-baselines the unreliable timeline (join / resync).
            this.#lastUnreliableSeq = 0;
            this.serializer.setState(buffer, it);
            this.onStateChange.invoke(this.serializer.getState());

        } else if (code === Protocol.ROOM_STATE_PATCH) {
            // The unreliable channel can reorder. `@unreliable` fields carry
            // absolute values, so a late frame would write a stale one that
            // survives until the field changes again — drop it instead.
            if (rawByte & ProtocolModifier.UNRELIABLE) {
                const seq = decode.uint16(buffer as Buffer, it);
                // wrap-safe at 65536: a positive int16 delta means newer
                if ((((seq - this.#lastUnreliableSeq) << 16) >> 16) <= 0) { return; }
                this.#lastUnreliableSeq = seq;
            }

            this.serializer.patch(buffer, it);
            this.onStateChange.invoke(this.serializer.getState());

        } else if (code === Protocol.ROOM_DATA) {
            const type = (decode.stringCheck(buffer as Buffer, it))
                ? decode.string(buffer as Buffer, it)
                : decode.number(buffer as Buffer, it);

            const message = (buffer.byteLength > it.offset)
                ? unpack(buffer as Buffer, { start: it.offset })
                : undefined;

            this.dispatchMessage(type, message);

        } else if (code === Protocol.ROOM_DATA_BYTES) {
            const type = (decode.stringCheck(buffer as Buffer, it))
                ? decode.string(buffer as Buffer, it)
                : decode.number(buffer as Buffer, it);

            this.dispatchMessage(type, buffer.subarray(it.offset));

        } else if (code === Protocol.ROOM_RESPONSE) {
            // reply to a pending `request()` / `send(..., callback)`.
            const requestId = decode.number(buffer as Buffer, it);
            const status = buffer[it.offset++];
            const payload = (buffer.byteLength > it.offset)
                ? unpack(buffer as Buffer, { start: it.offset })
                : undefined;

            const entry = this.#pending.get(requestId);
            if (entry === undefined) {
                // already answered (timed out / cancelled / closed) or an
                // unknown id — ignore.
            } else if (entry.epoch !== this.#connectionEpoch) {
                // Late response from a previous transport (its socket dropped
                // but this frame drained first). Do NOT remove the entry: it
                // still owns a live promise that the transport's own close
                // handler will reject. (Normally unreachable — the epoch bump
                // and rejection are atomic in the same onclose task.)
            } else {
                // Same epoch: this is the ONE place the wire status maps to
                // resolve/reject, and the entry's single terminal point.
                this.#pending.delete(requestId);
                entry.onReply(status, payload);
            }

        } else if (code === Protocol.PING) {
            this.#pingCallback?.(Math.round(now() - this.#lastPingTime));
            this.#pingCallback = undefined;
        }
    }

    private dispatchMessage(type: string | number, message: any) {
        const messageType = this.getMessageHandlerKey(type);

        if (this.onMessageHandlers.events[messageType]) {
            this.onMessageHandlers.emit(messageType, message);

        } else if (this.onMessageHandlers.events['*']) {
            this.onMessageHandlers.emit('*', type, message);

        } else if (!messageType.startsWith("__")) { // ignore internal messages
            console.warn?.(`@colyseus/sdk: onMessage() not registered for type '${type}'.`);
        }
    }

    private destroy () {
        // Lifecycle safety net: onLeave→removeAllListeners→destroy runs on
        // every termination path; make sure no round-trip outlives the room
        // even if the socket close event never fires.
        this.rejectAllPending(new RequestClosedError("room disposed before a response was received."));

        if (this.serializer) {
            this.serializer.teardown();
        }
    }

    private getMessageHandlerKey(type: string | number): string {
        switch (typeof(type)) {
            // string
            case "string": return type;

            // number
            case "number": return `i${type}`;

            default: throw new Error("invalid message type.");
        }
    }

    private handleReconnection(code: number, reason?: string) {
        if (!this.reconnection.enabled) {
            this.onLeave.invoke(code, reason);
            return;
        }

        if (Date.now() - this.joinedAtTime < this.reconnection.minUptime) {
            console.info(`[Colyseus reconnection]: ${String.fromCodePoint(0x274C)} Room has not been up for long enough for automatic reconnection. (min uptime: ${this.reconnection.minUptime}ms)`); // ❌
            this.onLeave.invoke(CloseCode.ABNORMAL_CLOSURE, "Room uptime too short for reconnection.");
            return;
        }

        if (!this.reconnection.isReconnecting) {
            this.reconnection.retryCount = 0;
            this.reconnection.isReconnecting = true;
            // The server allocates a FRESH input buffer for the reconnected client
            // (its consumed counter restarts at 0). Zero ours now so post-reconnect
            // seqs line up — otherwise every ack echo (≤ the old counter) is
            // discarded until the new counter catches up past it, and `sentCount`
            // stays permanently ahead. Reconcilers follow this reset on their own
            // (they poll the handle's `epoch`) — no `onReconnect` wiring needed.
            this.#input?.reset();
        }

        this.retryReconnection();
    }

    private retryReconnection() {
        if (this.reconnection.retryCount >= this.reconnection.maxRetries) {
            // No more retries
            console.info(`[Colyseus reconnection]: ${String.fromCodePoint(0x274C)} ❌ Reconnection failed after ${this.reconnection.maxRetries} attempts.`); // ❌
            this.reconnection.isReconnecting = false;
            this.onLeave.invoke(CloseCode.FAILED_TO_RECONNECT, "No more retries. Reconnection failed.");
            return;
        }

        this.reconnection.retryCount++;

        const delay = Math.min(this.reconnection.maxDelay, Math.max(this.reconnection.minDelay, this.reconnection.backoff(this.reconnection.retryCount, this.reconnection.delay)));
        console.info(`[Colyseus reconnection]: ${String.fromCodePoint(0x023F3)} will retry in ${(delay/1000).toFixed(1)} seconds...`); // 🔄

        // Wait before attempting reconnection
        setTimeout(() => {
            try {
                console.info(`[Colyseus reconnection]: ${String.fromCodePoint(0x1F504)} Re-establishing sessionId '${this.sessionId}' with roomId '${this.roomId}'... (attempt ${this.reconnection.retryCount} of ${this.reconnection.maxRetries})`); // 🔄
                this.connection.reconnect({
                    reconnectionToken: this.reconnectionToken.split(":")[1],
                    skipHandshake: true, // we already applied the handshake on first join
                });

            } catch (e) {
                this.retryReconnection();
            }
        }, delay);
    }
}
