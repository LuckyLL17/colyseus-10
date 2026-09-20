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
import { type OnReply, type RequestOptions, RequestTimeoutError, RequestClosedError, DuplicateRequestError, RequestCapacityError, toAbortError, toResponseError } from './RoomRequest.ts';

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
     * Bumped on every transport `onclose`. Every pending round-trip captures the
     * epoch it was registered under: a ROOM_RESPONSE that arrives on a NEW
     * connection (reconnect after a drop) can never resolve a request issued on
     * the old one — even if its monotonic id happens to be reused by a fresh
     * request. `#pending` is also rejected + cleared on close, so this is
     * defense-in-depth, not the primary guard. @internal
     */
    #connectionEpoch: number = 0;

    /**
     * In-flight round-trips awaiting a {@link Protocol.ROOM_RESPONSE}, keyed by the
     * monotonic request id. `onReply` receives the raw wire `(status, payload)`;
     * `onClose` (optional) rejects on disconnect — its absence drops the
     * registration silently. `epoch` pins the entry to the connection that
     * carried the request (see {@link #connectionEpoch}). @internal
     */
    #pending = new Map<number, {
        onReply: OnReply;
        onClose?: (code?: number, reason?: string) => void;
        epoch: number;
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
        this.connection.events.onmessage = this.onMessageCallback.bind(this);
        this.connection.events.onclose = (e: CloseEvent) => {
            // the in-flight requests can't be answered on a closed socket.
            // Bump the epoch BEFORE rejecting so late frames delivered on the
            // (possibly reconnected) next connection can't resolve these ids.
            this.#connectionEpoch++;
            this.#rejectAllPending(e.code, e.reason);

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
        // Nothing in flight can be answered after we leave — reject + clear now
        // rather than waiting for the server's socket close (which, for a
        // consented leave, only arrives after its onLeave tick). Idempotent with
        // the onclose handler: whichever runs first clears the map.
        this.#rejectAllPending(
            consented ? CloseCode.CONSENTED : CloseCode.GOING_AWAY,
            "room left before a response was received.",
        );

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
     * Failure classes are distinguishable by error type so callers can retry /
     * rethrow / back off appropriately:
     *
     * - **business error** — handler threw / no handler (`faulted` Error with
     *   the server's `name`/`message`/`code`) or the handler called
     *   `ctx.reject(reason)` (`err.name === "rejected"`, `err.reason` set).
     * - {@link RequestCapacityError} / {@link DuplicateRequestError} — the room
     *   refused the request before the handler ran (`maxPendingRequests` full /
     *   duplicate id policy).
     * - {@link RequestTimeoutError} (`err.name === "TimeoutError"`) — no reply
     *   within `timeout` (default {@link Room.defaultRequestTimeout}).
     * - {@link AbortError} — cancelled via `options.signal`.
     * - {@link RequestClosedError} — the transport closed while awaiting
     *   (leave / drop / server shutdown). Reconnection attempts do not resume
     *   the request.
     *
     * @example
     * ```typescript
     * const profile = await room.request("get-profile", { id: 42 });
     * ```
     *
     * Cancellation:
     * ```typescript
     * const controller = new AbortController();
     * const promise = room.request("slow", {}, { signal: controller.signal });
     * controller.abort(); // rejects with AbortError; server slot is released
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
            return Promise.reject(new Error(`cannot send request "${messageType}": connection is not open.`));
        }

        // Pre-aborted signal: reject before encoding/transmitting anything.
        if (options?.signal?.aborted) {
            return Promise.reject(toAbortError(options.signal));
        }

        const timeoutMs = options?.timeout ?? Room.defaultRequestTimeout;
        const signal = options?.signal;
        // request = sendRequest + fail-fast-offline (above) + promise + timeout +
        // signal. The timer and signal listener live in this closure, so the
        // shared #pending registry stays unaware of them; the reply callback and
        // onClose both tear them down.
        return new Promise((resolve, reject) => {
            let timer: ReturnType<typeof setTimeout>;
            let settled = false;

            const teardown = () => {
                clearTimeout(timer);
                signal?.removeEventListener("abort", onAbort);
            };

            const onAbort = () => {
                if (settled) { return; }
                settled = true;
                // Best-effort server-side release: frees the pending slot and
                // aborts the handler's ctx.signal. Safe while offline — the
                // frame just fails to transmit (cancel is not enqueued).
                this.#sendCancelFrame(id);
                this.cancelRequest(id);
                teardown();
                reject(toAbortError(signal));
            };

            const id = this.sendRequest(
                messageType, payload, { mode: options?.mode },
                (status, replyPayload) => {
                    if (settled) { return; } // timeout/abort/close won the race — drop the late reply
                    settled = true;
                    teardown();
                    const error = toResponseError(status, replyPayload);
                    if (error !== undefined) { reject(error); }
                    else { resolve(replyPayload); }
                },
                (code, reason) => {
                    if (settled) { return; }
                    settled = true;
                    teardown();
                    reject(new RequestClosedError(
                        reason
                            ? `connection closed before a response was received: ${reason}`
                            : "connection closed before a response was received.",
                        code, reason,
                    ));
                },
            );

            if (signal) { signal.addEventListener("abort", onAbort); }

            timer = setTimeout(() => {
                if (settled) { return; }
                settled = true;
                this.#sendCancelFrame(id);
                this.cancelRequest(id);
                teardown();
                reject(new RequestTimeoutError(messageType, timeoutMs));
            }, timeoutMs);
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

    /**
     * Low-level round-trip primitive: register `onReply` (called once with the
     * raw wire status + payload when the server replies) and transmit a
     * {@link Protocol.ROOM_REQUEST} (`mode` picks the channel). {@link request}
     * wraps it with a promise + timeout + abort signal. `onClose` (optional) is
     * invoked on disconnect with the close code/reason — omit it to drop the
     * registration silently. Returns the request id, or `-1` if an unreliable
     * send couldn't be transmitted (offline). @internal
     */
    protected sendRequest(
        messageType: string | number,
        payload: any,
        opts: { mode?: "reliable" | "unreliable" },
        onReply: OnReply,
        onClose?: (code?: number, reason?: string) => void,
    ): number {
        const requestId = this.#mintRequestId();

        const data = this.#encodeRequestFrame(requestId, messageType, payload);
        if (opts.mode === "unreliable") {
            if (!this.connection.isOpen) { return -1; }
            // Sent once — no unreliable channel falls back to reliable inside the
            // transport.
            this.connection.sendUnreliable(data);
        } else if (this.connection.isOpen) {
            this.connection.send(data);
        } else {
            // Reliable + offline: buffer so it flushes on (re)connect.
            enqueueMessage(this, new Uint8Array(data));
        }

        this.#pending.set(requestId, { onReply, onClose, epoch: this.#connectionEpoch });
        return requestId;
    }

    /** Encode + best-effort-send a {@link Protocol.ROOM_REQUEST_CANCEL} frame.
     *  Not enqueued while offline: a cancel for a request that never reached the
     *  server frees nothing there, and after reconnect the request id is stale
     *  anyway (the server dropped its pending map on disconnect). @internal */
    #sendCancelFrame(requestId: number): void {
        if (!this.connection.isOpen) { return; }
        const it: Iterator = { offset: 1 };
        this.sharedBuffer[0] = Protocol.ROOM_REQUEST_CANCEL;
        encode.number(this.sharedBuffer, requestId, it);
        this.connection.send(this.sharedBuffer.subarray(0, it.offset));
    }

    /** Drop a pending round-trip (request timeout/abort, or the predict layer's
     *  TTL/cancel path). Idempotent. @internal */
    protected cancelRequest(id: number): void {
        this.#pending.delete(id);
    }

    #rejectAllPending(code?: number, reason?: string) {
        if (this.#pending.size === 0) { return; }
        // request entries reject (their `onClose` clears the timer/signal);
        // entries with no `onClose` drop silently.
        for (const entry of this.#pending.values()) { entry.onClose?.(code, reason); }
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
            // Already settled (timed out / aborted / connection-closed), unknown
            // id, or — critically — a LATE response riding a NEW connection whose
            // monotonic id space has wrapped back onto this number: the epoch
            // mismatch means it answers a request from a previous socket, so it
            // must never resolve the current holder. Ignore.
            if (entry !== undefined && entry.epoch === this.#connectionEpoch) {
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
        // Last-resort lifecycle cleanup: onLeave → removeAllListeners → destroy.
        // onclose/leave() normally clear pending first; this covers rooms torn
        // down without a socket close event.
        this.#rejectAllPending(CloseCode.GOING_AWAY, "room disposed before a response was received.");

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
