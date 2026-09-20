import { decode, type Iterator } from '@colyseus/schema';
import { unpack } from 'msgpackr';

import { CloseCode, ErrorCode, Protocol, ResponseStatus, type MessageContext } from '@colyseus/shared-types';
import { getMessageBytes } from './Protocol.ts';
import { createNanoEvents } from './utils/nanoevents.ts';
import { standardValidate, type StandardSchemaV1 } from './utils/StandardSchema.ts';
import { isDevMode } from './utils/DevMode.ts';
import { debugMessage, debugAndPrintError } from './Debug.ts';
import { OnMessageException } from './errors/RoomExceptions.ts';
import type { Client, ClientPrivate } from './Transport.ts';
import type { Room } from './Room.ts';

/** How the room treats a ROOM_REQUEST whose requestId is already pending on
 *  that connection:
 *  - `"reject"` (default): answer the duplicate frame immediately with
 *    {@link ResponseStatus.DUPLICATE}; the in-flight request is untouched.
 *  - `"idempotent"`: drop the duplicate frame silently. The single in-flight
 *    handler's eventual reply answers both sends (both carry the same id). */
export type DuplicateRequestPolicy = "reject" | "idempotent";

/** Normalize a thrown value into the `{ name, message, code? }` shape echoed in a
 *  ROOM_RESPONSE error reply. */
function toResponseError(e: any): { name: string; message: string; code?: any } {
  if (e instanceof Error) {
    const code = (e as any).code;
    return (code !== undefined)
      ? { name: e.name, message: e.message, code }
      : { name: e.name, message: e.message };
  }
  return { name: "Error", message: String(e) };
}

// The handler's terminal action — NOT the wire ResponseStatus: `none` and
// `resolved` both reply OK (plain return value vs `ctx.resolve(value)`), a
// distinction a single OK can't carry; ERROR is never a handler decision
// (thrown/no-handler). Projected onto ResponseStatus once, in onRequest's reply.
const OUTCOME_NONE = 0, OUTCOME_REJECTED = 1, OUTCOME_RESOLVED = 2;

/**
 * Per-request bookkeeping for an in-flight ROOM_REQUEST. The `Map` this lives
 * in is keyed per transport connection (see RoomMessages.#pendingByRef), so it
 * dies with the socket: a late handler reply after disconnect / reconnect finds
 * no slot and is dropped rather than written to a new connection.
 *
 * `controller` is aborted on explicit cancel (ROOM_REQUEST_CANCEL), when the
 * connection leaves, or when the room disposes — it backs `ctx.signal`.
 */
interface PendingRequest {
  controller: AbortController;
}

/**
 * Runtime {@link MessageContext} for a {@link Protocol.ROOM_REQUEST} dispatch.
 * `reject`/`resolve` record the decision on the ctx; `onRequest` reads it after the
 * handler returns, so a bare side-effecting call works as well as `return
 * ctx.reject(r)` (the branded return is compile-time only — the runtime return is
 * unused).
 */
class DispatchContext implements MessageContext {
  readonly id: number | undefined;
  readonly signal: AbortSignal | undefined;
  _outcome = OUTCOME_NONE;
  _reason: any = undefined;
  _value: any = undefined;

  constructor(id: number | undefined, signal?: AbortSignal) {
    this.id = id;
    this.signal = signal;
  }

  reject(reason?: any): any {
    this._outcome = OUTCOME_REJECTED;
    this._reason = reason;
    return undefined;
  }

  resolve(value?: any): any {
    this._outcome = OUTCOME_RESOLVED;
    this._value = value;
    return undefined;
  }
}

/** Shared no-op context for fire-and-forget `ROOM_DATA` (no reply channel, so
 *  `reject`/`resolve` go nowhere). `id` is `undefined`; zero per-message allocation. */
const SEND_CONTEXT: MessageContext = Object.freeze({
  id: undefined,
  signal: undefined,
  reject: () => undefined as any,
  resolve: () => undefined as any,
});

/**
 * Per-room message-routing layer, owned by {@link Room}. Owns the user-message
 * handler registry (`onMessage`/`onMessageBytes` + per-type validators) and
 * decodes/dispatches the user-message wire frames (ROOM_DATA / ROOM_REQUEST /
 * ROOM_DATA_BYTES). Room's `_onMessage` keeps the protocol/lifecycle frames
 * (JOIN/PING/LEAVE/input) and calls one of these per frame, preserving the
 * original dispatch order. Reads back into its Room only for `onUncaughtException`
 * (handler wrapping) and `roomId`/`roomName` (logging).
 *
 * Also owns the per-connection pending-request ledger that backs the server
 * half of request/reply: the room-wide pending cap (`room.maxPendingRequests`),
 * duplicate-requestId policy (`room.duplicateRequestPolicy`), cancellation
 * (ROOM_REQUEST_CANCEL), and lifecycle cleanup (client leave / room dispose).
 *
 * @internal
 */
export class RoomMessages {
  private room: Room<any>;

  /** Handler registry (nanoevents). Public so Room can re-expose it as
   *  `onMessageEvents` for @colyseus/playground introspection and
   *  @colyseus/testing handler-swapping. */
  events = createNanoEvents();

  /** Per-type StandardSchema validators. Public for the same reason as
   *  {@link events} (`onMessageValidators`). */
  // null-prototype: keyed by client-supplied message type (colyseus/colyseus#951)
  validators: { [type: string]: StandardSchemaV1 } = Object.create(null);

  /**
   * In-flight requests keyed by the TRANSPORT CONNECTION (`client.ref`), then by
   * requestId. The outer WeakMap needs no explicit teardown when a socket is
   * GC'd; the inner Map is deleted on every leave/close via
   * {@link releaseConnection}. A new connection (post-reconnect) starts with an
   * EMPTY ledger — a late reply keyed off the old ref can never be delivered to
   * the new socket, even if a client reuses a requestId.
   */
  #pendingByRef = new WeakMap<object, Map<number, PendingRequest>>();

  /** Every ledger currently in {@link #pendingByRef}. WeakMap isn't iterable, so
   *  room-wide cleanup ({@link releaseAll} on dispose) keeps its own index.
   *  Ledgers are unregistered once emptied, so a long-lived room doesn't
   *  accumulate dead sets. @internal */
  #allPending = new Set<Map<number, PendingRequest>>();

  constructor(room: Room<any>) {
    this.room = room;
  }

  /**
   * Register a handler (body of {@link Room.onMessage}). Wraps the callback via
   * the room's `onUncaughtException` when set, stores the validator, and returns
   * the unbind closure.
   */
  on(
    _messageType: '*' | string | number,
    _validationSchema: StandardSchemaV1 | ((...args: any[]) => void),
    _callback?: (...args: any[]) => void,
  ): () => void {
    const messageType = _messageType.toString();

    const validationSchema = (typeof _callback === 'function')
      ? _validationSchema as StandardSchemaV1
      : undefined;

    const callback = (validationSchema === undefined)
      ? _validationSchema as (...args: any[]) => void
      : _callback;

    const removeListener = this.events.on(messageType, (this.room.onUncaughtException !== undefined)
      ? this.#wrapMessageHandler(callback, _messageType)
      : callback);

    if (validationSchema !== undefined) {
      this.validators[messageType] = validationSchema;
    }

    // returns a method to unbind the callback
    return () => {
      removeListener();
      if (this.events.events[messageType].length === 0) {
        delete this.validators[messageType];
      }
    };
  }

  /**
   * Wrap a user message handler so a sync throw or async rejection is reported to
   * `room.onUncaughtException` as an {@link OnMessageException} carrying the correct
   * `client` / `payload` / `type`. The handler is invoked with a different argument
   * shape per dispatch path, so those fields are normalized here rather than relying
   * on positional forwarding (which silently mis-mapped `type` once a context arg was
   * added — see ROOM_DATA/ROOM_REQUEST passing `SEND_CONTEXT`/`ctx`):
   *
   *   - typed handler (DATA / REQUEST / BYTES): `(client, payload, ctx?)` → `type` is
   *     the registered `_messageType`.
   *   - wildcard `'*'` handler:                 `(client, type, payload)` → `type` is
   *     the received message type.
   *
   * The error is swallowed (never rethrown) so a faulty handler can't break dispatch;
   * the handler's return value (or promise) is passed through for the REQUEST path.
   */
  #wrapMessageHandler(callback: (...args: any[]) => any, registeredType: '*' | string | number) {
    const onError = this.room.onUncaughtException!.bind(this.room);
    const isWildcard = (registeredType === '*');

    return (...args: any[]): any => {
      const report = (e: Error) => onError(
        new OnMessageException(
          e, e.message,
          args[0],                                  // client
          isWildcard ? args[2] : args[1],           // payload
          isWildcard ? args[1] : registeredType,    // type
        ),
        'onMessage',
      );

      try {
        const result = callback(...args);
        return (typeof result?.catch === 'function') ? result.catch(report) : result;
      } catch (e: any) {
        report(e);
      }
    };
  }

  /** Dispatch a `ROOM_DATA` frame: decode type + msgpack payload, validate, emit. */
  onData(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const messageType = (decode.stringCheck(buffer, it))
      ? decode.string(buffer, it)
      : decode.number(buffer, it);

    let message;
    try {
      message = (buffer.byteLength > it.offset)
        ? unpack(buffer.subarray(it.offset, buffer.byteLength))
        : undefined;
      debugMessage("received: '%s' -> %j (roomId: %s)", messageType, message, this.room.roomId);

      // custom message validation
      if (this.validators[messageType] !== undefined) {
        message = standardValidate(this.validators[messageType], message);
      }

    } catch (e: any) {
      debugAndPrintError(e);
      client.leave(CloseCode.WITH_ERROR);
      return;
    }

    if (this.events.events[messageType]) {
      this.events.emit(messageType as string, client, message, SEND_CONTEXT);

    } else if (this.events.events['*']) {
      this.events.emit('*', client, messageType, message);

    } else {
      this.#noHandler(client, messageType, message);
    }
  }

  /** Dispatch a `ROOM_REQUEST` frame: same handlers as ROOM_DATA, but the client
   *  opted into a reply, so echo the `requestId` with the outcome. The handler's
   *  reply is decided by what it returns / does to `ctx`: `ctx.reject(reason)` →
   *  `REJECTED(reason)`, `ctx.resolve(value)` → `OK(value)`, a thrown error or
   *  missing handler → `ERROR`, else `OK(return)`.
   *
   *  Before dispatch the frame passes the pending ledger: a full per-connection
   *  cap replies CAPACITY (no handler runs); a repeated requestId is either
   *  rejected (DUPLICATE) or swallowed ("idempotent" — the in-flight reply will
   *  answer both sends). */
  onRequest(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const requestId = decode.number(buffer, it);

    const messageType = (decode.stringCheck(buffer, it))
      ? decode.string(buffer, it)
      : decode.number(buffer, it);

    let message;
    try {
      message = (buffer.byteLength > it.offset)
        ? unpack(buffer.subarray(it.offset, buffer.byteLength))
        : undefined;
      debugMessage("request #%d: '%s' -> %j (roomId: %s)", requestId, messageType, message, this.room.roomId);

      // custom message validation (shared with the ROOM_DATA path)
      if (this.validators[messageType] !== undefined) {
        message = standardValidate(this.validators[messageType], message);
      }

    } catch (e: any) {
      // Reply with an error so the caller's pending request resolves instead of timing out.
      debugAndPrintError(e);
      this.#replyToRequest(client, requestId, ResponseStatus.ERROR, toResponseError(e));
      return;
    }

    const pending = this.#getOrCreatePending(client);

    // Duplicate requestId on this connection: policy decides.
    if (pending.has(requestId)) {
      if (this.room.duplicateRequestPolicy === "idempotent") {
        // The first dispatch owns the reply; it carries the same requestId, so
        // the duplicate sender is answered by it as well. Nothing to do.
        debugMessage("duplicate request #%d swallowed (idempotent policy, roomId: %s)", requestId, this.room.roomId);
        return;
      }
      this.#replyToRequest(client, requestId, ResponseStatus.DUPLICATE, {
        name: "duplicate_request",
        message: `request "${messageType}" (id ${requestId}) is already pending.`,
        requestId,
      });
      return;
    }

    // Pending cap (per connection). Overflow is refused BEFORE the handler runs
    // — replying ERROR after a handler already started would not free the slot.
    const limit = this.room.maxPendingRequests;
    if (Number.isFinite(limit) && pending.size >= limit) {
      debugMessage("request #%d refused: pending cap %d reached (roomId: %s)", requestId, limit, this.room.roomId);
      this.#replyToRequest(client, requestId, ResponseStatus.CAPACITY, {
        name: "pending_capacity_exceeded",
        message: `room has reached its limit of ${limit} pending requests per connection.`,
        limit,
      });
      return;
    }

    // Answered by the FIRST handler registered for the type (emit would discard
    // returns); wildcard handlers have no response contract, so they're ineligible.
    const handler = this.events.events[messageType as string]?.[0];

    if (handler === undefined) {
      this.#replyToRequest(client, requestId, ResponseStatus.ERROR, {
        name: "no_handler",
        message: `room "${this.room.roomName}" has no onMessage("${messageType}") handler to answer this request.`,
      });
      return;
    }

    const controller = new AbortController();
    pending.set(requestId, { controller });
    const ctx = new DispatchContext(requestId, controller.signal);

    // Sync handlers reply in this tick with no promise machinery; only a thenable
    // return defers to the microtask queue. A sync throw (unwrapped handler) and an
    // async rejection both fall to the ERROR reply. With onUncaughtException set the
    // handler is wrapped (swallows its own errors), so it never throws / its promise
    // resolves undefined, and the error reports there.
    let response: any;
    try {
      response = handler(client, message, ctx);
      if (response !== null && typeof response === 'object' && typeof response.then === 'function') {
        response.then(
          (resolved: any) => this.#finalizeRequest(client, requestId, ctx, resolved),
          (e: any) => {
            debugAndPrintError(e);
            this.#replyToRequest(client, requestId, ResponseStatus.ERROR, toResponseError(e));
          },
        );
        return;
      }
    } catch (e: any) {
      debugAndPrintError(e);
      this.#replyToRequest(client, requestId, ResponseStatus.ERROR, toResponseError(e));
      return;
    }

    this.#finalizeRequest(client, requestId, ctx, response);
  }

  /** Dispatch a `ROOM_REQUEST_CANCEL` frame: abort the in-flight request's
   *  {@link AbortController} (so `ctx.signal` observers in the handler stop) and
   *  release its pending slot. Idempotent — an unknown / already-settled id is a
   *  no-op (the reply, if it races, is discarded on the client). */
  onCancel(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const requestId = decode.number(buffer, it);
    const pending = this.#pendingByRef.get(client.ref as object);
    const entry = pending?.get(requestId);

    if (entry !== undefined) {
      debugMessage("request #%d cancelled by client (roomId: %s)", requestId, this.room.roomId);
      entry.controller.abort();
      this.#deletePending(pending!, requestId);
    }
  }

  /** Finalize a request: project the handler's outcome onto a ROOM_RESPONSE reply —
   *  `ctx.reject` → REJECTED(reason), `ctx.resolve(value)` → OK(value), else OK(return).
   *  A request already removed from the ledger (cancelled, disconnected, disposed)
   *  settles nowhere: the slot release IS the "don't reply to a ghost" guard. */
  #finalizeRequest(client: Client & ClientPrivate, requestId: number, ctx: DispatchContext, response: any): void {
    const pending = this.#pendingByRef.get(client.ref as object);
    if (pending === undefined || !pending.has(requestId)) {
      // Cancelled by the client, the connection is gone, or the room disposed.
      debugMessage("response #%d discarded: request no longer pending (roomId: %s)", requestId, this.room.roomId);
      return;
    }
    this.#deletePending(pending, requestId);

    if (ctx._outcome === OUTCOME_REJECTED) {
      this.#replyToRequest(client, requestId, ResponseStatus.REJECTED, ctx._reason);
    } else if (ctx._outcome === OUTCOME_RESOLVED) {
      this.#replyToRequest(client, requestId, ResponseStatus.OK, ctx._value);
    } else {
      this.#replyToRequest(client, requestId, ResponseStatus.OK, response);
    }
  }

  /**
   * Release EVERY pending request on one transport connection — called from the
   * Room when a client leaves (consented leave, drop, kick) and BEFORE a
   * reconnecting client's ref is transplanted. Aborts handler signals and drops
   * the ledger: any handler that settles afterwards finds no slot (#finalizeRequest
   * no-ops), so a late response is never written to the replacement connection.
   */
  releaseConnection(client: Client & ClientPrivate): void {
    const pending = this.#pendingByRef.get(client.ref as object);
    if (pending === undefined) { return; }

    for (const entry of pending.values()) {
      if (!entry.controller.signal.aborted) { entry.controller.abort(); }
    }
    this.#allPending.delete(pending);
    this.#pendingByRef.delete(client.ref as object);
  }

  /**
   * Abort and discard all pending requests across ALL connections — called once
   *  from Room disposal. Replies are not attempted (the sockets are going away;
   *  the SDK rejects its own pending on close), but handler `ctx.signal`s abort
   *  so async work can stop before the room tears down around it.
   */
  releaseAll(): void {
    if (this.#allPending.size === 0) { return; }
    for (const pending of this.#allPending) {
      for (const entry of pending.values()) {
        if (!entry.controller.signal.aborted) { entry.controller.abort(); }
      }
      pending.clear();
    }
    this.#allPending.clear();
  }

  /** Dispatch a `ROOM_DATA_BYTES` frame: raw bytes routed to `_$b`-prefixed handlers. */
  onDataBytes(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const messageType = (decode.stringCheck(buffer, it))
      ? decode.string(buffer, it)
      : decode.number(buffer, it);

    let message: any = buffer.subarray(it.offset, buffer.byteLength);
    debugMessage("received: '%s' -> %j (roomId: %s)", messageType, message, this.room.roomId);

    const bytesMessageType = `_$b${messageType}`;

    // custom message validation
    try {
      if (this.validators[bytesMessageType] !== undefined) {
        message = standardValidate(this.validators[bytesMessageType], message);
      }
    } catch (e: any) {
      debugAndPrintError(e);
      client.leave(CloseCode.WITH_ERROR);
      return;
    }

    if (this.events.events[bytesMessageType]) {
      this.events.emit(bytesMessageType, client, message);

    } else if (this.events.events['*']) {
      this.events.emit('*', client, messageType, message);

    } else {
      this.#noHandler(client, messageType, message);
    }
  }

  /** Fetch (lazily creating) the pending ledger for a client's transport
   *  connection. Keyed on `client.ref` so a reconnect (new ref) starts empty. */
  #getOrCreatePending(client: Client & ClientPrivate): Map<number, PendingRequest> {
    const ref = client.ref as object;
    let pending = this.#pendingByRef.get(ref);
    if (pending === undefined) {
      pending = new Map();
      this.#pendingByRef.set(ref, pending);
      this.#allPending.add(pending);
    }
    return pending;
  }

  /** Remove one settled request; drop the ledger from the room-wide index once
   *  empty so dead connections' maps don't linger (the WeakMap entry itself is
   *  collected with the ref). */
  #deletePending(pending: Map<number, PendingRequest>, requestId: number): void {
    pending.delete(requestId);
    if (pending.size === 0) {
      this.#allPending.delete(pending);
    }
  }

  /** Emit a ROOM_RESPONSE reply immediately. */
  #replyToRequest(client: Client & ClientPrivate, requestId: number, status: ResponseStatus, payload?: any): void {
    debugMessage("response #%d: status=%d -> %j (roomId: %s)", requestId, status, payload, this.room.roomId);
    client.enqueueRaw(getMessageBytes[Protocol.ROOM_RESPONSE](requestId, status, payload));
  }

  /** No handler for the type: error the client in dev, drop it in production. */
  #noHandler(client: Client, messageType: string | number, _message: unknown): void {
    const errorMessage = `room onMessage for "${messageType}" not registered.`;
    debugMessage(`${errorMessage} (roomId: ${this.room.roomId})`);
    if (isDevMode) {
      client.error(ErrorCode.INVALID_PAYLOAD, errorMessage);
    } else {
      client.leave(CloseCode.WITH_ERROR, errorMessage);
    }
  }
}
