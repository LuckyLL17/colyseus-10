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
  reject: () => undefined as any,
  resolve: () => undefined as any,
});

/**
 * Policy for a request whose `requestId` is already pending from the SAME
 * client (a retry before the first attempt answered):
 *
 * - `"allow"` (default) — every frame is dispatched independently. Matches
 *   the historical behavior; the handler runs again and both replies go out,
 *   the client correlating each to its own registration.
 * - `"idempotent"` — the duplicate is NOT dispatched: it is coalesced onto
 *   the in-flight attempt. When that settles, the SAME response is sent to
 *   every coalesced wait, so a retried non-idempotent-looking call (charge,
 *   spawn) executes once. A cancel from any wait drops just that wait; if the
 *   originating request cancels, the shared attempt is abandoned (its signal
 *   aborts) and remaining waits are released without a reply (their own
 *   timeout/close handles them).
 * - `"reject"` — the duplicate is refused before the handler runs with a
 *   {@link ResponseStatus.DUPLICATE} reply echoing the id.
 */
export type DuplicateRequestPolicy = "allow" | "idempotent" | "reject";

/** Server-side bookkeeping for one in-flight ROOM_REQUEST. */
interface PendingRequest {
  /** The client/correlation pair this attempt answers. Under
   *  `"idempotent"` a coalesced retry appends its own pair so the shared
   *  response fans out to every wait; under `"allow"` every attempt has
   *  exactly one. Kept as a list (rather than keying the registry by id)
   *  because `"allow"` permits concurrent same-id attempts. */
  waiters: Array<{ client: Client & ClientPrivate, requestId: number }>;
  /** Aborts when the request is cancelled (client timeout/abort/close) —
   *  surfaced to the handler as `ctx.signal`. */
  controller: AbortController;
  /** Settled guard — a late async resolution after cancel/reply is a no-op. */
  settled: boolean;
  /** Remove this attempt from its registry slot on settle. For an
   *  idempotent attempt it owns the slot (it's the only one); under
   *  `"allow"` the slot holds the concurrent attempts and this splices just
   *  this one out. */
  remove: () => void;
}

/**
 * Per-room message-routing layer, owned by {@link Room}. Owns the user-message
 * handler registry (`onMessage`/`onMessageBytes` + per-type validators) and
 * decodes/dispatches the user-message wire frames (ROOM_DATA / ROOM_REQUEST /
 * ROOM_REQUEST_CANCEL / ROOM_DATA_BYTES). Room's `_onMessage` keeps the
 * protocol/lifecycle frames (JOIN/PING/LEAVE/input) and calls one of these per
 * frame, preserving the original dispatch order. Reads back into its Room only
 * for `onUncaughtException` (handler wrapping) and `roomId`/`roomName`
 * (logging).
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

  /** Per-client in-flight requests. The outer map is keyed by wire request
   *  id; under the default `"allow"` policy concurrent attempts with the SAME
   *  id are stored as a list (each answers independently), while
   *  `"idempotent"` keeps exactly one attempt per id with multiple waiters.
   *  A WeakMap lets a GC'd client drop its map; {@link onClientLeave} is the
   *  deterministic cleanup. */
  #pending = new WeakMap<Client & ClientPrivate, Map<number, PendingRequest[]>>();

  constructor(room: Room<any>) {
    this.room = room;
  }

  /** Fetch (creating on demand) a client's pending-request map. @internal */
  #pendingFor(client: Client & ClientPrivate): Map<number, PendingRequest[]> {
    let map = this.#pending.get(client);
    if (map === undefined) {
      map = new Map();
      this.#pending.set(client, map);
    }
    return map;
  }

  /** Count of handler attempts this client still has in flight (a coalesced
   *  idempotent duplicate rides its attempt and counts ONCE). @internal */
  #pendingCount(client: Client & ClientPrivate): number {
    let total = 0;
    this.#pending.get(client)?.forEach((attempts) => { total += attempts.length; });
    return total;
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
   *  missing handler → `ERROR`, else `OK(return)`. */
  onRequest(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const requestId = decode.number(buffer, it);

    const messageType = (decode.stringCheck(buffer, it))
      ? decode.string(buffer, it)
      : decode.number(buffer, it);

    const pending = this.#pendingFor(client);
    const inFlight = pending.get(requestId);

    if (inFlight !== undefined && inFlight.length > 0) {
      const policy: DuplicateRequestPolicy = this.room.duplicateRequestPolicy;
      if (policy === "reject") {
        debugMessage("duplicate request #%d rejected (roomId: %s)", requestId, this.room.roomId);
        this.#replyToRequest(client, requestId, ResponseStatus.DUPLICATE, { requestId });
        return;

      } else if (policy === "idempotent") {
        // Share the FIRST in-flight attempt: no second dispatch, no second
        // slot. Its eventual settle fans the same response out to every wait.
        inFlight[0].waiters.push({ client, requestId });
        debugMessage("duplicate request #%d coalesced (%d waiters, roomId: %s)",
          requestId, inFlight[0].waiters.length, this.room.roomId);
        return;
      }
      // "allow" falls through and pushes a second independent attempt below.
    }

    // Pending cap counts handler ATTEMPTS; a coalesced duplicate rides an
    // existing attempt and returned above, so it can never trip the cap.
    if (this.#pendingCount(client) >= this.room.maxPendingRequests) {
      debugMessage("request #%d refused: pending cap %d reached (roomId: %s)",
        requestId, this.room.maxPendingRequests, this.room.roomId);
      this.#replyToRequest(client, requestId, ResponseStatus.BUSY, { limit: this.room.maxPendingRequests });
      return;
    }

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

    // Register the attempt BEFORE invoking the handler, so a synchronous
    // handler round-tripping back into the room observes a consistent
    // registry.
    const controller = new AbortController();
    const entry: PendingRequest = {
      waiters: [{ client, requestId }],
      controller,
      settled: false,
      remove: () => {}, // assigned once the attempt is in its registry slot
    };
    const slot = pending.get(requestId);
    if (slot === undefined) {
      pending.set(requestId, [entry]);
    } else {
      // "allow": a concurrent same-id attempt — both answer independently.
      slot.push(entry);
    }

    // Splice only THIS attempt out; an "allow" sibling keeps the slot alive.
    entry.remove = () => {
      const current = pending.get(requestId);
      if (current === undefined) { return; }
      const idx = current.indexOf(entry);
      if (idx !== -1) { current.splice(idx, 1); }
      if (current.length === 0) { pending.delete(requestId); }
    };

    const ctx = new DispatchContext(requestId, controller.signal);

    // Single fan-out point: this attempt's originator + every idempotent
    // waiter receive the SAME encoded outcome. An "allow" sibling has its own.
    // Idempotent after a cancel/settle race.
    const settle = (status: ResponseStatus, payload?: any): void => {
      entry.settled = true;
      for (const waiter of entry.waiters) {
        this.#replyToRequest(waiter.client, waiter.requestId, status, payload);
      }
      entry.remove();
    };

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
          (resolved: any) => {
            if (!entry.settled) { this.#finalizeRequest(settle, ctx, resolved); }
          },
          (e: any) => {
            if (entry.settled) { return; }
            debugAndPrintError(e);
            settle(ResponseStatus.ERROR, toResponseError(e));
          },
        );
        return;
      }
    } catch (e: any) {
      debugAndPrintError(e);
      settle(ResponseStatus.ERROR, toResponseError(e));
      return;
    }

    this.#finalizeRequest(settle, ctx, response);
  }

  /** Dispatch a `ROOM_REQUEST_CANCEL` frame: release a pending request from
   *  THIS client. Under `"idempotent"` the FIRST attempt's CANCEL aborts the
   *  shared `ctx.signal` (a cooperative handler can stop) and drops every
   *  coalesced waiter; a later waiter's CANCEL removes just that waiter.
   *  Under `"allow"` each concurrent same-id attempt is independent, so the
   *  canceling client's own attempt is removed. @internal */
  onCancel(client: Client & ClientPrivate, buffer: Buffer, it: Iterator): void {
    const requestId = decode.number(buffer, it);
    const pending = this.#pending.get(client);
    const attempts = pending?.get(requestId);
    if (attempts === undefined || attempts.length === 0) { return; } // settled/unknown

    if (attempts.length > 1) {
      // "allow": find THIS client's own attempt and drop just that one.
      // (Coalesced idempotent retries never create extra attempts.)
      const own = attempts.findIndex((a) => a.waiters.some((w) => w.client === client));
      if (own === -1) { return; }
      const [entry] = attempts.splice(own, 1);
      entry.settled = true;
      entry.controller.abort(new Error("request cancelled by client"));
      if (attempts.length === 0) { pending!.delete(requestId); }
      debugMessage("request #%d cancelled (%d still in flight, roomId: %s)",
        requestId, attempts.length, this.room.roomId);
      return;
    }

    const entry = attempts[0];
    // The single attempt: it carries the originator at waiters[0] plus any
    // idempotent retries. Originator cancel abandons the whole shared attempt;
    // a retry's cancel drops just that waiter and lets the attempt continue.
    const waiterIndex = entry.waiters.findIndex((w) => w.client === client);

    if (waiterIndex <= 0) {
      entry.settled = true;
      entry.controller.abort(new Error("request cancelled by client"));
      entry.remove();
      debugMessage("request #%d cancelled (roomId: %s)", requestId, this.room.roomId);
    } else {
      entry.waiters.splice(waiterIndex, 1);
    }
  }

  /** Drop every in-flight request owned by a client (leave / disconnect).
   *  No replies are sent — the client's transport is gone. The handlers'
   *  abort signals fire so cooperative async work can stop. Called by
   *  {@link Room._onLeave} before the user's `onLeave` runs. @internal */
  onClientLeave(client: Client & ClientPrivate): void {
    const pending = this.#pending.get(client);
    if (pending === undefined) { return; }
    for (const attempts of pending.values()) {
      for (const entry of attempts) {
        if (!entry.settled) {
          entry.settled = true;
          entry.controller.abort(new Error("client left before the request completed"));
        }
      }
    }
    this.#pending.delete(client);
  }

  /** Abort all tracked requests at room disposal. @internal */
  dispose(): void {
    // WeakMap has no clear(); per-client maps only survive for live clients,
    // and disposal forcibly closes every client through onClientLeave, so this
    // is a belt-and-braces path (a room disposed outside its normal lifecycle).
    this.#pending = new WeakMap();
  }

  /** Finalize a request: project the handler's outcome onto a ROOM_RESPONSE reply —
   *  `ctx.reject` → REJECTED(reason), `ctx.resolve(value)` → OK(value), else OK(return). */
  #finalizeRequest(
    settle: (status: ResponseStatus, payload?: any) => void,
    ctx: DispatchContext,
    response: any,
  ): void {
    if (ctx._outcome === OUTCOME_REJECTED) {
      settle(ResponseStatus.REJECTED, ctx._reason);
    } else if (ctx._outcome === OUTCOME_RESOLVED) {
      settle(ResponseStatus.OK, ctx._value);
    } else {
      settle(ResponseStatus.OK, response);
    }
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

  /** Emit a ROOM_RESPONSE reply immediately. */
  #replyToRequest(client: Client, requestId: number, status: ResponseStatus, payload?: any): void {
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
