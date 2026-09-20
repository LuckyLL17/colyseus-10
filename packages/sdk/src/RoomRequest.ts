import { ResponseStatus } from '@colyseus/shared-types';

/** Decoded `Protocol.ROOM_RESPONSE` outcome handed to a pending round-trip's
 *  `onReply` callback: `status` is the raw wire status (so the promise layer
 *  can map BUSY/DUPLICATE to their own errors); `payload` is the decoded body.
 *  @internal */
export type OnReply = (status: number, payload: any) => void;

/** Options for {@link Room.request}. */
export interface RequestOptions {
    /** Reject with a {@link RequestTimeoutError} if no reply arrives within
     *  this many ms. Defaults to {@link Room.defaultRequestTimeout}. Pass
     *  `false` (or `Infinity`) to wait indefinitely — then cancellation is
     *  solely the caller's job via {@link signal} or {@link Room.cancelRequest}. */
    timeout?: number | false;
    /** `"unreliable"` rides the unreliable channel (falls back to reliable when the
     *  transport has none) and is sent once — a genuine drop is surfaced by the
     *  `timeout`. Defaults to `"reliable"`. */
    mode?: "reliable" | "unreliable";
    /** Abort the in-flight request: aborting rejects with a
     *  {@link RequestAbortedError} (the abort `reason` rides on `.cause`),
     *  removes the pending registration, and — while connected — sends a
     *  {@link Protocol.ROOM_REQUEST_CANCEL} frame so the server can free its
     *  slot / stop work. An already-aborted signal rejects synchronously (the
     *  request is never transmitted). */
    signal?: AbortSignal;
    /** Pin the wire correlation id instead of letting the room mint a monotonic
     *  one. The ONLY reason to set it is server-side idempotency: a room
     *  configured with `duplicateRequestPolicy: "idempotent"` coalesces retries
     *  carrying the same id onto the first in-flight handler, so a retried
     *  request after a suspect send executes once. Ids are per-connection and
     *  uint32; callers are responsible for choosing unique values. */
    requestId?: number;
}

/**
 * Base class for every failure a {@link Room.request} promise can reject with —
 * including the local failure modes that never touch the wire
 * (timeout / abort / closed transport). `instanceof RequestError` is the single
 * discriminator from non-request errors; `.kind` names the specific cause.
 */
export class RequestError extends Error {
    /** Machine-readable failure category; stable across languages/SDKs. */
    public kind: RequestErrorKind;
    constructor(kind: RequestErrorKind, message: string, options?: { cause?: unknown }) {
        super(message);
        this.name = "RequestError";
        this.kind = kind;
        if (options?.cause !== undefined) {
            (this as any).cause = options.cause;
        }
        Object.setPrototypeOf(this, new.target.prototype);
    }
}

/** The five distinguishable failure categories of a request round-trip. */
export type RequestErrorKind = "timeout" | "aborted" | "closed" | "rejected" | "error" | "busy" | "duplicate";

/** No reply arrived before the per-request `timeout` elapsed. */
export class RequestTimeoutError extends RequestError {
    constructor(messageType: string | number, timeoutMs: number) {
        super("timeout", `request "${messageType}" timed out after ${timeoutMs}ms.`);
        this.name = "RequestTimeoutError";
    }
}

/** The request was cancelled — its {@link RequestOptions.signal} aborted or
 *  {@link Room.cancelRequest} was called. `.cause` carries the AbortSignal's
 *  reason when one was given. */
export class RequestAbortedError extends RequestError {
    constructor(messageType: string | number, cause?: unknown) {
        super("aborted", `request "${messageType}" was aborted.`, { cause });
        this.name = "RequestAbortedError";
    }
}

/** The transport closed (leave / disconnect / socket drop) before a reply
 *  arrived. `.code` carries the WebSocket close code when known. */
export class RequestClosedError extends RequestError {
    public code?: number;
    constructor(message: string, code?: number) {
        super("closed", message);
        this.name = "RequestClosedError";
        if (code !== undefined) { this.code = code; }
    }
}

/** A deliberate, typed `ctx.reject(reason)` from the server handler. `.reason`
 *  is the authored reason, surfaced verbatim and typed. */
export class RequestRejectedError extends RequestError {
    public reason: any;
    constructor(reason: any) {
        super("rejected", "request rejected");
        this.name = "RequestRejectedError";
        this.reason = reason;
    }
}

/** A server-side fault: the handler threw (or none was registered). Rebuilt
 *  from the sanitized `{ name, message, code }` payload. */
export class RequestFailedError extends RequestError {
    public code?: any;
    constructor(payload: { name?: string, message?: string, code?: any }) {
        super("error", payload?.message ?? "request failed");
        if (payload?.name) { this.name = payload.name; } else { this.name = "RequestFailedError"; }
        if (payload?.code !== undefined) { this.code = payload.code; }
    }
}

/** The room's per-client pending-request cap is full; the handler never ran.
 *  `.limit` echoes the configured `maxPendingRequests`. */
export class RequestBusyError extends RequestError {
    public limit: number;
    constructor(limit: number) {
        super("busy", `too many pending requests (limit is ${limit}).`);
        this.name = "RequestBusyError";
        this.limit = limit;
    }
}

/** A request with an already-pending id was refused under the room's
 *  `"reject"` duplicate policy; the handler never ran. `.requestId` echoes it. */
export class RequestDuplicateError extends RequestError {
    public requestId: number;
    constructor(requestId: number) {
        super("duplicate", `duplicate request id "${requestId}" is still pending.`);
        this.name = "RequestDuplicateError";
        this.requestId = requestId;
    }
}

/** Map a decoded ROOM_RESPONSE status to the Error the request promise rejects
 *  with. OK returns `undefined`. Keeps the wire's status byte as the single
 *  decision point — callers branch on `.kind`, never re-inspect the frame. */
export function toRequestError(status: number, payload: any): Error | undefined {
    switch (status) {
        case ResponseStatus.OK:
            return undefined;
        case ResponseStatus.REJECTED:
            return new RequestRejectedError(payload);
        case ResponseStatus.BUSY:
            return new RequestBusyError(payload?.limit ?? Infinity);
        case ResponseStatus.DUPLICATE:
            return new RequestDuplicateError(payload?.requestId);
        case ResponseStatus.ERROR:
        default:
            return new RequestFailedError(payload ?? {});
    }
}
