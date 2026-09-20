import { ResponseStatus } from '@colyseus/shared-types';
import { AbortError } from './errors/Errors.ts';

/** Decoded `Protocol.ROOM_RESPONSE` outcome handed to a pending round-trip's
 *  `onReply` callback: `status` is the raw wire {@link ResponseStatus}; `payload`
 *  is the decoded msgpack body (OK value / REJECTED reason / ERROR error object).
 *  @internal */
export type OnReply = (status: number, payload: any) => void;

/** Options for {@link Room.request}. */
export interface RequestOptions {
    /** Reject the returned promise if no reply arrives within this many ms
     *  (defaults to {@link Room.defaultRequestTimeout}). The server slot is
     *  released via a best-effort ROOM_REQUEST_CANCEL when the timer fires. */
    timeout?: number;
    /** `"unreliable"` rides the unreliable channel (falls back to reliable when the
     *  transport has none) and is sent once — a genuine drop is surfaced by the
     *  `timeout`. Defaults to `"reliable"`. */
    mode?: "reliable" | "unreliable";
    /** AbortSignal that cancels the in-flight request: aborting removes the
     *  pending callback (a late reply is dropped), sends a best-effort
     *  ROOM_REQUEST_CANCEL so the server can free its pending slot / abort the
     *  handler's `ctx.signal`, and rejects with an {@link AbortError}.
     *  An already-aborted signal rejects before anything is sent. */
    signal?: AbortSignal;
}

/** The request's own timeout elapsed before a reply arrived. */
export class RequestTimeoutError extends Error {
    public readonly requestType: string | number;
    public readonly timeout: number;
    constructor(messageType: string | number, timeoutMs: number) {
        super(`request "${messageType}" timed out after ${timeoutMs}ms.`);
        this.name = "TimeoutError";
        this.requestType = messageType;
        this.timeout = timeoutMs;
    }
}

/** The transport closed while the request was awaiting a reply (leave, drop,
 *  or server shutdown — NOT a local abort and NOT a timeout). */
export class RequestClosedError extends Error {
    public readonly code?: number;
    public readonly reason?: string;
    constructor(message: string, code?: number, reason?: string) {
        super(message);
        this.name = "RequestClosedError";
        if (code !== undefined) { this.code = code; }
        if (reason !== undefined) { this.reason = reason; }
    }
}

/** The room refused to start the request because its per-connection pending
 *  limit was full (`room.maxPendingRequests`); no handler ran. */
export class RequestCapacityError extends Error {
    public readonly limit: number;
    constructor(message: string, limit: number) {
        super(message);
        this.name = "RequestCapacityError";
        this.limit = limit;
    }
}

/** The room still considers this `requestId` pending and is configured to
 *  reject duplicates (`room.duplicateRequestPolicy === "reject"`). */
export class DuplicateRequestError extends Error {
    public readonly requestId: number;
    constructor(message: string, requestId: number) {
        super(message);
        this.name = "DuplicateRequestError";
        this.requestId = requestId;
    }
}

/** Build the AbortError a signal-aborted `request` rejects with. Reuses the
 *  SDK's exported AbortError so `err.name === "AbortError"` callers keep
 *  working. `reason` is the AbortSignal's abort reason when present. */
export function toAbortError(signal?: AbortSignal): AbortError {
    const reason = (signal as any)?.reason;
    const error = new AbortError(
        reason instanceof Error ? reason.message : "request aborted by signal.",
    );
    if (reason !== undefined) { (error as any).reason = reason; }
    return error;
}

/**
 * Map a wire ROOM_RESPONSE (already correlated to this pending request) to the
 * Error the promise rejects with — or `undefined` for OK. Keeping every
 * rejection taxonomy in one place is what lets callers distinguish the four
 * failure classes: timeout / abort / connection-closed are produced locally
 * (see {@link RequestTimeoutError}, {@link AbortError}, {@link RequestClosedError});
 * REJECTED / ERROR / CAPACITY / DUPLICATE arrive on the wire and map here.
 */
export function toResponseError(status: number, payload: any): Error | undefined {
    // OK → no error.
    if (status === ResponseStatus.OK) { return undefined; }

    // Deliberate ctx.reject(reason): name "rejected", .reason = raw typed reason.
    if (status === ResponseStatus.REJECTED) {
        const error: any = new Error("request rejected");
        error.name = "rejected";
        error.reason = payload;
        return error;
    }

    // Server fault: Error rebuilt from the sanitized { name, message, code }.
    if (status === ResponseStatus.ERROR) {
        const error: any = new Error(payload?.message ?? "request failed");
        if (payload?.name) { error.name = payload.name; }
        if (payload?.code !== undefined) { error.code = payload.code; }
        return error;
    }

    // Pending capacity exhausted — no handler ran.
    if (status === ResponseStatus.CAPACITY) {
        return new RequestCapacityError(
            payload?.message ?? "too many pending requests.",
            payload?.limit ?? Infinity,
        );
    }

    // Duplicate requestId under the "reject" policy.
    if (status === ResponseStatus.DUPLICATE) {
        return new DuplicateRequestError(
            payload?.message ?? "duplicate request id.",
            payload?.requestId ?? -1,
        );
    }

    // Forward-compatible: an unknown status from a newer server degrades to a
    // generic fault rather than resolving as success.
    const error: any = new Error(payload?.message ?? `request failed with status ${status}.`);
    if (payload?.name) { error.name = payload.name; }
    return error;
}
