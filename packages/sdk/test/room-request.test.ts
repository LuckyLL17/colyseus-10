import './util';
import { describe, test, beforeEach, afterEach, vi } from "vitest";
import { assert } from "chai";

import { Protocol, ResponseStatus } from "@colyseus/shared-types";
import { encode, decode, Iterator } from "@colyseus/schema";
import { Packr } from "msgpackr";

import {
    Room,
    RequestError,
    RequestTimeoutError,
    RequestAbortedError,
    RequestClosedError,
    RequestRejectedError,
    RequestFailedError,
    RequestBusyError,
    RequestDuplicateError,
} from "../src/index.ts";

//
// SDK boundary for request/reply: per-request timeout + abort signal, the
// four-way failure taxonomy (timeout / aborted / closed / business error,
// incl. busy/duplicate), stale-response isolation across transports, and
// lifecycle cleanup on leave/destroy. Frames are driven directly into
// `onMessageCallback` with a stub transport recording outbound bytes — no
// socket needed.
//

const packr = new Packr({ useRecords: false });

/** Build a ROOM_RESPONSE frame exactly like the server:
 *  `[byte][requestId varint][status uint8][msgpack?]`. */
function responseFrame(requestId: number, status: number, payload?: any): Uint8Array {
    const header = new Uint8Array(16);
    header[0] = Protocol.ROOM_RESPONSE;
    const it: Iterator = { offset: 1 };
    encode.number(header, requestId, it);
    header[it.offset++] = status;
    if (payload === undefined) {
        return header.subarray(0, it.offset);
    }
    const body = packr.pack(payload);
    const out = new Uint8Array(it.offset + body.byteLength);
    out.set(header.subarray(0, it.offset), 0);
    out.set(body, it.offset);
    return out;
}

/** Read a varint the SDK itself decodes (used to assert outbound frames). */
function readRequestId(buffer: Uint8Array, code: number) {
    assert.equal(buffer[0], code);
    const it: Iterator = { offset: 1 };
    return decode.number(buffer as any, it);
}

interface StubConnection {
    sent: Uint8Array[];
    isOpen: boolean;
}

function stubRoom(): { room: Room, conn: StubConnection, feed: (f: Uint8Array) => void } {
    const room = new Room("game");
    const conn: StubConnection = {
        sent: [],
        isOpen: true,
        send(data: Uint8Array) { this.sent.push(new Uint8Array(data)); },
    };
    (room as any).connection = conn;
    const feed = (frame: Uint8Array) =>
        room['onMessageCallback']({ data: frame } as unknown as MessageEvent);
    return { room, conn, feed };
}

describe("Room.request — SDK boundary", () => {
    beforeEach(() => {
        Room.defaultRequestTimeout = 10000;
    });

    test("resolves with the OK payload", async () => {
        const { room, feed } = stubRoom();
        const promise = room.request("profile", { id: 7 });
        feed(responseFrame(0, ResponseStatus.OK, { name: "ada" }));
        assert.deepEqual(await promise, { name: "ada" });
    });

    test("rejects on a deliberate ctx.reject with the authored reason", async () => {
        const { room, feed } = stubRoom();
        const promise = room.request("buy", {});
        feed(responseFrame(0, ResponseStatus.REJECTED, "no gold"));
        try {
            await promise;
            assert.fail("should have rejected");
        } catch (e) {
            assert.instanceOf(e, RequestRejectedError);
            assert.instanceOf(e, RequestError);
            assert.equal((e as RequestError).kind, "rejected");
            assert.equal((e as RequestRejectedError).reason, "no gold");
        }
    });

    test("rejects on a handler fault with the sanitized error shape", async () => {
        const { room, feed } = stubRoom();
        const promise = room.request("boom", {});
        feed(responseFrame(0, ResponseStatus.ERROR, { name: "TypeError", message: "x is not a function", code: 42 }));
        try {
            await promise;
            assert.fail("should have rejected");
        } catch (e) {
            assert.instanceOf(e, RequestFailedError);
            assert.equal((e as RequestError).kind, "error");
            assert.equal((e as Error).name, "TypeError");
            assert.equal((e as Error).message, "x is not a function");
            assert.equal((e as RequestFailedError).code, 42);
        }
    });

    test("maps BUSY / DUPLICATE statuses to their typed errors", async () => {
        const { room, feed } = stubRoom();

        const busy = room.request("a", {});
        feed(responseFrame(0, ResponseStatus.BUSY, { limit: 4 }));
        try { await busy; assert.fail(); } catch (e) {
            assert.instanceOf(e, RequestBusyError);
            assert.equal((e as RequestBusyError).limit, 4);
            assert.equal((e as RequestError).kind, "busy");
        }

        const dup = room.request("b", {});
        feed(responseFrame(1, ResponseStatus.DUPLICATE, { requestId: 1 }));
        try { await dup; assert.fail(); } catch (e) {
            assert.instanceOf(e, RequestDuplicateError);
            assert.equal((e as RequestDuplicateError).requestId, 1);
            assert.equal((e as RequestError).kind, "duplicate");
        }
    });

    test("times out with RequestTimeoutError and sends a CANCEL frame", async () => {
        const { room, conn } = stubRoom();
        const t = Date.now();
        try {
            await room.request("slow", {}, { timeout: 20 });
            assert.fail("should have timed out");
        } catch (e) {
            assert.instanceOf(e, RequestTimeoutError);
            assert.equal((e as RequestError).kind, "timeout");
            assert.match((e as Error).message, /timed out after 20ms/);
            assert.isAtLeast(Date.now() - t, 15);
        }
        const cancel = conn.sent.find((f) => f[0] === Protocol.ROOM_REQUEST_CANCEL);
        assert.isDefined(cancel, "timeout must notify the server so it frees the slot");
        assert.equal(readRequestId(cancel!, Protocol.ROOM_REQUEST_CANCEL), 0);
    });

    test("abort via signal rejects with RequestAbortedError and cancels", async () => {
        const { room, conn } = stubRoom();
        const controller = new AbortController();
        const promise = room.request("work", {}, { signal: controller.signal, timeout: false });

        controller.abort(new Error("navigated away"));

        try {
            await promise;
            assert.fail("should have aborted");
        } catch (e) {
            assert.instanceOf(e, RequestAbortedError);
            assert.equal((e as RequestError).kind, "aborted");
            assert.equal((e as any).cause?.message, "navigated away");
        }
        const cancel = conn.sent.find((f) => f[0] === Protocol.ROOM_REQUEST_CANCEL);
        assert.isDefined(cancel);
        assert.equal(readRequestId(cancel!, Protocol.ROOM_REQUEST_CANCEL), 0);
    });

    test("an already-aborted signal fails before transmission", async () => {
        const { room, conn } = stubRoom();
        const controller = new AbortController();
        controller.abort();
        try {
            await room.request("work", {}, { signal: controller.signal });
            assert.fail();
        } catch (e) {
            assert.instanceOf(e, RequestAbortedError);
        }
        assert.lengthOf(conn.sent, 0);
    });

    test("a reply after abort is a no-op (settled exactly once)", async () => {
        const { room, feed } = stubRoom();
        const controller = new AbortController();
        const promise = room.request("work", {}, { signal: controller.signal, timeout: false });
        controller.abort();
        await promise.catch(() => {});
        // late OK must not resolve/reject anything a second time
        feed(responseFrame(0, ResponseStatus.OK, { late: true }));
    });

    test("timeout:false waits until a reply", async () => {
        vi.useFakeTimers();
        try {
            const { room, feed } = stubRoom();
            const promise = room.request("slow", {}, { timeout: false });
            await vi.advanceTimersByTimeAsync(60_000);
            feed(responseFrame(0, ResponseStatus.OK, "finally"));
            assert.equal(await promise, "finally");
        } finally {
            vi.useRealTimers();
        }
    });

    test("transport close rejects pending requests with RequestClosedError (kind: closed)", async () => {
        const { room } = stubRoom();
        const a = room.request("a", {}, { timeout: false });
        const b = room.request("b", {}, { timeout: false });

        // The same chokepoint the connect()-bound onclose invokes.
        (room as any).rejectAllPending(new RequestClosedError(
            "connection closed before a response was received.", 1006));

        for (const p of [a, b]) {
            try { await p; assert.fail(); }
            catch (e) {
                assert.instanceOf(e, RequestClosedError);
                assert.equal((e as RequestError).kind, "closed");
                assert.equal((e as RequestClosedError).code, 1006);
            }
        }
    });

    test("leave() rejects every pending request and subsequent late replies don't resolve", async () => {
        const { room, conn, feed } = stubRoom();
        const a = room.request("a", {}, { timeout: false });
        const b = room.request("b", {}, { timeout: false });
        room.leave();

        for (const p of [a, b]) {
            try { await p; assert.fail(); }
            catch (e) { assert.instanceOf(e, RequestClosedError); }
        }

        // Responses drained from the dying socket afterwards: dropped (and
        // nothing further is sent on a closed transport).
        const sentBefore = conn.sent.length;
        feed(responseFrame(0, ResponseStatus.OK, "late a"));
        feed(responseFrame(1, ResponseStatus.OK, "late b"));
        assert.equal(conn.sent.length, sentBefore);
    });

    test("a response arriving after the transport changed epochs never resolves a live registration", async () => {
        const { room, feed } = stubRoom();

        // Request in flight on epoch N; transport is replaced (reconnect)
        // before the frame drains out of the old socket.
        const stale = room.request("a", {}, { timeout: false });
        (room as any).bumpConnectionEpoch();

        // The old socket's delayed reply carries the right id but the WRONG
        // epoch: it must not resolve the live promise.
        feed(responseFrame(0, ResponseStatus.OK, "stale from old socket"));

        let settled = false;
        stale.then(() => { settled = true; }, () => { settled = true; });
        await Promise.resolve();
        assert.isFalse(settled, "stale frame must not settle the request");

        // The new transport rejects it through the normal close path.
        (room as any).rejectAllPending(new RequestClosedError("closed"));
        try { await stale; assert.fail(); } catch (e) {
            assert.instanceOf(e, RequestClosedError);
        }
    });

    test("end-to-end: after close + reconnect, late frames are dropped while a fresh round-trip resolves", async () => {
        vi.useFakeTimers();
        try {
            const { room, feed } = stubRoom();

            // Old connection: ids 0,1 pending; it drops (onclose path).
            const old0 = room.request("a", {}, { timeout: false });
            const old1 = room.request("b", {}, { timeout: false });
            (room as any).bumpConnectionEpoch();
            (room as any).rejectAllPending(new RequestClosedError("connection closed."));
            for (const p of [old0, old1]) {
                try { await p; assert.fail(); } catch (e) {
                    assert.instanceOf(e, RequestClosedError);
                }
            }

            // New transport comes up; ids keep climbing monotonically.
            (room as any).bumpConnectionEpoch();
            const fresh = room.request("c", {}, { timeout: false });

            // Frames drained from the dead socket are unmatched...
            feed(responseFrame(0, ResponseStatus.OK, "late 0"));
            feed(responseFrame(1, ResponseStatus.ERROR, { message: "late 1" }));
            // ...and the fresh round-trip on id 2 resolves normally.
            feed(responseFrame(2, ResponseStatus.OK, "fresh"));
            assert.equal(await fresh, "fresh");
        } finally {
            vi.useRealTimers();
        }
    });

    test("outbound frames: request id is monotonic; pinned requestId is used verbatim", () => {
        const { room, conn } = stubRoom();
        void room.request("one", {});
        void room.request("two", {}, { requestId: 12345 });

        assert.equal(readRequestId(conn.sent[0], Protocol.ROOM_REQUEST), 0);
        assert.equal(readRequestId(conn.sent[1], Protocol.ROOM_REQUEST), 12345);
    });

    test("backwards-compatible send(type, payload, callback) still resolves/rejects", async () => {
        const { room, feed } = stubRoom();

        const ok = await new Promise<any[]>((resolve) => {
            room.send("q", { x: 1 } as any, (response: any, error?: Error) =>
                resolve([response, error]));
            feed(responseFrame(0, ResponseStatus.OK, { answered: true }));
        });
        assert.deepEqual(ok[0], { answered: true });
        assert.isUndefined(ok[1]);

        const fail = await new Promise<any[]>((resolve) => {
            room.send("q2", {} as any, (response: any, error?: Error) =>
                resolve([response, error]));
            feed(responseFrame(1, ResponseStatus.REJECTED, "nope"));
        });
        assert.isUndefined(fail[0]);
        assert.instanceOf(fail[1], Error);
        assert.equal((fail[1] as RequestError).kind, "rejected");
    });
});
