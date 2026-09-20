// @vitest-environment node
/**
 * Server-side RoomMessages boundary for request/reply:
 *
 * - pending-request cap (`Room.maxPendingRequests`) → BUSY, no handler run;
 * - duplicate requestId policy: allow / idempotent coalesce / reject;
 * - CANCEL frame releases the slot and aborts ctx.signal;
 * - client leave / room dispose clear pending (lifecycle boundary);
 * - replies are encoded ROOM_RESPONSE frames on the fake client's outbound
 *   queue, so status/payload can be asserted at the wire level.
 *
 * Drives RoomMessages directly with a duck-typed fake Room + fake Client —
 * no transport / matchmaker needed.
 */
import { EventEmitter } from 'node:events';
import { Buffer } from 'node:buffer';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { encode, decode, type Iterator } from '@colyseus/schema';
import { Packr } from 'msgpackr';

import { Protocol, ResponseStatus } from '@colyseus/shared-types';
import { RoomMessages, type DuplicateRequestPolicy } from '../src/RoomMessages.ts';

const packr = new Packr({ useRecords: false });

function requestFrame(requestId: number, type: string, payload?: any): Buffer {
  const header = Buffer.allocUnsafe(64);
  header[0] = Protocol.ROOM_REQUEST;
  const it: Iterator = { offset: 1 };
  encode.number(header, requestId, it);
  encode.string(header, type, it);
  const headLen = it.offset;
  if (payload === undefined) {
    return Buffer.from(header.subarray(0, headLen));
  }
  const body = packr.pack(payload);
  return Buffer.concat([header.subarray(0, headLen), body]);
}

function cancelFrame(requestId: number): Buffer {
  const buf = Buffer.allocUnsafe(16);
  buf[0] = Protocol.ROOM_REQUEST_CANCEL;
  const it: Iterator = { offset: 1 };
  encode.number(buf, requestId, it);
  return Buffer.from(buf.subarray(0, it.offset));
}

/** Decode an outbound ROOM_RESPONSE frame: `[id, status, payload?]`. */
function decodeResponse(bytes: Buffer): { id: number, status: number, payload?: any } {
  const it: Iterator = { offset: 1 };
  const id = decode.number(bytes, it);
  const status = bytes[it.offset++];
  const payload = bytes.byteLength > it.offset
    ? packr.unpack(bytes.subarray(it.offset))
    : undefined;
  return { id, status, payload };
}

interface FakeClient {
  sessionId: string;
  state: number;
  ref: EventEmitter;
  outbound: Buffer[];
  enqueueRaw: (data: Buffer) => void;
  leave: ReturnType<typeof vi.fn>;
  error: ReturnType<typeof vi.fn>;
}

function makeClient(sessionId = "session-a"): FakeClient {
  const client: any = {
    sessionId,
    state: 1,
    ref: new EventEmitter(),
    outbound: [] as Buffer[],
    enqueueRaw(data: Buffer) { this.outbound.push(Buffer.from(data)); },
    leave: vi.fn(),
    error: vi.fn(),
  };
  return client;
}

function makeRoom(opts?: {
  maxPendingRequests?: number;
  duplicateRequestPolicy?: DuplicateRequestPolicy;
  onUncaughtException?: any;
}) {
  const messages = new RoomMessages(undefined as any);
  const room: any = {
    roomId: "room-1",
    roomName: "test_room",
    maxPendingRequests: opts?.maxPendingRequests ?? Infinity,
    duplicateRequestPolicy: opts?.duplicateRequestPolicy ?? "allow",
    onUncaughtException: opts?.onUncaughtException,
  };
  (messages as any).room = room;
  return { messages, room };
}

function dispatch(messages: RoomMessages, client: FakeClient, buffer: Buffer) {
  const it: Iterator = { offset: 1 };
  const code = buffer[0];
  if (code === Protocol.ROOM_REQUEST) {
    messages.onRequest(client as any, buffer, it);
  } else if (code === Protocol.ROOM_REQUEST_CANCEL) {
    messages.onCancel(client as any, buffer, it);
  } else {
    throw new Error(`unexpected frame code ${code}`);
  }
}

describe("RoomMessages — request handling boundary", () => {
  it("replies OK with the handler's return value", () => {
    const { messages } = makeRoom();
    const client = makeClient();
    const off = messages.on("ping", () => "pong");

    dispatch(messages, client, requestFrame(0, "ping"));

    expect(client.outbound).toHaveLength(1);
    const reply = decodeResponse(client.outbound[0]);
    expect(reply).toEqual({ id: 0, status: ResponseStatus.OK, payload: "pong" });
    off();
  });

  it("surfaces ctx.reject as REJECTED and a thrown error as ERROR", () => {
    const { messages } = makeRoom();
    const client = makeClient();

    messages.on("nope", (_c: any, _m: any, ctx: any) => ctx.reject({ why: "denied" }));
    messages.on("boom", () => { throw new Error("kaboom"); });

    dispatch(messages, client, requestFrame(0, "nope"));
    dispatch(messages, client, requestFrame(1, "boom"));

    expect(decodeResponse(client.outbound[0])).toEqual({
      id: 0, status: ResponseStatus.REJECTED, payload: { why: "denied" },
    });
    expect(decodeResponse(client.outbound[1])).toMatchObject({
      id: 1, status: ResponseStatus.ERROR,
      payload: { name: "Error", message: "kaboom" },
    });
  });

  it("replies ERROR (no_handler) when no handler is registered", () => {
    const { messages } = makeRoom();
    const client = makeClient();

    dispatch(messages, client, requestFrame(7, "ghost"));

    const reply = decodeResponse(client.outbound[0]);
    expect(reply.id).toBe(7);
    expect(reply.status).toBe(ResponseStatus.ERROR);
    expect(reply.payload.name).toBe("no_handler");
  });

  it("async handler: resolves on a later tick with ctx.signal available", async () => {
    const { messages } = makeRoom();
    const client = makeClient();
    let signal: AbortSignal | undefined;

    messages.on("slow", (_c: any, _m: any, ctx: any) => {
      signal = ctx.signal;
      return new Promise((resolve) => setTimeout(() => resolve("done"), 10));
    });

    dispatch(messages, client, requestFrame(3, "slow"));
    expect(client.outbound).toHaveLength(0);
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal!.aborted).toBe(false);

    await vi.waitFor(() => expect(client.outbound).toHaveLength(1));
    expect(decodeResponse(client.outbound[0])).toEqual({
      id: 3, status: ResponseStatus.OK, payload: "done",
    });
  });
});

describe("RoomMessages — pending cap", () => {
  it("refuses with BUSY once the per-client cap is reached; slot frees on reply", async () => {
    const { messages } = makeRoom({ maxPendingRequests: 2 });
    const client = makeClient();
    const releases: Array<() => void> = [];
    messages.on("work", () => new Promise<void>((r) => { releases.push(r); }));
    messages.on("instant", () => "ok");

    // 2 in flight — at the cap.
    dispatch(messages, client, requestFrame(0, "work"));
    dispatch(messages, client, requestFrame(1, "work"));
    expect(client.outbound).toHaveLength(0);

    // 3rd is refused before the handler runs.
    dispatch(messages, client, requestFrame(2, "instant"));
    const busy = decodeResponse(client.outbound[0]);
    expect(busy).toMatchObject({ id: 2, status: ResponseStatus.BUSY });
    expect(busy.payload.limit).toBe(2);

    // One completes → its slot frees → a new request is accepted.
    releases[1]();
    await new Promise((r) => setTimeout(r, 0));
    expect(client.outbound.map(decodeResponse).find((r) => r.id === 1)?.status)
      .toBe(ResponseStatus.OK);

    dispatch(messages, client, requestFrame(3, "instant"));
    const all = client.outbound.map(decodeResponse);
    expect(all.some((r) => r.id === 3 && r.status === ResponseStatus.OK)).toBe(true);

    releases[0](); // cleanup
  });

  it("counts one slot per coalesced idempotent attempt", () => {
    const { messages } = makeRoom({ maxPendingRequests: 1, duplicateRequestPolicy: "idempotent" });
    const client = makeClient();
    messages.on("work", () => new Promise(() => {}));

    dispatch(messages, client, requestFrame(0, "work"));
    // duplicate id coalesces onto the single slot — NOT refused as busy.
    dispatch(messages, client, requestFrame(0, "work"));
    // a DIFFERENT id is refused: only one attempt slot exists.
    dispatch(messages, client, requestFrame(1, "work"));

    const statuses = client.outbound.map((b) => decodeResponse(b).status);
    expect(statuses).toEqual([ResponseStatus.BUSY]);
  });
});

describe("RoomMessages — duplicate requestId policy", () => {
  it("'allow' (default) dispatches both attempts and answers both independently", () => {
    const { messages } = makeRoom();
    const client = makeClient();
    const runs: number[] = [];
    messages.on("n", () => { runs.push(Date.now() + Math.random()); return runs.length; });

    dispatch(messages, client, requestFrame(5, "n"));
    dispatch(messages, client, requestFrame(5, "n"));

    expect(runs).toHaveLength(2);
    const replies = client.outbound.map(decodeResponse);
    expect(replies).toEqual([
      { id: 5, status: ResponseStatus.OK, payload: 1 },
      { id: 5, status: ResponseStatus.OK, payload: 2 },
    ]);
  });

  it("'reject' refuses an in-flight duplicate with DUPLICATE and runs the handler once", async () => {
    const { messages } = makeRoom({ duplicateRequestPolicy: "reject" });
    const client = makeClient();
    let runs = 0;
    // Must still be PENDING when the duplicate arrives — a settled request no
    // longer occupies its id, so a later retry legitimately re-runs.
    messages.on("n", () => new Promise((r) =>
      setTimeout(() => r(++runs), 5)));

    dispatch(messages, client, requestFrame(5, "n"));
    dispatch(messages, client, requestFrame(5, "n"));

    const dup = decodeResponse(client.outbound[0]);
    expect(dup).toMatchObject({
      id: 5, status: ResponseStatus.DUPLICATE, payload: { requestId: 5 },
    });

    await new Promise((r) => setTimeout(r, 10));
    expect(runs).toBe(1, "the duplicate must never dispatch a second handler run");
  });

  it("'reject' allows a retry AFTER the first attempt already settled", async () => {
    const { messages } = makeRoom({ duplicateRequestPolicy: "reject" });
    const client = makeClient();
    let runs = 0;
    messages.on("n", () => ++runs);

    dispatch(messages, client, requestFrame(5, "n"));
    dispatch(messages, client, requestFrame(5, "n"));

    expect(runs).toBe(2);
    expect(client.outbound.map((b) => decodeResponse(b).status))
      .toEqual([ResponseStatus.OK, ResponseStatus.OK]);
  });

  it("'idempotent' coalesces: one handler run, identical response fanned to both", async () => {
    const { messages } = makeRoom({ duplicateRequestPolicy: "idempotent" });
    const client = makeClient();
    let runs = 0;
    messages.on("charge", () => new Promise((r) => {
      setTimeout(() => r({ charge: ++runs, token: "t-1" }), 5);
    }));

    // Retry fired before the first attempt answered (same client + id).
    dispatch(messages, client, requestFrame(9, "charge"));
    dispatch(messages, client, requestFrame(9, "charge"));

    await new Promise((r) => setTimeout(r, 15));

    expect(runs).toBe(1, "the handler must execute exactly once");
    const replies = client.outbound.map(decodeResponse);
    expect(replies).toHaveLength(2);
    expect(replies[0]).toEqual(replies[1]);
    expect(replies[0]).toMatchObject({
      id: 9, status: ResponseStatus.OK, payload: { charge: 1, token: "t-1" },
    });
  });

  it("two different clients sharing a numeric id are tracked independently", () => {
    const { messages } = makeRoom({ duplicateRequestPolicy: "reject" });
    const a = makeClient("a");
    const b = makeClient("b");
    messages.on("n", () => "ok");

    dispatch(messages, a, requestFrame(0, "n"));
    dispatch(messages, b, requestFrame(0, "n"));

    expect(a.outbound).toHaveLength(1);
    expect(b.outbound).toHaveLength(1);
    expect(decodeResponse(b.outbound[0]).status).toBe(ResponseStatus.OK);
  });

  it("'allow': concurrent async same-id attempts both settle and leave no ghost slot", async () => {
    const { messages } = makeRoom({ maxPendingRequests: 2 });
    const client = makeClient();
    const releases: Array<() => void> = [];
    messages.on("work", () => new Promise<void>((r) => { releases.push(r); }));

    // Two genuinely concurrent attempts with the SAME id ("allow" default).
    dispatch(messages, client, requestFrame(4, "work"));
    dispatch(messages, client, requestFrame(4, "work"));
    // At the cap: a third (different id) is refused.
    dispatch(messages, client, requestFrame(5, "work"));
    expect(client.outbound.map(decodeResponse).map((r) => r.status))
      .toEqual([ResponseStatus.BUSY]);

    // Settle the SECOND attempt first (out of order).
    releases[1]();
    await new Promise((r) => setTimeout(r, 0));
    // One slot freed → id 5 is now accepted (and answers).
    messages.on("instant", () => "ok");
    dispatch(messages, client, requestFrame(6, "instant"));

    // Settle the FIRST attempt — no ghost entry may keep the slot occupied.
    releases[0]();
    await new Promise((r) => setTimeout(r, 0));

    const replies = client.outbound.map(decodeResponse);
    expect(replies.filter((r) => r.id === 4 && r.status === ResponseStatus.OK))
      .toHaveLength(2);
    expect(replies.some((r) => r.id === 6 && r.status === ResponseStatus.OK)).toBe(true);
    expect(replies.filter((r) => r.status === ResponseStatus.BUSY)).toHaveLength(1);
  });
});

describe("RoomMessages — CANCEL + lifecycle cleanup", () => {
  it("CANCEL frees the slot, aborts ctx.signal, and suppresses the late reply", async () => {
    const { messages } = makeRoom({ maxPendingRequests: 1 });
    const client = makeClient();
    let aborted = false;
    messages.on("work", (_c: any, _m: any, ctx: any) =>
      new Promise((_resolve, reject) => {
        ctx.signal.addEventListener("abort", () => {
          aborted = true;
          reject(new Error("aborted"));
        });
      }));

    dispatch(messages, client, requestFrame(0, "work"));
    dispatch(messages, client, cancelFrame(0));

    expect(aborted).toBe(true);

    // Slot is free again: a fresh request is accepted (would have been BUSY).
    messages.on("ping", () => "pong");
    dispatch(messages, client, requestFrame(1, "ping"));
    const reply = decodeResponse(client.outbound[0]);
    expect(reply).toEqual({ id: 1, status: ResponseStatus.OK, payload: "pong" });

    // Let the cancelled promise's microtasks run; nothing for id 0 ships.
    await new Promise((r) => setTimeout(r, 5));
    expect(client.outbound.every((b) => decodeResponse(b).id !== 0)).toBe(true);
  });

  it("onClientLeave aborts + clears all of the client's pending requests", () => {
    const { messages } = makeRoom();
    const client = makeClient();
    const signals: AbortSignal[] = [];
    messages.on("work", (_c: any, _m: any, ctx: any) => {
      signals.push(ctx.signal);
      return new Promise(() => {});
    });

    dispatch(messages, client, requestFrame(0, "work"));
    dispatch(messages, client, requestFrame(1, "work"));
    expect(signals).toHaveLength(2);
    expect(signals.every((s) => !s.aborted)).toBe(true);

    messages.onClientLeave(client as any);

    expect(signals.every((s) => s.aborted)).toBe(true);
    // No replies are attempted on a dead transport.
    expect(client.outbound).toHaveLength(0);

    // Internal map is gone: cancelling afterwards is a no-op (no throw).
    expect(() => dispatch(messages, client, cancelFrame(0))).not.toThrow();
  });

  it("a settled request doesn't leak its registry slot", async () => {
    const { messages } = makeRoom({ maxPendingRequests: 1 });
    const client = makeClient();
    messages.on("instant", () => "ok");

    dispatch(messages, client, requestFrame(0, "instant"));
    expect(decodeResponse(client.outbound[0]).status).toBe(ResponseStatus.OK);

    // Second request must fit: the first slot was released at settle.
    dispatch(messages, client, requestFrame(1, "instant"));
    expect(client.outbound).toHaveLength(2);
    expect(decodeResponse(client.outbound[1]).id).toBe(1);
  });
});
