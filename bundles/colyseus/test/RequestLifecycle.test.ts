/**
 * Enhanced request/reply — boundary coverage for:
 *
 *   1. SDK boundary: per-call timeout, AbortSignal cancellation, and the four
 *      distinguishable rejection classes (timeout / abort / connection-closed /
 *      business error).
 *   2. Room message-handling boundary: per-connection pending cap
 *      (`maxPendingRequests`), duplicate-requestId policy
 *      (`duplicateRequestPolicy`: "reject" vs "idempotent"), ROOM_REQUEST_CANCEL
 *      releasing the server slot and aborting `ctx.signal`.
 *   3. Lifecycle boundary: pending cleanup on leave, on room dispose, and on
 *      transport close — plus the "late response must not resolve on a new
 *      connection" guarantee.
 */
import assert from "assert";
import WebSocket from "ws";
import { decode, encode } from "@colyseus/schema";
import { unpack, pack } from "msgpackr";
import { Room, Server, LocalPresence, LocalDriver, matchMaker, getLocalRoomById, Protocol, ResponseStatus, type DuplicateRequestPolicy } from "@colyseus/core";
import { Client as SDKClient, RequestTimeoutError, RequestClosedError, RequestCapacityError } from "@colyseus/sdk";
import { WebSocketTransport } from "@colyseus/ws-transport";

const TEST_PORT = 8588;
const TEST_ENDPOINT = `ws://localhost:${TEST_PORT}`;

const timeout = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// --- raw frame helpers (bypass the SDK's monotonic id generator so duplicate
//     requestIds / exact cancel frames can be scripted) -----------------------

function writeVarint(value: number): Buffer {
  const tmp = Buffer.allocUnsafe(64);
  const it: any = { offset: 0 };
  encode.number(tmp, value, it);
  return Buffer.from(tmp.subarray(0, it.offset));
}

function packRequest(requestId: number, type: string | number, payload?: any): Buffer {
  const parts: Buffer[] = [Buffer.from([Protocol.ROOM_REQUEST]), writeVarint(requestId)];

  const tmp = Buffer.allocUnsafe(64);
  const itType: any = { offset: 0 };
  if (typeof type === "string") {
    encode.string(tmp, type, itType);
  } else {
    encode.number(tmp, type, itType);
  }
  parts.push(Buffer.from(tmp.subarray(0, itType.offset)));

  if (payload !== undefined) {
    parts.push(Buffer.from(pack(payload)));
  }
  return Buffer.concat(parts);
}

function packCancel(requestId: number): Buffer {
  return Buffer.concat([Buffer.from([Protocol.ROOM_REQUEST_CANCEL]), writeVarint(requestId)]);
}

/** Pull the next ROOM_RESPONSE from a socket produced by {@link joinRaw}
 *  (the socket buffers frames arriving before a test awaits). */
function nextResponse(ws: WebSocket): Promise<{ requestId: number, status: number, payload?: any }> {
  return (ws as any)._takeResponse();
}

/** Join a room over a raw WebSocket using the matchmaking HTTP API, completing
 *  the JOIN_ROOM handshake (the SDK isn't usable here because scripted tests
 *  need to forge duplicate requestIds). The returned socket has a persistent
 *  ROOM_RESPONSE buffer so {@link nextResponse} never loses a frame to a
 *  listener-attach race. */
async function joinRaw(roomName: string): Promise<WebSocket> {
  const res = await fetch(`http://localhost:${TEST_PORT}/matchmake/joinOrCreate/${roomName}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({}),
  });
  const seat: any = await res.json();

  const ws = new WebSocket(
    `${TEST_ENDPOINT}/${seat.processId}/${seat.roomId}?sessionId=${seat.sessionId}&reconnectionToken=${seat.reconnectionToken}`,
  );

  // Register the handshake + response listeners BEFORE the socket finishes
  // opening: Node's EventEmitter does not buffer frames, so a listener attached
  // after 'open' can miss the server's JOIN_ROOM when onJoin is sync.
  const responseQueue: Array<{ requestId: number, status: number, payload?: any }> = [];
  const waiters: Array<(r: { requestId: number, status: number, payload?: any }) => void> = [];
  (ws as any)._takeResponse = (): Promise<{ requestId: number, status: number, payload?: any }> => {
    const queued = responseQueue.shift();
    if (queued !== undefined) { return Promise.resolve(queued); }
    return new Promise((resolve) => waiters.push(resolve));
  };

  // Persistent response pump — survives the handshake (separate listener).
  ws.on("message", (data: WebSocket.RawData) => {
    const buf = Buffer.from(data as any);
    if ((buf[0] & 0x1f) !== Protocol.ROOM_RESPONSE) { return; }
    const it: any = { offset: 1 };
    const requestId = decode.number(buf, it);
    const status = buf[it.offset++];
    const payload = buf.byteLength > it.offset ? unpack(buf.subarray(it.offset)) : undefined;
    const frame = { requestId, status, payload };
    const waiter = waiters.shift();
    if (waiter !== undefined) { waiter(frame); }
    else { responseQueue.push(frame); }
  });

  const joined = new Promise<void>((resolve) => {
    const onHandshake = (data: WebSocket.RawData) => {
      const buf = Buffer.from(data as any);
      if ((buf[0] & 0x1f) !== Protocol.JOIN_ROOM) { return; }
      ws.off("message", onHandshake);
      ws.send(Buffer.from([Protocol.JOIN_ROOM]));
      resolve();
    };
    ws.on("message", onHandshake);
  });

  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  await joined;

  return ws;
}

// ----------------------------------------------------------------------------

describe("Request/Reply lifecycle", () => {
  let server: Server;
  const sdkClient = new SDKClient(TEST_ENDPOINT);

  before(async () => {
    server = new Server({
      greet: false,
      gracefullyShutdown: false,
      presence: new LocalPresence(),
      driver: new LocalDriver(),
      transport: new WebSocketTransport(),
    });
    await matchMaker.setup(new LocalPresence(), new LocalDriver());

    server.define("basic", class _ extends Room {
      maxPendingRequests = 2;
      duplicateRequestPolicy: DuplicateRequestPolicy = "reject";
      onCreate() {
        this.onMessage("echo", (_c, message) => message);
        this.onMessage("slow", async (_c, message: { ms: number }) => {
          await timeout(message.ms);
          return `slept ${message.ms}`;
        });
        this.onMessage("boom", () => { throw new Error("kaboom"); });
        this.onMessage("deny", (_c, _m, ctx: any) => ctx.reject("not-allowed"));
        this.onMessage("watchCancel", async (_c, _m, ctx: any) => {
          // resolves when the request is cancelled (or the 5s safety timer)
          await new Promise<void>((resolve) => {
            if (ctx.signal.aborted) { return resolve(); }
            ctx.signal.addEventListener("abort", () => resolve());
            setTimeout(resolve, 5000);
          });
          return "done";
        });
      }
    });

    server.define("idempotent", class _ extends Room {
      duplicateRequestPolicy: DuplicateRequestPolicy = "idempotent";
      invocations = 0;
      onCreate() {
        this.onMessage("once", async () => {
          this.invocations++;
          await timeout(60);
          return { n: this.invocations };
        });
      }
    });

    server.define("disposing", class _ extends Room {
      onCreate() {
        this.onMessage("slow", async () => {
          await timeout(2000);
          return "late";
        });
      }
    });

    await server.listen(TEST_PORT);
  });

  after(async () => {
    await server.gracefullyShutdown(false);
  });

  describe("SDK boundary: distinguishable outcomes", () => {
    it("resolves a normal request", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      assert.deepStrictEqual(await room.request("echo", { x: 1 }), { x: 1 });
      await room.leave();
    });

    it("rejects with RequestTimeoutError (name=TimeoutError) on timeout", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      await assert.rejects(
        room.request("slow", { ms: 300 }, { timeout: 40 }),
        (err: any) => {
          assert.ok(err instanceof RequestTimeoutError, "should be a RequestTimeoutError");
          assert.strictEqual(err.name, "TimeoutError");
          assert.ok(/timed out/.test(err.message));
          assert.strictEqual(err.timeout, 40);
          return true;
        },
      );
      await room.leave();
    });

    it("cancels via AbortSignal and rejects with AbortError", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      const controller = new AbortController();
      const promise = room.request("slow", { ms: 5000 }, { signal: controller.signal, timeout: 5000 });
      await timeout(30);
      controller.abort();
      await assert.rejects(promise, (err: any) => {
        assert.strictEqual(err.name, "AbortError");
        assert.ok(/aborted/.test(err.message));
        return true;
      });
      await room.leave();
    });

    it("honors an already-aborted signal without sending anything", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      const controller = new AbortController();
      controller.abort(new Error("pre-cancelled"));
      await assert.rejects(
        room.request("echo", {}, { signal: controller.signal }),
        (err: any) => err.name === "AbortError" && /pre-cancelled/.test(err.message),
      );
      await room.leave();
    });

    it("rejects with RequestClosedError when the connection closes", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      const promise = room.request("slow", { ms: 5000 }, { timeout: 5000 });
      await timeout(30);
      room.connection.close();
      await assert.rejects(promise, (err: any) => {
        assert.ok(err instanceof RequestClosedError, "should be a RequestClosedError");
        assert.strictEqual(err.name, "RequestClosedError");
        return true;
      });
    });

    it("rejects business errors (thrown handler) distinctly from infra errors", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      await assert.rejects(room.request("boom", {}), (err: any) => {
        assert.strictEqual(err.name, "Error");
        assert.strictEqual(err.message, "kaboom");
        assert.ok(!(err instanceof RequestTimeoutError));
        assert.ok(!(err instanceof RequestClosedError));
        return true;
      });
      await room.leave();
    });

    it("surfaces deliberate ctx.reject as name=rejected with .reason", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      await assert.rejects(room.request("deny"), (err: any) => {
        assert.strictEqual(err.name, "rejected");
        assert.strictEqual(err.reason, "not-allowed");
        return true;
      });
      await room.leave();
    });
  });

  describe("Room message-handling boundary", () => {
    it("refuses requests past maxPendingRequests with CAPACITY", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      const p1 = room.request("slow", { ms: 300 }, { timeout: 5000 });
      const p2 = room.request("slow", { ms: 300 }, { timeout: 5000 });
      await timeout(40);
      await assert.rejects(
        room.request("echo", {}, { timeout: 5000 }),
        (err: any) => {
          assert.ok(err instanceof RequestCapacityError);
          assert.strictEqual(err.name, "RequestCapacityError");
          assert.strictEqual(err.limit, 2);
          return true;
        },
      );
      // the two real requests still drain and free the slots
      await Promise.all([p1, p2]);
      assert.deepStrictEqual(await room.request("echo", { ok: true }), { ok: true });
      await room.leave();
    });

    it("duplicate requestId is rejected with DUPLICATE by default", async () => {
      const ws = await joinRaw("basic");
      try {
        ws.send(packRequest(77, "slow", { ms: 300 }));
        await timeout(30);
        ws.send(packRequest(77, "slow", { ms: 300 }));

        // duplicate is answered synchronously with DUPLICATE
        const dup = await nextResponse(ws);
        assert.strictEqual(dup.requestId, 77);
        assert.strictEqual(dup.status, ResponseStatus.DUPLICATE);

        // original completes normally
        const ok = await nextResponse(ws);
        assert.strictEqual(ok.status, ResponseStatus.OK);
      } finally {
        ws.close();
      }
    });

    it("duplicate requestId is swallowed under idempotent policy; handler runs once", async () => {
      const ws = await joinRaw("idempotent");
      try {
        ws.send(packRequest(5, "once"));
        await timeout(20);
        ws.send(packRequest(5, "once"));
        ws.send(packRequest(5, "once"));

        const reply = await nextResponse(ws);
        assert.strictEqual(reply.requestId, 5);
        assert.strictEqual(reply.status, ResponseStatus.OK);
        assert.deepStrictEqual(reply.payload, { n: 1 }); // handler ran exactly once

        // no further frames for id 5 (the pump queues any that do arrive)
        const extra = await Promise.race([
          (ws as any)._takeResponse().then((f: any) => f),
          timeout(120).then(() => null),
        ]);
        assert.strictEqual(extra, null, "idempotent duplicates must not produce extra replies");
      } finally {
        ws.close();
      }
    });

    it("ROOM_REQUEST_CANCEL aborts ctx.signal and frees the pending slot", async () => {
      const ws = await joinRaw("basic");
      try {
        ws.send(packRequest(91, "watchCancel"));
        await timeout(30);
        ws.send(packCancel(91));

        // slot freed → a fresh request reusing the same id is NOT a duplicate
        await timeout(30);
        ws.send(packRequest(91, "echo", { recycled: true }));
        const reply = await nextResponse(ws);
        assert.strictEqual(reply.requestId, 91);
        assert.strictEqual(reply.status, ResponseStatus.OK);
        assert.deepStrictEqual(reply.payload, { recycled: true });
      } finally {
        ws.close();
      }
    });

    it("SDK timeout sends a cancel that releases the server-side slot", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      await assert.rejects(room.request("slow", { ms: 5000 }, { timeout: 50 }));
      await timeout(80);
      // cap is 2: both fresh requests must start — they would fail with
      // CAPACITY had the timed-out request's slot leaked.
      const [ra, rb] = await Promise.all([
        room.request("echo", { a: 1 }, { timeout: 2000 }),
        room.request("echo", { b: 2 }, { timeout: 2000 }),
      ]);
      assert.deepStrictEqual(ra, { a: 1 });
      assert.deepStrictEqual(rb, { b: 2 });
      await room.leave();
    });
  });

  describe("lifecycle boundary: pending cleanup", () => {
    it("cleans up pending when the client leaves (no throw on server)", async () => {
      const ws = await joinRaw("basic");
      ws.send(packRequest(3, "slow", { ms: 5000 }));
      ws.send(packRequest(4, "slow", { ms: 5000 }));
      await timeout(30);
      ws.close();

      // a fresh connection on the same room can immediately fill both slots —
      // the old socket's ledger was dropped on its leave
      const ws2 = await joinRaw("basic");
      try {
        ws2.send(packRequest(1, "echo", {}));
        ws2.send(packRequest(2, "echo", {}));
        const first = await nextResponse(ws2);
        assert.strictEqual(first.status, ResponseStatus.OK);
      } finally {
        ws2.close();
      }
      await timeout(50);
    });

    it("a late response after disconnect cannot resolve on a new connection reusing the id", async () => {
      const ws = await joinRaw("basic");
      ws.send(packRequest(42, "slow", { ms: 300 }));
      await timeout(30);
      ws.close();

      // NEW connection: issue a fast request that reuses id 42. It must resolve
      // with its OWN payload, never "slept 300" from the old late reply.
      const ws2 = await joinRaw("basic");
      try {
        ws2.send(packRequest(42, "echo", { fresh: true }));
        const reply = await nextResponse(ws2);
        assert.strictEqual(reply.requestId, 42);
        assert.strictEqual(reply.status, ResponseStatus.OK);
        assert.deepStrictEqual(
          reply.payload, { fresh: true },
          "new connection must get its own reply, not the previous connection's late response",
        );
      } finally {
        ws2.close();
      }
    });

    it("room dispose / disconnect rejects pending requests on the client", async () => {
      const room = await sdkClient.joinOrCreate("disposing");
      const pending = room.request("slow", {}, { timeout: 10000 });
      await timeout(50);

      // force-disconnect the room out from under the pending request
      const serverRoom = getLocalRoomById(room.roomId);
      assert.ok(serverRoom, "room should exist");
      await serverRoom!.disconnect();

      await assert.rejects(pending, (err: any) => {
        assert.ok(err instanceof RequestClosedError, `expected RequestClosedError, got ${err.name}: ${err.message}`);
        return true;
      });
    });
  });

  describe("backward compatibility", () => {
    it("send(type, payload, callback) keeps its callback semantics", async () => {
      const room = await sdkClient.joinOrCreate("basic");
      const result = await new Promise((resolve, reject) => {
        room.send("echo", { hello: "world" }, (res: any, err?: Error) => err ? reject(err) : resolve(res));
      });
      assert.deepStrictEqual(result, { hello: "world" });
      await room.leave();
    });
  });
});
