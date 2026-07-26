import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawn } from "bun";

const TOKEN = "test-token-0123456789abcdef";
const PORT = 3155;
const WS_URL = `ws://127.0.0.1:${PORT}`;
const HTTP_URL = `http://127.0.0.1:${PORT}/`;

let relay: ReturnType<typeof spawn>;

/**
 * WebSocket client that queues every frame it receives, so tests can await
 * frames without racing the relay.
 */
class Peer {
  private frames: any[] = [];
  private waiters: ((frame: any) => void)[] = [];

  private constructor(private ws: WebSocket) {
    ws.onmessage = (event: MessageEvent) => {
      const frame = JSON.parse(event.data as string);
      const waiter = this.waiters.shift();
      if (waiter) waiter(frame);
      else this.frames.push(frame);
    };
  }

  static async connect(): Promise<Peer> {
    const ws = new WebSocket(WS_URL);
    await new Promise((resolve, reject) => {
      ws.onopen = resolve;
      ws.onerror = reject;
    });
    const peer = new Peer(ws);
    await peer.next(); // welcome frame
    return peer;
  }

  next(timeoutMs = 5000): Promise<any> {
    const queued = this.frames.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for frame")), timeoutMs);
      this.waiters.push((frame) => {
        clearTimeout(timer);
        resolve(frame);
      });
    });
  }

  send(payload: unknown) {
    this.ws.send(JSON.stringify(payload));
  }

  join(channel: string, token: string = TOKEN) {
    this.send({ type: "join", id: "req-1", channel, token });
    return this.next();
  }

  /** Join and consume the second confirmation frame sent on success. */
  async joinOk(channel: string) {
    const frame = await this.join(channel);
    await this.next();
    return frame;
  }

  close() {
    this.ws.close();
  }
}

function errorText(frame: any): string {
  return typeof frame.message === "string" ? frame.message : frame.message?.error;
}

beforeAll(async () => {
  relay = spawn({
    cmd: ["bun", "run", "src/socket.ts"],
    env: { ...process.env, PORT: String(PORT), FIGMA_SOCKET_TOKEN: TOKEN },
    stdout: "ignore",
    stderr: "inherit",
  });

  for (let i = 0; i < 50; i++) {
    try {
      (await Peer.connect()).close();
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("relay did not start");
});

afterAll(() => relay?.kill());

describe("relay authentication", () => {
  test("rejects join without a token", async () => {
    const peer = await Peer.connect();
    peer.send({ type: "join", channel: "chan0001" });
    expect(errorText(await peer.next())).toContain("Authentication failed");
  });

  test("rejects join with a wrong token", async () => {
    const peer = await Peer.connect();
    expect(errorText(await peer.join("chan0002", "not-the-token"))).toContain(
      "Authentication failed"
    );
  });

  test("accepts join with the shared token", async () => {
    const peer = await Peer.connect();
    const frame = await peer.join("chan0003");
    expect(frame.type).toBe("system");
    expect(frame.message).toBe("Joined channel: chan0003");
    peer.close();
  });

  test("rejects a third participant in a channel", async () => {
    const channel = "chan0004";
    const first = await Peer.connect();
    await first.joinOk(channel);
    const second = await Peer.connect();
    await second.joinOk(channel);

    const third = await Peer.connect();
    expect(errorText(await third.join(channel))).toContain("Channel is full");
    first.close();
    second.close();
  });

  test("rejects broadcasts from clients that never joined", async () => {
    const joined = await Peer.connect();
    await joined.joinOk("chan0005");

    const outsider = await Peer.connect();
    outsider.send({
      type: "message",
      channel: "chan0005",
      message: { command: "delete_node" },
    });
    expect(errorText(await outsider.next())).toContain("must join the channel first");
    joined.close();
    outsider.close();
  });

  test("relays messages between the two authenticated peers", async () => {
    const channel = "chan0006";
    const a = await Peer.connect();
    await a.joinOk(channel);
    const b = await Peer.connect();
    await b.joinOk(channel);

    a.send({ type: "message", channel, message: { command: "get_document_info" } });
    const frame = await b.next();
    expect(frame.type).toBe("broadcast");
    expect(frame.message.command).toBe("get_document_info");
    a.close();
    b.close();
  });
});

describe("origin allowlist", () => {
  test("allows Figma origins and requests without an Origin header", async () => {
    for (const headers of [{}, { Origin: "https://www.figma.com" }]) {
      const res = await fetch(HTTP_URL, { headers });
      expect(res.status).toBe(200);
    }
  });

  test("rejects unknown browser origins", async () => {
    const res = await fetch(HTTP_URL, { headers: { Origin: "http://evil.example" } });
    expect(res.status).toBe(403);
  });
});
