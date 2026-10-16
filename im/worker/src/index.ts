import { DurableObject } from "cloudflare:workers";

/**
 * im-rv —— IM 的"仅协调"服务端,跑在 Cloudflare Workers 上。
 *
 * 它只做两件事:
 *   - POST /v1/register 记录某个 peer 加入的房间、昵称与其可拨号地址;
 *   - GET  /v1/discover 把同房间其它在线 peer 的地址返回给请求方。
 *
 * 服务器不转发、不存储任何聊天内容,聊天与媒体全部由客户端之间点对点直连。
 *
 * 注册表按房间放在 Durable Object 里:每个房间一个实例、单线程串行处理,
 * 因此不需要额外的锁。状态只放内存,DO 被回收后客户端下一次注册(最长 5 秒
 * 一个周期)即恢复。
 */

/** 与客户端 internal/wire 的 Msg JSON 形状保持一致。 */
interface PeerAddr {
  id: string;
  nick?: string;
  addrs: string[];
}

interface RegEntry extends PeerAddr {
  seen: number;
}

export interface Env {
  ROOMS: DurableObjectNamespace<RoomRegistry>;
}

const TTL_MS = 30_000; // 注册条目有效期,与客户端 5 秒的注册周期配套
const MAX_BODY = 8 << 10; // 请求体上限 8 KiB
const MAX_ROOM_LEN = 64;
const MAX_ID_LEN = 128;
const MAX_NICK_LEN = 64;
const MAX_ADDRS = 32;
const MAX_ADDR_LEN = 256;
const MAX_ROOM_PEERS = 256;

const ROOM_RE = /^[A-Za-z0-9._-]+$/;
const ID_RE = /^[A-Za-z0-9]+$/;

/** RoomRegistry 是单个房间的在线 peer 注册表。 */
export class RoomRegistry extends DurableObject<Env> {
  private peers = new Map<string, RegEntry>();

  /** 记录或刷新一个 peer。 */
  register(e: RegEntry): void {
    this.purge();
    if (!this.peers.has(e.id) && this.peers.size >= MAX_ROOM_PEERS) {
      this.evictOldest();
    }
    e.seen = Date.now();
    this.peers.set(e.id, e);
  }

  /** 返回同房间内其它在线 peer。 */
  discover(selfID: string): PeerAddr[] {
    this.purge();
    const out: PeerAddr[] = [];
    for (const [id, e] of this.peers) {
      if (id === selfID) {
        continue;
      }
      out.push({ id, nick: e.nick, addrs: e.addrs });
    }
    return out;
  }

  /** 清理超过 TTL 的条目。 */
  private purge(): void {
    const now = Date.now();
    for (const [id, e] of this.peers) {
      if (now - e.seen > TTL_MS) {
        this.peers.delete(id);
      }
    }
  }

  private evictOldest(): void {
    let oldestID: string | undefined;
    let oldestSeen = Number.POSITIVE_INFINITY;
    for (const [id, e] of this.peers) {
      if (e.seen < oldestSeen) {
        oldestSeen = e.seen;
        oldestID = id;
      }
    }
    if (oldestID !== undefined) {
      this.peers.delete(oldestID);
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    try {
      switch (pathname) {
        case "/v1/register":
          return await handleRegister(request, env);
        case "/v1/discover":
          return await handleDiscover(request, env);
        default:
          return json({ ok: false, error: "not found" }, 404);
      }
    } catch (err) {
      return json({ ok: false, error: `内部错误: ${err}` }, 500);
    }
  },
} satisfies ExportedHandler<Env>;

async function handleRegister(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") {
    return json({ ok: false, error: "只接受 POST" }, 405);
  }
  if (Number(request.headers.get("content-length") ?? "0") > MAX_BODY) {
    return json({ ok: false, error: "请求体过大" }, 413);
  }
  const text = await request.text();
  if (text.length > MAX_BODY) {
    return json({ ok: false, error: "请求体过大" }, 413);
  }

  let body: { id?: unknown; room?: unknown; nick?: unknown; addrs?: unknown };
  try {
    body = JSON.parse(text);
  } catch {
    return bad("请求体不是合法 JSON");
  }

  const id = typeof body.id === "string" ? body.id : "";
  if (!ID_RE.test(id) || id.length > MAX_ID_LEN) {
    return bad("id 非法");
  }
  const room = body.room === undefined || body.room === "" ? "lobby" : body.room;
  if (typeof room !== "string" || room.length > MAX_ROOM_LEN || !ROOM_RE.test(room)) {
    return bad("room 非法");
  }
  const nick = typeof body.nick === "string" ? body.nick.slice(0, MAX_NICK_LEN) : "";

  const raw = Array.isArray(body.addrs) ? body.addrs : [];
  for (const a of raw) {
    if (typeof a !== "string" || a.length > MAX_ADDR_LEN || !a.startsWith("/")) {
      return bad("addrs 非法");
    }
  }
  const addrs = (raw as string[]).slice(0, MAX_ADDRS);

  const stub = env.ROOMS.get(env.ROOMS.idFromName(room));
  await stub.register({ id, nick, addrs, seen: 0 });
  return json({ ok: true, room });
}

async function handleDiscover(request: Request, env: Env): Promise<Response> {
  if (request.method !== "GET") {
    return json({ ok: false, error: "只接受 GET" }, 405);
  }
  const query = new URL(request.url).searchParams;
  const room = query.get("room") || "lobby";
  if (room.length > MAX_ROOM_LEN || !ROOM_RE.test(room)) {
    return bad("room 非法");
  }
  const selfID = query.get("id") ?? "";
  if (!ID_RE.test(selfID) || selfID.length > MAX_ID_LEN) {
    return bad("id 非法");
  }

  const stub = env.ROOMS.get(env.ROOMS.idFromName(room));
  const peers = await stub.discover(selfID);
  return json({ ok: true, peers });
}

function json(v: unknown, status = 200): Response {
  return new Response(JSON.stringify(v), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function bad(message: string): Response {
  return json({ ok: false, error: message }, 400);
}
