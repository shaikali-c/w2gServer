import type { ServerWebSocket } from "bun";

type WS = ServerWebSocket<unknown>;

/*
 * Protocol (JSON over /ws, always { event, data }):
 *
 *  C -> S  JOIN    { name: string }
 *  S -> C  SYNC    { currentTime: number, isPlaying: boolean, users: string[], you: string, seq: number, serverTime: number }
 *  S -> ALL JOIN   { name: string, users: string[] }
 *  S -> ALL LEFT   { name: string, users: string[] }
 *
 *  C -> S  PLAY    { name: string, currentTime?: number }  // clients SHOULD send currentTime; server falls back to its clock
 *  S -> ALL PLAY   { currentTime: number, isPlaying: true, by: string, seq: number, serverTime: number }
 *
 *  C -> S  PAUSE   { name: string, currentTime?: number }  // clients SHOULD send currentTime; server falls back to its clock
 *  S -> ALL PAUSE  { currentTime: number, isPlaying: false, by: string, seq: number, serverTime: number }
 *
 *  C -> S  SEEK    { name: string, currentTime: number }
 *  S -> ALL SEEK   { currentTime: number, isPlaying: boolean, by: string, seq: number, serverTime: number }
 *
 *  C -> S  MESSAGE { name: string, text: string }
 *  S -> OTHERS MESSAGE { from: string, text: string, at: number }
 *
 *  C -> S  TYPING  { name: string, isTyping: boolean }
 *  S -> OTHERS TYPING { name: string, isTyping: boolean }
 *
 *  C -> S  PING    {}
 *  S -> C  PONG    { serverTime: number }
 *
 *  S -> C  ERROR   { message: string }
 *
 * Playback events (PLAY/PAUSE/SEEK) and SYNC carry a monotonic `seq`
 * (incremented on every PLAY/PAUSE/SEEK state change) plus `serverTime`
 * (Date.now() at broadcast) so peers can dedupe, order, and latency-compensate.
 */

// ---------- Config ----------
const args = Bun.argv.slice(2);
const videoPath = args[0];
const subtitlePath = args[1] ?? "subs.vtt";
const PORT = Number(process.env.PORT ?? 8080);

if (!videoPath) {
  console.error("Usage: bun server.ts <video-file> [subtitles-file]");
  process.exit(1);
}

const videoFile = Bun.file(videoPath);
if (!(await videoFile.exists())) {
  console.error(`Video file not found: ${videoPath}`);
  process.exit(1);
}

// ---------- State ----------
const nameToWs = new Map<string, WS>();
const wsToName = new Map<WS, string>();

let currentVideoTime = 0;
let isPlaying = false;
let lastRealTimeAnchor = Date.now();
// Monotonic sequence number, bumped on every PLAY/PAUSE/SEEK state change (S1).
let playbackSeq = 0;

function getLiveVideoTime(): number {
  if (!isPlaying) return currentVideoTime;
  return currentVideoTime + (Date.now() - lastRealTimeAnchor) / 1000;
}

const PLAYBACK_EPSILON = 0.05;

function nearlyEqual(a: number, b: number, epsilon = PLAYBACK_EPSILON) {
  return Math.abs(a - b) <= epsilon;
}

function users(): string[] {
  return [...nameToWs.keys()];
}

// ---------- Rate limiting (token bucket per socket, S5) ----------
const RATE_CAPACITY = 20;
const RATE_REFILL_PER_SEC = 10;
const rateBuckets = new Map<WS, { tokens: number; last: number }>();

function checkRateLimit(ws: WS): boolean {
  const now = Date.now();
  let bucket = rateBuckets.get(ws);
  if (!bucket) {
    bucket = { tokens: RATE_CAPACITY, last: now };
    rateBuckets.set(ws, bucket);
  }
  const elapsed = Math.max(0, (now - bucket.last) / 1000);
  bucket.tokens = Math.min(
    RATE_CAPACITY,
    bucket.tokens + elapsed * RATE_REFILL_PER_SEC,
  );
  bucket.last = now;
  if (bucket.tokens < 1) return false;
  bucket.tokens -= 1;
  return true;
}

// ---------- Send helpers ----------
function safeSend(ws: WS, payload: unknown): void {
  try {
    ws.send(JSON.stringify(payload));
  } catch {
    // Socket is dead — cleanup happens on close.
    cleanup(ws);
  }
}

function broadcast(except: WS | null, payload: unknown): void {
  const raw = JSON.stringify(payload);
  for (const client of nameToWs.values()) {
    if (client === except) continue;
    try {
      client.send(raw);
    } catch {
      cleanup(client);
    }
  }
}

function sendError(ws: WS, message: string): void {
  safeSend(ws, { event: "ERROR", data: { message } });
}

// Unified playback broadcast: every PLAY/PAUSE/SEEK goes through here so
// peers always get a monotonic seq + serverTime for ordering/dedupe
// and latency compensation (S1/S2/S3).
function broadcastPlayback(
  except: WS | null,
  event: "PLAY" | "PAUSE" | "SEEK",
  data: { currentTime: number; isPlaying: boolean; by: string },
): void {
  broadcast(except, {
    event,
    data: { ...data, seq: playbackSeq, serverTime: Date.now() },
  });
}

function syncPayload(name: string): unknown {
  return {
    event: "SYNC",
    data: {
      currentTime: getLiveVideoTime(),
      isPlaying,
      users: users(),
      you: name,
      seq: playbackSeq,
      serverTime: Date.now(),
    },
  };
}

function cleanup(ws: WS): void {
  const name = wsToName.get(ws);
  if (!name) {
    rateBuckets.delete(ws);
    return;
  }
  wsToName.delete(ws);
  rateBuckets.delete(ws);
  // Only delete the name mapping if it still points at this socket
  // (prevents a stale close from evicting a reconnected user).
  if (nameToWs.get(name) === ws) {
    nameToWs.delete(name);
    broadcast(null, { event: "LEFT", data: { name, users: users() } });
    console.log(`[leave] ${name} (${nameToWs.size} online)`);
  }
}

// ---------- Validation ----------
function asName(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const name = value.trim().slice(0, 32);
  return name.length >= 1 ? name : null;
}

function asFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function isStaleSocket(ws: WS): boolean {
  try {
    const state = (ws as WS & { readyState?: unknown }).readyState;
    return state === 2 || state === 3; // CLOSING / CLOSED
  } catch {
    return false;
  }
}

function senderName(ws: WS, claimed: unknown): string | null {
  const actual = wsToName.get(ws);
  const name = asName(claimed);
  if (!actual) return null;
  // Prevent spoofing: the claimed name must match the JOINed name.
  if (name !== actual) return null;
  return actual;
}

// ---------- HTTP (movie + subtitles with Range support) ----------
function contentTypeFor(path: string, fallback: string): string {
  const lower = path.toLowerCase();
  if (lower.endsWith(".mp4")) return "video/mp4";
  if (lower.endsWith(".mkv")) return "video/x-matroska";
  if (lower.endsWith(".webm")) return "video/webm";
  if (lower.endsWith(".vtt")) return "text/vtt; charset=utf-8";
  if (lower.endsWith(".srt")) return "application/x-subrip; charset=utf-8";
  return fallback;
}

const baseCors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "*",
};

async function serveFileWithRange(
  req: Request,
  path: string,
  fallbackType: string,
): Promise<Response> {
  const file = Bun.file(path);
  if (!(await file.exists())) return new Response("Not Found", { status: 404 });

  const size = file.size;
  const type = contentTypeFor(path, fallbackType);
  const range = req.headers.get("range");

  if (!range) {
    return new Response(file, {
      status: 200,
      headers: {
        ...baseCors,
        "Content-Type": type,
        "Content-Length": String(size),
        "Accept-Ranges": "bytes",
      },
    });
  }

  const match = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
  if (!match) {
    return new Response("Range Not Satisfiable", {
      status: 416,
      headers: {
        ...baseCors,
        "Content-Range": `bytes */${size}`,
        "Accept-Ranges": "bytes",
      },
    });
  }

  let start = match[1] === "" ? NaN : Number(match[1]);
  let end = match[2] === "" ? NaN : Number(match[2]);

  if (Number.isNaN(start) && Number.isNaN(end)) {
    return new Response("Range Not Satisfiable", {
      status: 416,
      headers: {
        ...baseCors,
        "Content-Range": `bytes */${size}`,
        "Accept-Ranges": "bytes",
      },
    });
  }
  if (Number.isNaN(start)) {
    // Suffix range: last N bytes.
    start = Math.max(0, size - end);
    end = size - 1;
  } else if (Number.isNaN(end)) {
    end = size - 1;
  }

  if (start < 0 || end < start || start >= size) {
    return new Response("Range Not Satisfiable", {
      status: 416,
      headers: {
        ...baseCors,
        "Content-Range": `bytes */${size}`,
        "Accept-Ranges": "bytes",
      },
    });
  }
  end = Math.min(end, size - 1);

  return new Response(file.slice(start, end + 1), {
    status: 206,
    headers: {
      ...baseCors,
      "Content-Type": type,
      "Content-Range": `bytes ${start}-${end}/${size}`,
      "Content-Length": String(end - start + 1),
      "Accept-Ranges": "bytes",
    },
  });
}

// ---------- Server ----------
Bun.serve({
  hostname: "0.0.0.0",
  port: PORT,
  async fetch(req, server) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: baseCors });
    }

    if (url.pathname === "/ws") {
      const ok = server.upgrade(req);
      if (ok) return undefined;
      return new Response("WebSocket upgrade failed", { status: 400 });
    }

    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        users: nameToWs.size,
        currentTime: getLiveVideoTime(),
        isPlaying,
        seq: playbackSeq,
        serverTime: Date.now(),
      });
    }

    if (url.pathname === "/movie") {
      return serveFileWithRange(req, videoPath, "video/mp4");
    }

    if (url.pathname === "/subtitles") {
      return serveFileWithRange(req, subtitlePath, "text/vtt; charset=utf-8");
    }

    return new Response("Not Found", { status: 404 });
  },

  websocket: {
    // Dead sockets are reaped after 60s idle; Bun also sends protocol pings (S6).
    idleTimeout: 60,
    sendPings: true,
    open(_ws: WS) {
      // Identity is established on JOIN; nothing to do yet.
    },

    message(ws: WS, message) {
      let msg: { event?: unknown; data?: Record<string, unknown> };
      try {
        const parsed: unknown =
          typeof message === "string"
            ? JSON.parse(message)
            : JSON.parse(String(message));
        if (typeof parsed !== "object" || parsed === null)
          throw new Error("not an object");
        msg = parsed as typeof msg;
      } catch {
        sendError(ws, "Invalid JSON payload");
        return;
      }

      if (typeof msg.event !== "string") {
        sendError(ws, "Missing event");
        return;
      }

      // App-level heartbeat is exempt from rate limiting (S6).
      if (msg.event === "PING") {
        safeSend(ws, { event: "PONG", data: { serverTime: Date.now() } });
        return;
      }
      if (msg.event === "PONG") return;

      if (!checkRateLimit(ws)) {
        sendError(ws, "Rate limited: slow down");
        return;
      }

      const data = (msg.data ?? {}) as Record<string, unknown>;

      switch (msg.event) {
        case "JOIN": {
          const name = asName(data.name);
          if (!name) {
            sendError(ws, "JOIN requires data.name (1-32 chars)");
            return;
          }

          // Re-JOIN with the same socket + same name is idempotent.
          if (wsToName.get(ws) === name) {
            safeSend(ws, syncPayload(name));
            return;
          }

          // If this socket had a previous name, release it first.
          const prev = wsToName.get(ws);
          if (prev && prev !== name) {
            wsToName.delete(ws);
            if (nameToWs.get(prev) === ws) {
              nameToWs.delete(prev);
              broadcast(null, {
                event: "LEFT",
                data: { name: prev, users: users() },
              });
            }
          }

          // Enforce unique names, evicting dead sockets (S7).
          const taken = nameToWs.get(name);
          if (taken && taken !== ws) {
            if (isStaleSocket(taken)) {
              cleanup(taken);
              try {
                taken.close();
              } catch {
                // Already dead — cleanup() handled the maps.
              }
            } else {
              sendError(ws, `Name "${name}" is already taken`);
              return;
            }
          }

          nameToWs.set(name, ws);
          wsToName.set(ws, name);
          console.log(`[join] ${name} (${nameToWs.size} online)`);

          broadcast(ws, { event: "JOIN", data: { name, users: users() } });
          safeSend(ws, syncPayload(name));
          break;
        }

        case "MESSAGE": {
          const from = senderName(ws, data.name);

          if (!from) {
            sendError(ws, "Send JOIN first (and use your own name)");
            return;
          }

          const text =
            typeof data.text === "string"
              ? data.text.trim().slice(0, 2000)
              : "";

          if (!text) return;

          // Client-generated ID so the sender and receiver refer to
          // the exact same message when replying to it.
          const id =
            typeof data.id === "string" && data.id.length > 0
              ? data.id.slice(0, 100)
              : crypto.randomUUID();

          // Preserve reply information if supplied.
          let replyTo:
            | {
                id: string;
                from: string;
                text: string;
              }
            | undefined;

          if (typeof data.replyTo === "object" && data.replyTo !== null) {
            const r = data.replyTo as Record<string, unknown>;

            if (
              typeof r.id === "string" &&
              typeof r.from === "string" &&
              typeof r.text === "string"
            ) {
              replyTo = {
                id: r.id.slice(0, 100),
                from: r.from.slice(0, 32),
                text: r.text.slice(0, 200),
              };
            }
          }

          const message = {
            id,
            from,
            text,
            at: Date.now(),
            ...(replyTo ? { replyTo } : {}),
          };

          broadcast(ws, {
            event: "MESSAGE",
            data: message,
          });

          break;
        }

        case "PLAY": {
          const by = senderName(ws, data.name);

          if (!by) {
            sendError(ws, "Send JOIN first (and use your own name)");
            return;
          }

          const t = asFiniteNumber(data.currentTime);
          const nextTime = t !== null ? Math.max(0, t) : getLiveVideoTime();

          const liveTime = getLiveVideoTime();

          // Already playing at essentially the same position.
          if (isPlaying && nearlyEqual(nextTime, liveTime, 0.15)) {
            return;
          }

          currentVideoTime = nextTime;
          isPlaying = true;
          lastRealTimeAnchor = Date.now();

          playbackSeq++;

          broadcastPlayback(ws, "PLAY", {
            currentTime: currentVideoTime,
            isPlaying: true,
            by,
          });

          break;
        }

        case "PAUSE": {
          const by = senderName(ws, data.name);

          if (!by) {
            sendError(ws, "Send JOIN first (and use your own name)");
            return;
          }

          const t = asFiniteNumber(data.currentTime);
          const nextTime = t !== null ? Math.max(0, t) : getLiveVideoTime();

          if (!isPlaying && nearlyEqual(nextTime, currentVideoTime)) {
            return;
          }

          currentVideoTime = nextTime;
          isPlaying = false;
          lastRealTimeAnchor = Date.now();

          playbackSeq++;

          broadcastPlayback(ws, "PAUSE", {
            currentTime: currentVideoTime,
            isPlaying: false,
            by,
          });

          break;
        }

        case "SEEK": {
          const by = senderName(ws, data.name);

          if (!by) {
            sendError(ws, "Send JOIN first (and use your own name)");
            return;
          }

          const t = asFiniteNumber(data.currentTime);

          if (t === null) {
            sendError(ws, "SEEK requires data.currentTime (number)");
            return;
          }

          const nextTime = Math.max(0, t);

          // Ignore an actual no-op seek while paused.
          if (!isPlaying && nearlyEqual(nextTime, currentVideoTime)) {
            return;
          }

          currentVideoTime = nextTime;
          lastRealTimeAnchor = Date.now();

          playbackSeq++;

          broadcastPlayback(ws, "SEEK", {
            currentTime: currentVideoTime,
            isPlaying,
            by,
          });

          break;
        }

        case "TYPING": {
          const name = senderName(ws, data.name);
          if (!name) {
            sendError(ws, "Send JOIN first (and use your own name)");
            return;
          }
          broadcast(ws, {
            event: "TYPING",
            data: { name, isTyping: data.isTyping === true },
          });
          break;
        }

        default:
          sendError(ws, `Unknown event: ${msg.event}`);
      }
    },

    close(ws: WS) {
      cleanup(ws);
    },
  },
});

console.log(`Serving "${videoPath}" on :${PORT}`);
console.log(`Subtitles: "${subtitlePath}"`);
