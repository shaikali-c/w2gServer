import type { ServerWebSocket } from "bun";

const clients = new Map<string, ServerWebSocket>();
const dum = new Map<ServerWebSocket, string>();

const args = Bun.argv.slice(2);
const videoPath = args[0];

if (videoPath === undefined) {
  console.error("Usage: bun server.ts <video-file>");
  process.exit(1);
}

// Core synchronization states
let currentVideoTime: number = 0;
let isPlaying: boolean = false;
let lastRealTimeAnchor: number = Date.now();

function sendMessage(sock: ServerWebSocket, msg: string) {
  sock.send(msg);
}

function getLiveVideoTime(): number {
  if (!isPlaying) return currentVideoTime;
  const elapsedRealSeconds = (Date.now() - lastRealTimeAnchor) / 1000;
  return currentVideoTime + elapsedRealSeconds;
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "*",
};

Bun.serve({
  hostname: "0.0.0.0",
  port: 8080,
  fetch(req, server) {
    const url = new URL(req.url);
    if (url.pathname === "/ws" && server.upgrade(req)) return;
    if (url.pathname === "/movie") {
      console.log("Serving movie with CORS");
      return new Response(Bun.file(videoPath), {
        headers: corsHeaders,
      });
    }
    if (url.pathname === "/subtitles") {
      return new Response(Bun.file("subtitles.vtt"), {
        headers: corsHeaders,
      });
    }
    return new Response("Not Found", { status: 404 });
  },

  websocket: {
    open(ws: ServerWebSocket) {
      console.log("Total Users Connected: ", clients.size);
    },

    message(ws, message) {
      console.log("Received:", message.toString());

      try {
        const msg = JSON.parse(message.toString());
        const data = { msg };

        switch (msg.event) {
          case "MESSAGE": {
            const usrMessage = JSON.stringify({
              event: "MESSAGE",
              data: {
                from: "them",
                text:msg.data.text
              },
            });
            for (const [name, client] of clients) {
              if (name !== data.msg.data.name) sendMessage(client, usrMessage);
            }
            break;
          }

          case "JOIN": {
            const joinedName = data.msg.data.name;
            clients.set(joinedName, ws);
            dum.set(ws, joinedName);

            const joinAlert = JSON.stringify({
              event: "JOIN",
              data: msg.data.name,
            });
            for (const [name, client] of clients) {
              sendMessage(client, joinAlert);
            }

            sendMessage(
              ws,
              JSON.stringify({
                event: "SYNC",
                data: {
                  currentTime: getLiveVideoTime(),
                  isPlaying,
                },
              }),
            );
            break;
          }

          case "PLAY": {
            currentVideoTime = getLiveVideoTime();
            isPlaying = true;
            lastRealTimeAnchor = Date.now();

            const response = JSON.stringify({
              event: "PLAY", // Changed to sync
              data: {},
            });
            for (const [name, client] of clients) {
              if (name !== msg.data.name) sendMessage(client, response);
            }
            break;
          }

          case "PAUSE": {
            currentVideoTime = getLiveVideoTime();
            isPlaying = false;
            lastRealTimeAnchor = Date.now();

            const response = JSON.stringify({
              event: "PAUSE", // Changed to sync
              data,
            });
            for (const [name, client] of clients) {
              if (name !== msg.data.name) sendMessage(client, response);
            }
            break;
          }
          case "TYPING": {
            const response = JSON.stringify({
              event: "TYPING", // Changed to sync
              data: msg.data.isTyping,
            });
            for (const [name, client] of clients) {
              if (name !== msg.data.name) sendMessage(client, response);
            }
            break;
          }

          case "SEEK": {
            currentVideoTime = msg.data.currentTime;
            lastRealTimeAnchor = Date.now();
            const response = JSON.stringify({
              event: "SEEK",
              data: {
                currentTime: currentVideoTime,
                isPlaying: isPlaying,
              },
            });
            for (const [name, client] of clients) {
              if (name !== msg.data.name) sendMessage(client, response);
            }
            break;
          }
        }
      } catch (err) {
        console.error("Payload parse error:", err);
      }
    },

    close(ws) {
      const name = dum.get(ws);
      if (!name) return;

      clients.delete(name);
      dum.delete(ws);

      const leaveAlert = JSON.stringify({
        event: "LEFT", // Changed to lowercase
        data: name,
      });
      for (const client of clients.values()) {
        sendMessage(client, leaveAlert);
      }
    },
  },
});
