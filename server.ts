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
let currentVideoTime: number = 0; // The current video playback position in seconds
let isPlaying: boolean = false; // Is the video currently playing?
let lastRealTimeAnchor: number = Date.now(); // System clock timestamp when the video state last updated

function sendMessage(sock: ServerWebSocket, msg: string) {
  sock.send(msg);
}

// Calculate the EXACT video time right now based on the system clock
function getLiveVideoTime(): number {
  if (!isPlaying) return currentVideoTime;

  // Calculate exactly how many real-world seconds passed since the last play event
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
    // if (url.pathname === "/movie") return new Response(Bun.file(videoPath));
    if (url.pathname === "/movie") {
      console.log("Serving movie with CORS");

      return new Response(Bun.file(videoPath), {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }
    if (url.pathname === "/subtitles") return new Response(Bun.file("subtitles.vtt"), {
      headers:corsHeaders
    });
    return new Response("Not Found", { status: 404 });
  },

  websocket: {
    open(ws) {
      console.log("Total Users Connected: ", clients.size);
    },
    message(ws, message) {
      console.log(message);
      try {
        const msg = JSON.parse(message.toString());

        switch (msg.action) {
          case "MESSAGE": {
            const usrMessage = JSON.stringify({
              type: "MESSAGE",
              name: msg.name,
              message: msg.message,
              pic: msg.pic,
            });
            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, usrMessage);
            }
            break;
          }
          case "JOIN": {
            clients.set(msg.name, ws);
            dum.set(ws, msg.name);

            // Broadcast join alert to everyone else
            const joinAlert = JSON.stringify({
              type: "ALERT",
              message: `${msg.name} Joined`,
              pic: msg.pic,
            });
            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, joinAlert);
            }

            console.log(getLiveVideoTime());

            // Catch the new user up with the live room timeline and play state
            sendMessage(
              ws,
              JSON.stringify({
                type: "SYNC",
                videoTime: getLiveVideoTime(),
                isPlaying: isPlaying,
              }),
            );
            break;
          }

          case "PLAY": {
            currentVideoTime = getLiveVideoTime();
            isPlaying = true;
            lastRealTimeAnchor = Date.now();

            const response = JSON.stringify({
              action: "PLAY",
              videoTime: currentVideoTime,
            });
            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, response); // Skip sender
            }
            break;
          }

          case "PAUSE": {
            // Lock the timestamp down at the paused time position
            currentVideoTime = getLiveVideoTime();
            isPlaying = false;
            lastRealTimeAnchor = Date.now();

            const response = JSON.stringify({
              action: "PAUSE",
              videoTime: currentVideoTime,
            });
            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, response); // Skip sender
            }
            break;
          }

          case "SEEK": {
            currentVideoTime = msg.timestamp;
            const response = JSON.stringify({
              action: "SEEK",
              videoTime: currentVideoTime,
            });
            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, response);
            }
            break;
          }

          case "SYNC": {
            // Handles timeline scrubbing/seeking
            currentVideoTime = msg.timestamp;
            lastRealTimeAnchor = Date.now();

            const response = JSON.stringify({
              type: "SYNC",
              videoTime: currentVideoTime,
              isPlaying: isPlaying,
            });

            for (const [name, client] of clients) {
              if (name !== msg.name) sendMessage(client, response); // Skip sender
            }
            break;
          }
          case "SERVERSYNC": {
            let clientTimestamp: number = msg.timestamp;
            const timeDifference = Math.abs(
              getLiveVideoTime() - clientTimestamp,
            );
            if (timeDifference >= 10) {
              const response = JSON.stringify({
                type: "SERVERSYNC",
                videoTime: currentVideoTime,
                isPlaying: isPlaying,
              });
              sendMessage(ws, response);
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
        type: "ALERT",
        action: "LEFT",
        message: `${name} Left`,
      });
      for (const client of clients.values()) {
        sendMessage(client, leaveAlert);
      }
    },
  },
});
