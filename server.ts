import type { ServerWebSocket } from "bun";

const clients = new Map<string, ServerWebSocket>();
const dum = new Map<ServerWebSocket, string>();
Bun.serve({
  hostname: "0.0.0.0",
  port: 8080,
  fetch(req, server) {
    const url = new URL(req.url);

    // WebSocket
    if (url.pathname === "/ws") {
      if (server.upgrade(req)) return;
      return new Response("Upgrade failed", { status: 500 });
    }

    // Video
    const movie = Bun.file("./output.mp4");

    if (url.pathname === "/movie") {
      return new Response(movie);
    }

    return new Response("Not Found", { status: 404 });
  },

  websocket: {
    open(ws) {
      console.log("Clients: ", clients.size);
    },
    message(ws, message) {
      console.log(message);
      const msg = JSON.parse(message.toString());
      if (msg.action === "JOIN") {
        clients.set(msg.name, ws);
        dum.set(ws, msg.name);
        for (const [address, client] of clients) {
          if (address === ws.remoteAddress) continue;
          client.send(
            JSON.stringify({
              type: "alert",
              action: "JOIN",
              message: `${msg.name} Joined`,
              pic: msg.pic,
            }),
          );
        }
      } else {
        for (const [address, client] of clients) {
          if (client === ws) continue;
          client.send(message);
        }
      }
    },
    close(ws, code, reason) {
      const name = dum.get(ws);

      if (!name) return;

      clients.delete(name);
      dum.delete(ws);

      for (const client of clients.values()) {
        client.send(
          JSON.stringify({
            type: "alert",
            action: "LEFT",
            message: `${name} Left`,
          }),
        );
      }
    },
  },
});
