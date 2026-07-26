#!/usr/bin/env bun

import { Server, ServerWebSocket } from "bun";
import { randomBytes, timingSafeEqual } from "crypto";

// Store clients by channel
const channels = new Map<string, Set<ServerWebSocket<any>>>();

// Shared secret required to join a channel. Provide it out-of-band via
// FIGMA_SOCKET_TOKEN to both the MCP server and the Figma plugin; when unset a
// fresh token is generated per relay process and printed at startup.
const AUTH_TOKEN = process.env.FIGMA_SOCKET_TOKEN || randomBytes(32).toString("hex");
const TOKEN_FROM_ENV = !!process.env.FIGMA_SOCKET_TOKEN;

// A channel is a private link between exactly two peers (MCP server + plugin).
const MAX_CHANNEL_CLIENTS = Number(process.env.MAX_CHANNEL_CLIENTS || 2);

// Loopback only unless explicitly overridden.
const HOST = process.env.HOST || "127.0.0.1";

// Browser origins allowed to upgrade. Figma plugin iframes send "null"; native
// WebSocket clients (the MCP server) send no Origin header at all.
const ALLOWED_ORIGINS = new Set([
  "null",
  "https://www.figma.com",
  "https://figma.com",
  ...(process.env.ALLOWED_ORIGINS || "")
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean),
]);

function isTokenValid(token: unknown): boolean {
  if (typeof token !== "string") return false;
  const expected = Buffer.from(AUTH_TOKEN);
  const actual = Buffer.from(token);
  if (expected.length !== actual.length) return false;
  return timingSafeEqual(expected, actual);
}

function isOriginAllowed(origin: string | null): boolean {
  if (!origin) return true; // non-browser client
  return ALLOWED_ORIGINS.has(origin);
}

function removeFromChannels(ws: ServerWebSocket<any>) {
  channels.forEach((clients, channelName) => {
    if (!clients.delete(ws)) return;

    clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({
          type: "system",
          message: "A user has left the channel",
          channel: channelName,
        }));
      }
    });

    if (clients.size === 0) channels.delete(channelName);
  });
}

function handleConnection(ws: ServerWebSocket<any>) {
  // Don't add to clients immediately - wait for an authenticated channel join
  console.log("New client connected");

  ws.send(JSON.stringify({
    type: "system",
    message: "Please join a channel with a valid token to start chatting",
  }));
}

const server = Bun.serve({
  port: Number(process.env.PORT || 3055),
  hostname: HOST,
  fetch(req: Request, server: Server) {
    const origin = req.headers.get("origin");

    if (!isOriginAllowed(origin)) {
      console.warn("Rejected request from disallowed origin");
      return new Response("Forbidden origin", { status: 403 });
    }

    const corsHeaders = origin
      ? { "Access-Control-Allow-Origin": origin, Vary: "Origin" }
      : {};

    // Handle CORS preflight
    if (req.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          ...corsHeaders,
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    // Handle WebSocket upgrade
    const success = origin
      ? server.upgrade(req, { headers: new Headers(corsHeaders) })
      : server.upgrade(req);

    if (success) {
      return; // Upgraded to WebSocket
    }

    // Return response for non-WebSocket requests
    return new Response("WebSocket server running", { headers: corsHeaders });
  },
  websocket: {
    open: handleConnection,
    message(ws: ServerWebSocket<any>, message: string | Buffer) {
      try {
        const data = JSON.parse(message as string);
        console.log(`\n=== Received message from client ===`);
        console.log(`Type: ${data.type}`);
        if (data.message?.command) {
          console.log(`Command: ${data.message.command}, ID: ${data.id}`);
        } else if (data.message?.result) {
          console.log(`Response: ID: ${data.id}, Has Result: ${!!data.message.result}`);
        }

        if (data.type === "join") {
          const channelName = data.channel;
          if (!channelName || typeof channelName !== "string") {
            ws.send(JSON.stringify({
              type: "error",
              message: "Channel name is required",
            }));
            return;
          }

          if (!isTokenValid(data.token)) {
            console.warn("Rejected join with invalid token");
            ws.send(JSON.stringify({
              type: "error",
              message: {
                id: data.id,
                error: "Authentication failed: a valid token is required to join a channel",
              },
            }));
            ws.close(4401, "Unauthorized");
            return;
          }

          // Create channel if it doesn't exist
          if (!channels.has(channelName)) {
            channels.set(channelName, new Set());
          }

          // Add client to channel
          const channelClients = channels.get(channelName)!;
          if (!channelClients.has(ws) && channelClients.size >= MAX_CHANNEL_CLIENTS) {
            console.warn("Rejected join: channel is full");
            ws.send(JSON.stringify({
              type: "error",
              message: {
                id: data.id,
                error: `Channel is full (max ${MAX_CHANNEL_CLIENTS} participants)`,
              },
            }));
            ws.close(4403, "Channel full");
            return;
          }
          channelClients.add(ws);

          console.log(`\n✓ Client joined channel (${channelClients.size} total clients)`);

          // Notify client they joined successfully
          ws.send(JSON.stringify({
            type: "system",
            message: `Joined channel: ${channelName}`,
            channel: channelName,
          }));

          ws.send(JSON.stringify({
            type: "system",
            message: {
              id: data.id,
              result: "Connected to channel: " + channelName,
            },
            channel: channelName,
          }));

          // Notify other clients in channel
          channelClients.forEach((client) => {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
              client.send(JSON.stringify({
                type: "system",
                message: "A new user has joined the channel",
                channel: channelName,
              }));
            }
          });
          return;
        }

        // Handle regular messages
        if (data.type === "message") {
          const channelName = data.channel;
          if (!channelName || typeof channelName !== "string") {
            ws.send(JSON.stringify({
              type: "error",
              message: "Channel name is required",
            }));
            return;
          }

          const channelClients = channels.get(channelName);
          if (!channelClients || !channelClients.has(ws)) {
            ws.send(JSON.stringify({
              type: "error",
              message: "You must join the channel first",
            }));
            return;
          }

          // Broadcast to all OTHER clients in the channel (not the sender)
          // This prevents echo and ensures proper request-response flow
          let broadcastCount = 0;
          channelClients.forEach((client) => {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
              broadcastCount++;
              client.send(JSON.stringify({
                type: "broadcast",
                message: data.message,
                sender: "peer",
                channel: channelName,
              }));
            }
          });

          if (broadcastCount === 0) {
            console.log(`⚠️  No other clients in the channel to receive message!`);
          } else {
            console.log(`✓ Broadcast to ${broadcastCount} peer(s)`);
          }
        }

        // Forward progress_update messages to the MCP server so it can reset
        if (data.type === "progress_update") {
          const channelName = data.channel;
          if (!channelName) return;

          const channelClients = channels.get(channelName);
          if (!channelClients || !channelClients.has(ws)) return;

          channelClients.forEach((client) => {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
              client.send(JSON.stringify(data));
            }
          });
        }
      } catch (err) {
        console.error("Error handling message:", err);
      }
    },
    close(ws: ServerWebSocket<any>) {
      console.log("Client disconnected");
      removeFromChannels(ws);
    },
  },
});

console.log(`WebSocket server running on ${HOST}:${server.port}`);
if (!TOKEN_FROM_ENV) {
  console.log(
    `\nAuth token (share with the MCP server and Figma plugin, e.g. FIGMA_SOCKET_TOKEN):\n  ${AUTH_TOKEN}\n`
  );
} else {
  console.log("Auth token loaded from FIGMA_SOCKET_TOKEN");
}
