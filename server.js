const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
app.use(express.static(path.join(__dirname, "public")));

const rooms = new Map();
const makeCode = () => Math.random().toString(36).slice(2, 6).toUpperCase();
const send = (ws, data) => { if (ws.readyState === 1) ws.send(JSON.stringify(data)); };

function roomState(room) {
  return {
    code: room.code,
    phase: room.phase,
    players: [...room.players.values()].map(p => ({
      id: p.id, name: p.name, connected: !!p.ws
    })),
    writerId: room.writerId,
    writerName: room.players.get(room.writerId)?.name || null,
    round: room.round,
    options: room.options,
    votes: room.votes.size,
    totalVoters: room.players.size - (room.writerId ? 1 : 0),
    results: room.phase === "results" ? room.resultCounts : null
  };
}
function broadcast(room) {
  const state = roomState(room);
  for (const p of room.players.values()) if (p.ws) send(p.ws, { type: "state", state, myId: p.id });
}
function chooseWriter(room) {
  const ids = [...room.players.keys()];
  if (ids.length < 4) return null;
  const candidates = ids.filter(id => id !== room.lastWriter);
  const pool = candidates.length ? candidates : ids;
  return pool[Math.floor(Math.random() * pool.length)];
}
function startRound(room) {
  room.round++;
  room.writerId = chooseWriter(room);
  room.phase = "writing";
  room.options = null;
  room.votes = new Map();
  room.resultCounts = null;
  broadcast(room);
}
function finishVoting(room) {
  const counts = [0, 0];
  for (const v of room.votes.values()) counts[v]++;
  room.resultCounts = counts;
  room.phase = "results";
  broadcast(room);
}

wss.on("connection", ws => {
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === "join") {
      const name = String(msg.name || "").trim().slice(0, 18);
      const code = String(msg.code || "").trim().toUpperCase();
      if (!name) return send(ws, { type: "error", message: "Írj be egy nevet." });
      if (msg.create) {
        let newCode = makeCode();
        while (rooms.has(newCode)) newCode = makeCode();
        rooms.set(newCode, {
          code: newCode, players: new Map(), phase: "lobby",
          writerId: null, lastWriter: null, round: 0,
          options: null, votes: new Map(), resultCounts: null
        });
        msg.code = newCode;
      }
      const room = rooms.get(String(msg.code || "").toUpperCase());
      if (!room) return send(ws, { type: "error", message: "Nem találom ezt a lobby-kódot." });
      if (room.phase !== "lobby") return send(ws, { type: "error", message: "A játék már elkezdődött." });
      if (room.players.size >= 5) return send(ws, { type: "error", message: "A lobby tele van (max. 5 fő)." });
      const id = crypto.randomUUID();
      room.players.set(id, { id, name, ws });
      ws.roomCode = room.code;
      ws.playerId = id;
      send(ws, { type: "joined", code: room.code, id });
      broadcast(room);
      return;
    }

    const room = rooms.get(ws.roomCode);
    const player = room?.players.get(ws.playerId);
    if (!room || !player) return;

    if (msg.type === "start") {
      if (room.players.size < 4) return send(ws, { type: "error", message: "Legalább 4 játékos kell." });
      if ([...room.players.values()][0].id !== player.id) return send(ws, { type: "error", message: "A lobby létrehozója indíthatja a játékot." });
      startRound(room);
    }
    if (msg.type === "submitOptions") {
      if (room.phase !== "writing" || room.writerId !== player.id) return;
      const a = String(msg.a || "").trim().slice(0, 90);
      const b = String(msg.b || "").trim().slice(0, 90);
      if (!a || !b || a.toLowerCase() === b.toLowerCase()) return send(ws, { type: "error", message: "Adj meg két különböző, nem üres opciót." });
      room.options = [a, b];
      room.phase = "voting";
      room.votes = new Map();
      broadcast(room);
    }
    if (msg.type === "vote") {
      if (room.phase !== "voting" || player.id === room.writerId) return;
      if (msg.choice !== 0 && msg.choice !== 1) return;
      room.votes.set(player.id, msg.choice);
      const eligible = room.players.size - 1;
      if (room.votes.size >= eligible) finishVoting(room);
      else broadcast(room);
    }
    if (msg.type === "next") {
      if (room.phase !== "results" || [...room.players.values()][0].id !== player.id) return;
      room.lastWriter = room.writerId;
      startRound(room);
    }
  });

  ws.on("close", () => {
    const room = rooms.get(ws.roomCode);
    const player = room?.players.get(ws.playerId);
    if (player) {
      player.ws = null;
      broadcast(room);
      if ([...room.players.values()].every(p => !p.ws)) rooms.delete(room.code);
    }
  });
});

const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`Party game running on ${port}`));
