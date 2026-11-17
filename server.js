const { WebSocketServer } = require("ws");
const http = require("http");
const fs = require("fs");

const PORT = process.env.PORT || 8080;
const HISTORY_FILE = __dirname + "/history.json";

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Messenger server OK");
});

const wss = new WebSocketServer({ server });

// name -> ws
const users = new Map();

// { "pairKey": [ { from, text, ts }, ... ] }
let history = {};
try { history = JSON.parse(fs.readFileSync(HISTORY_FILE, "utf8")); } catch {}

function saveHistory() {
  try { fs.writeFileSync(HISTORY_FILE, JSON.stringify(history)); } catch (e) { console.error(e); }
}

function pairKey(a, b) { return [a, b].sort().join("|"); }

function sendTo(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function userHistory(name) {
  const out = {};
  for (const key of Object.keys(history)) {
    const [a, b] = key.split("|");
    if (a === name || b === name) out[key] = history[key];
  }
  return out;
}

wss.on("connection", (ws) => {
  let name = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "login": {
        const base = String(msg.name || "").trim().slice(0, 30);
        if (!base) {
          sendTo(ws, { type: "login_failed", reason: "Пустое имя" });
          ws.close();
          return;
        }
        // Вход под любым именем: если занято — добавляем 2, 3, 4...
        name = base;
        let n = 2;
        while (users.has(name)) name = base + n++;
        users.set(name, ws);
        sendTo(ws, { type: "login_ok", name });
        sendTo(ws, { type: "history", history: userHistory(name) });
        const all = [...users.keys()];
        for (const [n, c] of users) sendTo(c, { type: "users", users: all });
        console.log("login:", name);
        break;
      }
      case "get_history": {
        const key = pairKey(name, msg.with);
        sendTo(ws, { type: "history", history: { [key]: history[key] || [] } });
        break;
      }
      case "message": {
        const to = msg.to;
        const text = String(msg.text || "").slice(0, 2000);
        const key = pairKey(name, to);
        if (!history[key]) history[key] = [];
        history[key].push({ from: name, text, ts: Date.now() });
        // Не даём истории расти бесконечно: максимум 500 сообщений на чат
        if (history[key].length > 500) history[key] = history[key].slice(-500);
        saveHistory();
        const item = { from: name, text, ts: Date.now() };
        sendTo(ws, { type: "message", to: to, from: name, text, ts: item.ts, echo: true });
        const toWs = users.get(to);
        if (toWs) sendTo(toWs, { type: "message", from: name, text, ts: item.ts });
        break;
      }
      case "call": {
        const to = users.get(msg.to);
        if (to) sendTo(to, { type: "call", from: name });
        break;
      }
      case "accept": {
        const to = users.get(msg.to);
        if (to) sendTo(to, { type: "accept", from: name });
        break;
      }
      case "decline": {
        const to = users.get(msg.to);
        if (to) sendTo(to, { type: "decline", from: name });
        break;
      }
      case "webrtc": {
        const to = users.get(msg.to);
        if (to) sendTo(to, { type: "webrtc", from: name, data: msg.data });
        break;
      }
      case "hangup": {
        const to = users.get(msg.to);
        if (to) sendTo(to, { type: "hangup", from: name });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (name && users.get(name) === ws) {
      users.delete(name);
      const all = [...users.keys()];
      for (const [n, c] of users) sendTo(c, { type: "users", users: all });
      console.log("left:", name);
    }
  });
});

server.listen(PORT, () => {
  console.log("Server on port " + PORT);
});
