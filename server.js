const { WebSocketServer } = require("ws");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const DATA_DIR = process.env.DATA_DIR || __dirname;
const HISTORY_FILE = DATA_DIR + "/history.json";
const ACCOUNTS_FILE = DATA_DIR + "/accounts.json";
const GROUPS_FILE = DATA_DIR + "/groups.json";
const AVATARS_FILE = DATA_DIR + "/avatars.json";

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Messenger server OK");
});

const wss = new WebSocketServer({ server });

// name -> ws (только онлайн)
const users = new Map();
// name -> { id, name, members: [] }
let groups = {};
// name -> { salt, hash }
let accounts = {};
// name -> "data:image/jpeg;base64,..." (миниатюра)
let avatars = {};

function loadJson(file, def) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return def; }
}
let history = loadJson(HISTORY_FILE, {});

function saveJson(file, obj) {
  try { fs.writeFileSync(file, JSON.stringify(obj)); } catch (e) { console.error(e); }
}
const saveHistory = () => saveJson(HISTORY_FILE, history);
const saveAccounts = () => saveJson(ACCOUNTS_FILE, accounts);
const saveGroups = () => saveJson(GROUPS_FILE, groups);
const saveAvatars = () => saveJson(AVATARS_FILE, avatars);
const MAX_AVATAR = 60 * 1024; // ~60 КБ на аватар

function hashPass(pass, salt) {
  return crypto.scryptSync(pass, salt, 32).toString("hex");
}

function pairKey(a, b) { return [a, b].sort().join("|"); }

function sendTo(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function sendToName(name, obj) {
  sendTo(users.get(name), obj);
}

function userGroupList(name) {
  return Object.values(groups).filter(g => g.members.includes(name));
}

function userHistory(name) {
  const out = {};
  for (const key of Object.keys(history)) {
    if (key.startsWith("g:")) {
      const g = groups[key.slice(2)];
      if (g && g.members.includes(name)) out[key] = history[key];
    } else {
      const [a, b] = key.split("|");
      if (a === name || b === name) out[key] = history[key];
    }
  }
  return out;
}

function broadcastUsers() {
  const all = Object.keys(accounts);
  const online = [...users.keys()];
  for (const [, c] of users) sendTo(c, { type: "users", users: all, online });
}

function validName(n) { return /^[a-zA-Z0-9_\u0430-\u044f\u0410-\u042f]{2,20}$/.test(n); }

wss.on("connection", (ws) => {
  let name = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "login": {
        const u = String(msg.name || "").trim();
        const pass = String(msg.pass || "");
        const register = !!msg.register;
        if (!validName(u)) {
          sendTo(ws, { type: "login_failed", reason: "Имя: 2–20 символов, буквы, цифры, _" });
          ws.close();
          return;
        }
        if (pass.length < 4) {
          sendTo(ws, { type: "login_failed", reason: "Пароль минимум 4 символа" });
          ws.close();
          return;
        }
        if (register) {
          if (accounts[u]) {
            sendTo(ws, { type: "login_failed", reason: "Это имя уже занято" });
            return;
          }
          const salt = crypto.randomBytes(8).toString("hex");
          accounts[u] = { salt, hash: hashPass(pass, salt) };
          saveAccounts();
        } else {
          const acc = accounts[u];
          if (!acc) {
            sendTo(ws, { type: "login_failed", reason: "Пользователь не найден. Зарегистрируйтесь" });
            ws.close();
            return;
          }
          if (hashPass(pass, acc.salt) !== acc.hash) {
            sendTo(ws, { type: "login_failed", reason: "Неверный пароль" });
            ws.close();
            return;
          }
        }
        // Выкинуть старое соединение этого же имени
        const old = users.get(u);
        if (old && old !== ws) { try { old.close(4000, "relogin"); } catch {} }
        name = u;
        users.set(name, ws);
        sendTo(ws, { type: "login_ok", name });
        sendTo(ws, { type: "groups", groups: userGroupList(name) });
        sendTo(ws, { type: "history", history: userHistory(name) });
        sendTo(ws, { type: "avatars", avatars });
        broadcastUsers();
        console.log("login:", name, register ? "(registered)" : "");
        break;
      }
      case "get_history": {
        const with_ = msg.with;
        const key = String(with_).startsWith("g:") ? with_ : pairKey(name, with_);
        sendTo(ws, { type: "history", history: { [key]: history[key] || [] } });
        break;
      }
      case "find_user": {
        const u = String(msg.name || "").trim();
        sendTo(ws, { type: "user_found", name: u, exists: !!accounts[u] });
        break;
      }
      case "create_group": {
        const gname = String(msg.name || "").trim().slice(0, 30);
        const members = [...new Set([name, ...(Array.isArray(msg.members) ? msg.members : [])])]
          .filter(m => accounts[m]).slice(0, 50);
        if (!gname || members.length < 1) return;
        const id = "g" + crypto.randomBytes(4).toString("hex");
        groups[id] = { id, name: gname, members };
        saveGroups();
        const payload = { type: "group", group: groups[id] };
        for (const m of members) sendToName(m, payload);
        console.log("group created:", gname, members.join(","));
        break;
      }
      case "message": {
        const to = String(msg.to || "");
        const text = String(msg.text || "").slice(0, 2000);
        const MAX_FILE = 1200 * 1024;
        let file = null;
        if (typeof msg.file === "string" && msg.file.startsWith("data:") && msg.file.length <= MAX_FILE) file = msg.file;
        const fname = String(msg.fname || "").slice(0, 100);
        const reply = String(msg.reply || "").slice(0, 200);
        if (!text && !file) return;
        const ts = Date.now();
        const item = { from: name, text, ts };
        if (reply) item.reply = reply;
        if (file) { item.file = file; if (fname) item.fname = fname; }
        if (to.startsWith("g:")) {
          const g = groups[to.slice(2)];
          if (!g || !g.members.includes(name)) return;
          if (!history[to]) history[to] = [];
          history[to].push(item);
          if (history[to].length > 300) history[to] = history[to].slice(-300);
          saveHistory();
          sendTo(ws, { type: "message", from: name, to, text, ts, echo: true, group: true, file, fname, reply });
          for (const m of g.members) if (m !== name) sendToName(m, { type: "message", from: name, to, text, ts, group: true, file, fname, reply });
        } else {
          if (!accounts[to]) return;
          const key = pairKey(name, to);
          if (!history[key]) history[key] = [];
          history[key].push(item);
          if (history[key].length > 300) history[key] = history[key].slice(-300);
          saveHistory();
          sendTo(ws, { type: "message", from: name, to, text, ts, echo: true, file, fname, reply });
          sendToName(to, { type: "message", from: name, text, ts, file, fname, reply });
        }
        break;
      }
      case "delete_chat": {
        const to = String(msg.to || "");
        let key, peers;
        if (to.startsWith("g:")) {
          const g = groups[to.slice(2)];
          if (!g || !g.members.includes(name)) return;
          key = to; peers = g.members.filter(m => m !== name);
        } else {
          if (!accounts[to]) return;
          key = pairKey(name, to); peers = [to];
        }
        delete history[key];
        saveHistory();
        sendTo(ws, { type: "chat_deleted", key });
        for (const p of peers) sendToName(p, { type: "chat_deleted", key });
        console.log("chat deleted:", key);
        break;
      }
      case "delete_group": {
        const gid = String(msg.id || "");
        const g = groups[gid];
        if (!g || !g.members.includes(name)) return;
        const key = "g:" + gid;
        delete groups[gid];
        delete history[key];
        saveGroups();
        saveHistory();
        const note = { type: "group_deleted", key };
        for (const m of g.members) sendToName(m, note);
        console.log("group deleted:", g.name);
        break;
      }
      case "react": {
        const to = String(msg.to || "");
        const ts = Number(msg.ts);
        const emoji = String(msg.emoji || "").slice(0, 8);
        const key = to.startsWith("g:") ? to : pairKey(name, to);
        const arr = history[key];
        if (!arr) return;
        const it = arr.find(m => m.ts === ts);
        if (!it) return;
        it.reactions = it.reactions || {};
        const list = it.reactions[emoji] || [];
        const i = list.indexOf(name);
        if (i >= 0) list.splice(i, 1); else list.push(name);
        if (list.length) it.reactions[emoji] = list; else delete it.reactions[emoji];
        if (!Object.keys(it.reactions).length) delete it.reactions;
        saveHistory();
        const payload = { type: "reactions", key, ts, reactions: it.reactions || {} };
        if (key.startsWith("g:")) {
          const g = groups[key.slice(2)];
          if (g) for (const m of g.members) sendToName(m, payload);
        } else {
          const [a, b] = key.split("|");
          sendToName(a, payload);
          sendToName(b, payload);
        }
        break;
      }
      case "set_avatar": {
        const data = String(msg.data || "");
        if (!data.startsWith("data:image/") || data.length > MAX_AVATAR) {
          sendTo(ws, { type: "avatar_failed", reason: "Слишком большое изображение" });
          return;
        }
        avatars[name] = data;
        saveAvatars();
        for (const [, c] of users) sendTo(c, { type: "avatar", name, data });
        console.log("avatar set:", name);
        break;
      }
      case "call": { const to = users.get(msg.to); if (to) sendTo(to, { type: "call", from: name }); break; }
      case "accept": { const to = users.get(msg.to); if (to) sendTo(to, { type: "accept", from: name }); break; }
      case "decline": { const to = users.get(msg.to); if (to) sendTo(to, { type: "decline", from: name }); break; }
      case "webrtc": { const to = users.get(msg.to); if (to) sendTo(to, { type: "webrtc", from: name, data: msg.data }); break; }
      case "hangup": { const to = users.get(msg.to); if (to) sendTo(to, { type: "hangup", from: name }); break; }
    }
  });

  ws.on("close", () => {
    if (name && users.get(name) === ws) {
      users.delete(name);
      broadcastUsers();
      console.log("left:", name);
    }
  });
});

server.listen(PORT, () => {
  accounts = loadJson(ACCOUNTS_FILE, {});
  groups = loadJson(GROUPS_FILE, {});
  avatars = loadJson(AVATARS_FILE, {});
  console.log("Server on port " + PORT);
});
