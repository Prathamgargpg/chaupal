import express from "express";
import http from "http";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Server } from "socket.io";
import Database from "better-sqlite3";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const MAX_ROOM_SIZE = Number(process.env.MAX_ROOM_SIZE || 6);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "chaupal.db");

let JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  JWT_SECRET = "dev-only-secret-" + Math.random().toString(36).slice(2);
  console.warn("JWT_SECRET not set. Using a temporary one; logins reset on restart.");
}

/* ---------- database ---------- */
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
)`);
const findByEmail = db.prepare("SELECT * FROM users WHERE email = ?");
const findById = db.prepare("SELECT id, email, name FROM users WHERE id = ?");
const insertUser = db.prepare("INSERT INTO users (email, name, password_hash) VALUES (?, ?, ?)");

/* ---------- app ---------- */
const app = express();
app.set("trust proxy", 1);
app.use(express.json({ limit: "10kb" }));
app.use(express.static(path.join(__dirname, "public")));

const sign = u => jwt.sign({ id: u.id, name: u.name }, JWT_SECRET, { expiresIn: "7d" });
const publicUser = u => ({ id: u.id, name: u.name, email: u.email });

// tiny in-memory rate limit for auth endpoints
const hits = new Map();
function limit(req, res, next) {
  const key = req.ip, now = Date.now();
  const arr = (hits.get(key) || []).filter(t => now - t < 10 * 60 * 1000);
  if (arr.length >= 30) return res.status(429).json({ error: "Too many attempts. Try again in a few minutes." });
  arr.push(now); hits.set(key, arr); next();
}

// Express 4 does not catch errors thrown in async routes; this does, and answers in JSON
const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

app.post("/api/signup", limit, wrap(async (req, res) => {
  const { name, email, password } = req.body || {};
  const e = String(email || "").trim().toLowerCase();
  const n = String(name || "").trim().slice(0, 40) || e.split("@")[0];
  if (!EMAIL_RE.test(e)) return res.status(400).json({ error: "Enter a valid email address." });
  if (String(password || "").length < 8) return res.status(400).json({ error: "Password must be at least 8 characters." });
  if (findByEmail.get(e)) return res.status(409).json({ error: "An account with this email already exists. Sign in instead." });
  const hash = await bcrypt.hash(password, 10);
  let info;
  try { info = insertUser.run(e, n, hash); }
  catch (err) {
    if (String(err.code).startsWith("SQLITE_CONSTRAINT")) return res.status(409).json({ error: "An account with this email already exists. Sign in instead." });
    throw err;
  }
  const user = { id: info.lastInsertRowid, name: n, email: e };
  res.json({ token: sign(user), user: publicUser(user) });
}));

app.post("/api/login", limit, wrap(async (req, res) => {
  const { email, password } = req.body || {};
  const row = findByEmail.get(String(email || "").trim().toLowerCase());
  const ok = row && (await bcrypt.compare(String(password || ""), row.password_hash));
  if (!ok) return res.status(401).json({ error: "Email or password is incorrect." });
  res.json({ token: sign(row), user: publicUser(row) });
}));

function auth(req, res, next) {
  const h = req.headers.authorization || "";
  try {
    const p = jwt.verify(h.replace(/^Bearer /, ""), JWT_SECRET);
    const u = findById.get(p.id);
    if (!u) throw new Error("no user");
    req.user = u; next();
  } catch { res.status(401).json({ error: "Please sign in again." }); }
}

app.get("/api/me", auth, (req, res) => res.json({ user: req.user }));

app.get("/api/ice", auth, (req, res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:global.stun.twilio.com:3478" }
  ];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(","),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }
  res.json({ iceServers });
});

app.get("/healthz", (_req, res) => {
  try { db.prepare("SELECT 1").get(); res.json({ ok: true, db: "ok" }); }
  catch (e) { res.status(500).json({ ok: false, db: String(e.message) }); }
});

// any unhandled route error (including bad JSON bodies) -> JSON, never an HTML page
app.use((err, _req, res, _next) => {
  console.error("Request error:", err);
  if (err.type === "entity.parse.failed") return res.status(400).json({ error: "Bad request." });
  res.status(500).json({ error: "Server error. Please try again in a moment." });
});

process.on("unhandledRejection", e => console.error("Unhandled rejection:", e));

/* ---------- socket.io signaling ---------- */
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: false } });

io.use((socket, next) => {
  try {
    const p = jwt.verify(socket.handshake.auth?.token || "", JWT_SECRET);
    const u = findById.get(p.id);
    if (!u) throw new Error("no user");
    socket.user = { id: u.id, name: u.name };
    next();
  } catch { next(new Error("unauthorized")); }
});

const ROOM_RE = /^[a-z]{3}-[a-z]{4}-[a-z]{3}$/;

io.on("connection", socket => {
  socket.on("join", async (room, ack) => {
    if (typeof ack !== "function") return;
    if (!ROOM_RE.test(room)) return ack({ error: "Invalid meeting code." });
    if (socket.room) return ack({ error: "Already in a meeting." });
    const members = await io.in(room).fetchSockets();
    if (members.length >= MAX_ROOM_SIZE) return ack({ error: `This meeting is full (${MAX_ROOM_SIZE} people max).` });
    socket.join(room);
    socket.room = room;
    socket.to(room).emit("peer-joined", { id: socket.id, name: socket.user.name });
    ack({ peers: members.map(s => ({ id: s.id, name: s.user.name })) });
  });

  // relay SDP offers/answers and ICE candidates between two peers in the same room
  socket.on("signal", ({ to, data } = {}) => {
    const target = io.sockets.sockets.get(to);
    if (!target || !socket.room || target.room !== socket.room) return;
    target.emit("signal", { from: socket.id, name: socket.user.name, data });
  });

  socket.on("chat", text => {
    if (!socket.room || typeof text !== "string") return;
    socket.to(socket.room).emit("chat", { name: socket.user.name, text: text.slice(0, 1000) });
  });

  socket.on("disconnecting", () => {
    if (socket.room) socket.to(socket.room).emit("peer-left", { id: socket.id });
  });
});

server.listen(PORT, () => console.log(`Chaupal running on http://localhost:${PORT}`));
