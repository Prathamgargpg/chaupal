import express from "express";
import http from "http";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Server } from "socket.io";
import Database from "better-sqlite3";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const MAX_ROOM_SIZE = Number(process.env.MAX_ROOM_SIZE || 8);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "chaupal.db");

let JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  JWT_SECRET = "dev-only-secret-" + Math.random().toString(36).slice(2);
  console.warn("JWT_SECRET not set. Using a temporary one; logins reset on restart.");
}

/* ---------- database ---------- */
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

/* ---------- permanent storage ----------
   The app runs on SQLite. If DATABASE_URL (a free Neon Postgres) is set, a snapshot of the SQLite
   file is restored from Postgres at startup and saved back shortly after every change and on shutdown. */
let pool = null;
if (process.env.DATABASE_URL) {
  pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? false : { rejectUnauthorized: false }, max: 2 });
  await pool.query("CREATE TABLE IF NOT EXISTS app_snapshot (id INT PRIMARY KEY, data BYTEA NOT NULL, updated_at TIMESTAMPTZ DEFAULT now())");
  const r = await pool.query("SELECT data FROM app_snapshot WHERE id = 1");
  for (const ext of ["", "-wal", "-shm"]) fs.rmSync(DB_PATH + ext, { force: true });
  if (r.rows[0]) { fs.writeFileSync(DB_PATH, r.rows[0].data); console.log("Restored database from Postgres snapshot."); }
  else console.log("No snapshot yet. Starting fresh.");
} else console.warn("DATABASE_URL not set: data lives only on this server's temporary disk.");
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
)`);
for (const [c, d] of [["title", "TEXT DEFAULT ''"], ["bio", "TEXT DEFAULT ''"], ["role", "TEXT DEFAULT 'user'"], ["disabled", "INTEGER DEFAULT 0"], ["visibility", "TEXT DEFAULT 'public'"], ["show_streak", "INTEGER DEFAULT 1"], ["show_activity", "INTEGER DEFAULT 0"]])
  if (!db.prepare("PRAGMA table_info(users)").all().some(x => x.name === c)) db.exec(`ALTER TABLE users ADD COLUMN ${c} ${d}`);
db.exec(`CREATE TABLE IF NOT EXISTS encouragements (
  id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL,
  message TEXT, created_at INTEGER NOT NULL)`);
db.exec(`
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, type TEXT NOT NULL, data TEXT, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id, created_at);
CREATE TABLE IF NOT EXISTS login_days (user_id INTEGER NOT NULL, day TEXT NOT NULL, PRIMARY KEY (user_id, day));
CREATE TABLE IF NOT EXISTS follows (follower_id INTEGER NOT NULL, followee_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (follower_id, followee_id));
CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL);`);
const findByEmail = db.prepare("SELECT * FROM users WHERE email = ?");
const findById = db.prepare("SELECT id, email, name, title, bio, role, disabled, visibility, show_streak, show_activity FROM users WHERE id = ?");
const insertUser = db.prepare("INSERT INTO users (email, name, password_hash) VALUES (?, ?, ?)");

let savedChanges = -1, saving = false;
async function saveSnapshot(force = false) {
  if (!pool || saving) return;
  const c = db.prepare("SELECT total_changes() AS c").get().c;
  if (!force && c === savedChanges) return;
  saving = true;
  try {
    await pool.query("INSERT INTO app_snapshot (id, data) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()", [db.serialize()]);
    savedChanges = c;
  } catch (e) { console.error("Snapshot failed:", e.message); }
  saving = false;
}
if (pool) {
  setInterval(saveSnapshot, 10000).unref();
  for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, async () => {
    console.log("Saving database before shutdown...");
    while (saving) await new Promise(r => setTimeout(r, 100));
    await saveSnapshot(true);
    process.exit(0);
  });
}

/* ---------- app ---------- */
const app = express();
app.set("trust proxy", 1);
app.use(express.json({ limit: "10kb" }));
// Find the front-end folder even if the upload put it somewhere unexpected
const staticDir = [path.join(__dirname, "public"), path.join(__dirname, "chaupal-app", "public")]
  .find(d => fs.existsSync(path.join(d, "index.html")));
if (staticDir) app.use(express.static(staticDir));
app.get("/", (_req, res) => {
  const rootIndex = path.join(__dirname, "index.html");
  if (fs.existsSync(rootIndex)) return res.sendFile(rootIndex);   // index.html uploaded next to server.js
  const files = fs.readdirSync(__dirname).filter(f => f !== "node_modules" && !f.startsWith(".env")).join(", ");
  res.status(500).type("text").send(
    "Chaupal server is running, but index.html was not found.\n" +
    "Create the file public/index.html in your GitHub repo.\n\n" +
    "Files next to server.js: " + files
  );
});

const sign = u => jwt.sign({ id: u.id, name: u.name }, JWT_SECRET, { expiresIn: "7d" });
const publicUser = u => ({ id: u.id, name: u.name, email: u.email, title: u.title || "", bio: u.bio || "", role: u.role || "user", visibility: u.visibility || "public", show_streak: u.show_streak ?? 1, show_activity: u.show_activity ?? 0 });

/* ---------- settings: titles, encouragement rules ---------- */
const TITLES = ["Student", "Developer", "Designer", "Founder", "Mentor", "Teacher", "Writer", "Job seeker"];
const ADMIN_EMAIL = (process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const COOLDOWN = Number(process.env.ENCOURAGE_COOLDOWN_MIN ?? 15) * 60000;   // wait between encouragements
const DAILY = Number(process.env.ENCOURAGE_DAILY_MAX ?? 3);                    // max per day
const TZ = Number(process.env.TZ_OFFSET_MIN ?? 330) * 60000;                   // day boundary (330 = India)
const dayStart = () => Math.floor((Date.now() + TZ) / 864e5) * 864e5 - TZ;
const qToday = db.prepare("SELECT COUNT(*) c FROM encouragements WHERE from_id = ? AND created_at >= ?");
const qLast = db.prepare("SELECT MAX(created_at) t FROM encouragements WHERE from_id = ?");
function quota(uid) {
  const used = qToday.get(uid, dayStart()).c, last = qLast.get(uid).t || 0;
  return { left: Math.max(0, DAILY - used), max: DAILY, nextAt: last ? last + COOLDOWN : 0 };
}

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
  const id = Number(info.lastInsertRowid);
  if ((ADMIN_EMAIL && e === ADMIN_EMAIL) || db.prepare("SELECT COUNT(*) c FROM users").get().c === 1)
    db.prepare("UPDATE users SET role = 'admin' WHERE id = ?").run(id);
  recordLogin(id);
  const user = findById.get(id);
  res.json({ token: sign(user), user: publicUser(user) });
}));

app.post("/api/login", limit, wrap(async (req, res) => {
  const { email, password } = req.body || {};
  const row = findByEmail.get(String(email || "").trim().toLowerCase());
  const ok = row && (await bcrypt.compare(String(password || ""), row.password_hash));
  if (!ok) return res.status(401).json({ error: "Email or password is incorrect." });
  if (row.disabled) return res.status(403).json({ error: "This account has been disabled. Contact the admin." });
  recordLogin(row.id);
  res.json({ token: sign(row), user: publicUser(row) });
}));

function auth(req, res, next) {
  const h = req.headers.authorization || "";
  try {
    const p = jwt.verify(h.replace(/^Bearer /, ""), JWT_SECRET);
    const u = findById.get(p.id);
    if (!u || u.disabled) throw new Error("no user");
    req.user = u; next();
  } catch { res.status(401).json({ error: "Please sign in again." }); }
}

app.get("/api/me", auth, (req, res) => { recordLogin(req.user.id); res.json({ user: publicUser(req.user), quota: quota(req.user.id), streak: streakOf(req.user.id) }); });
app.get("/api/titles", auth, (_req, res) => res.json({ titles: TITLES }));
app.put("/api/me", auth, (req, res) => {
  const { name, title, bio, visibility, show_streak, show_activity } = req.body || {};
  const n = String(name || "").trim().slice(0, 40) || req.user.name;
  db.prepare("UPDATE users SET name = ?, title = ?, bio = ? WHERE id = ?")
    .run(n, TITLES.includes(title) ? title : "", String(bio || "").trim().slice(0, 200), req.user.id);
  db.prepare("UPDATE users SET visibility = ?, show_streak = ?, show_activity = ? WHERE id = ?")
    .run(["public", "followers", "private"].includes(visibility) ? visibility : "public", show_streak ? 1 : 0, show_activity ? 1 : 0, req.user.id);
  res.json({ user: publicUser(findById.get(req.user.id)) });
});

/* ---------- user data timeline, streaks, privacy ---------- */
const dkey = t => new Date(t + TZ).toISOString().slice(0, 10);
const logEvent = (uid, type, data = {}) => db.prepare("INSERT INTO events (user_id, type, data, created_at) VALUES (?, ?, ?, ?)").run(uid, type, JSON.stringify(data), Date.now());
function recordLogin(uid) {
  const d = dkey(Date.now());
  if (db.prepare("INSERT OR IGNORE INTO login_days (user_id, day) VALUES (?, ?)").run(uid, d).changes) logEvent(uid, "daily_login", { day: d });
}
function streakOf(uid) {
  const ds = db.prepare("SELECT day FROM login_days WHERE user_id = ? ORDER BY day").all(uid).map(r => Date.parse(r.day) / 864e5);
  let best = 0, run = 0;
  ds.forEach((d, i) => { run = i && d - ds[i - 1] === 1 ? run + 1 : 1; best = Math.max(best, run); });
  const today = Date.parse(dkey(Date.now())) / 864e5, last = ds[ds.length - 1];
  return { current: last !== undefined && today - last <= 1 ? run : 0, best, total: ds.length };
}
const eventsOf = (uid, n) => db.prepare("SELECT type, data, created_at FROM events WHERE user_id = ? ORDER BY id DESC LIMIT ?").all(uid, n).map(e => ({ ...e, data: JSON.parse(e.data || "{}") }));
const follows = (a, b) => !!db.prepare("SELECT 1 FROM follows WHERE follower_id = ? AND followee_id = ?").get(a, b);
const canView = (v, o) => v.id === o.id || v.role === "admin" || o.visibility === "public" || (o.visibility === "followers" && follows(v.id, o.id));
const POSTQ = "SELECT p.id, p.body, p.created_at, u.id uid, u.name, u.title, u.visibility FROM posts p JOIN users u ON u.id = p.user_id WHERE u.disabled = 0 AND ";

app.get("/api/timeline", auth, (req, res) => res.json({ events: eventsOf(req.user.id, 50), streak: streakOf(req.user.id) }));

app.get("/api/users", auth, (req, res) => {
  const q = "%" + String(req.query.q || "").trim().replace(/[%_]/g, "") + "%";
  const rows = db.prepare("SELECT id, name, title FROM users WHERE disabled = 0 AND visibility != 'private' AND id != ? AND name LIKE ? LIMIT 10").all(req.user.id, q);
  res.json({ users: rows.map(r => ({ ...r, isFollowing: follows(req.user.id, r.id) })) });
});

app.get("/api/users/:id", auth, (req, res) => {
  const o = findById.get(Number(req.params.id));
  if (!o || o.disabled) return res.status(404).json({ error: "User not found." });
  const cnt = col => db.prepare(`SELECT COUNT(*) c FROM follows WHERE ${col} = ?`).get(o.id).c;
  const base = { id: o.id, name: o.name, title: o.title, visibility: o.visibility, followers: cnt("followee_id"), following: cnt("follower_id"), isFollowing: follows(req.user.id, o.id), isMe: o.id === req.user.id };
  if (!canView(req.user, o)) return res.json({ ...base, restricted: true });
  const self = o.id === req.user.id || req.user.role === "admin";
  res.json({ ...base, bio: o.bio, streak: self || o.show_streak ? streakOf(o.id) : null,
    posts: db.prepare(POSTQ + "p.user_id = ? ORDER BY p.id DESC LIMIT 20").all(o.id),
    activity: self || o.show_activity ? eventsOf(o.id, 20) : null });
});

app.post("/api/users/:id/follow", auth, (req, res) => {
  const o = findById.get(Number(req.params.id));
  if (!o || o.disabled || o.id === req.user.id) return res.status(400).json({ error: "You can't follow this user." });
  if (o.visibility === "private") return res.status(403).json({ error: "This profile is private." });
  if (db.prepare("INSERT OR IGNORE INTO follows VALUES (?, ?, ?)").run(req.user.id, o.id, Date.now()).changes) logEvent(req.user.id, "followed", { name: o.name });
  res.json({ ok: true });
});
app.delete("/api/users/:id/follow", auth, (req, res) => {
  db.prepare("DELETE FROM follows WHERE follower_id = ? AND followee_id = ?").run(req.user.id, Number(req.params.id));
  res.json({ ok: true });
});

app.post("/api/posts", auth, (req, res) => {
  const body = String((req.body || {}).body || "").trim().slice(0, 280);
  if (!body) return res.status(400).json({ error: "Write something first." });
  if (db.prepare("SELECT COUNT(*) c FROM posts WHERE user_id = ? AND created_at >= ?").get(req.user.id, dayStart()).c >= 20)
    return res.status(429).json({ error: "You've reached today's limit of 20 posts." });
  const id = db.prepare("INSERT INTO posts (user_id, body, created_at) VALUES (?, ?, ?)").run(req.user.id, body, Date.now()).lastInsertRowid;
  logEvent(req.user.id, "posted", { post: Number(id) });
  res.json({ ok: true });
});
app.get("/api/feed", auth, (req, res) => {
  const me = req.user.id;
  const rows = db.prepare(POSTQ + "(p.user_id = ? OR p.user_id IN (SELECT followee_id FROM follows WHERE follower_id = ?)) ORDER BY p.id DESC LIMIT 50").all(me, me);
  res.json({ posts: rows.filter(r => r.uid === me || r.visibility !== "private") });
});
app.delete("/api/posts/:id", auth, (req, res) => {
  const p = db.prepare("SELECT user_id FROM posts WHERE id = ?").get(Number(req.params.id));
  if (!p || (p.user_id !== req.user.id && req.user.role !== "admin")) return res.status(404).json({ error: "Post not found." });
  db.prepare("DELETE FROM posts WHERE id = ?").run(Number(req.params.id));
  res.json({ ok: true });
});

/* ---------- admin ---------- */
const admin = (req, res, next) => (req.user.role === "admin" ? next() : res.status(403).json({ error: "Admins only." }));
const kick = uid => { for (const s of io.sockets.sockets.values()) if (s.user.id === uid) s.disconnect(true); };
app.get("/api/admin/users", auth, admin, (_req, res) => {
  const socks = [...io.sockets.sockets.values()];
  res.json({
    users: db.prepare(`SELECT id, email, name, title, role, disabled,
      (SELECT COUNT(*) FROM encouragements WHERE from_id = users.id) sent,
      (SELECT COUNT(*) FROM encouragements WHERE to_id = users.id) received FROM users ORDER BY id`).all(),
    online: [...new Set(socks.map(x => x.user.id))],
    live: socks.filter(x => x.room).map(x => ({ room: x.room, name: x.user.name }))
  });
});
app.patch("/api/admin/users/:id", auth, admin, wrap(async (req, res) => {
  const id = Number(req.params.id);
  if (!findById.get(id)) return res.status(404).json({ error: "User not found." });
  const { role, disabled, password } = req.body || {};
  if (id === req.user.id && (role === "user" || disabled)) return res.status(400).json({ error: "You can't demote or disable yourself." });
  if (role) db.prepare("UPDATE users SET role = ? WHERE id = ?").run(role === "admin" ? "admin" : "user", id);
  if (disabled !== undefined) db.prepare("UPDATE users SET disabled = ? WHERE id = ?").run(disabled ? 1 : 0, id);
  if (password !== undefined) {
    if (String(password).length < 8) return res.status(400).json({ error: "Password must be at least 8 characters." });
    db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(await bcrypt.hash(String(password), 10), id);
  }
  if (disabled) kick(id);
  res.json({ ok: true });
}));
app.delete("/api/admin/users/:id", auth, admin, (req, res) => {
  const id = Number(req.params.id);
  if (id === req.user.id) return res.status(400).json({ error: "You can't delete yourself." });
  db.prepare("DELETE FROM users WHERE id = ?").run(id);
  kick(id);
  res.json({ ok: true });
});

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
  try { db.prepare("SELECT 1").get(); res.json({ ok: true, db: "ok", permanent: !!pool }); }
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
    if (!u || u.disabled) throw new Error("no user");
    socket.user = { id: u.id, name: u.name, title: u.title || "", bio: u.bio || "" };
    next();
  } catch { next(new Error("unauthorized")); }
});

const peerInfo = s => ({
  id: s.id, userId: s.user.id, name: s.user.name, title: s.user.title, bio: s.user.bio,
  pomo: s.pomo ? { ...s.pomo, left: s.pomo.running ? Math.max(0, s.pomo.left - (Date.now() - s.pomo.at)) : s.pomo.left } : null
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
    logEvent(socket.user.id, "joined_room", { room });
    socket.to(room).emit("peer-joined", peerInfo(socket));
    ack({ peers: members.map(peerInfo) });
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

  // encouragement: one per cooldown window, max per day, enforced here on the server
  socket.on("encourage", ({ to, message } = {}, ack) => {
    if (typeof ack !== "function") return;
    const t = io.sockets.sockets.get(to);
    if (!t || !socket.room || t.room !== socket.room || t.user.id === socket.user.id)
      return ack({ error: "Pick someone else in this meeting." });
    const msg = String(message || "You've got this!").trim().slice(0, 80) || "You've got this!";
    const now = Date.now();
    const result = db.transaction(() => {
      const q = quota(socket.user.id);
      if (q.left <= 0) return { error: `You've used all ${DAILY} encouragements for today. They reset at midnight.` };
      if (now < q.nextAt) return { error: `You can encourage again in ${Math.ceil((q.nextAt - now) / 60000)} min.` };
      db.prepare("INSERT INTO encouragements (from_id, to_id, message, created_at) VALUES (?, ?, ?, ?)").run(socket.user.id, t.user.id, msg, now);
      logEvent(socket.user.id, "encouraged", { to: t.user.name });
      logEvent(t.user.id, "encouraged_by", { from: socket.user.name });
      return { ok: true };
    })();
    if (!result.ok) return ack({ ...result, quota: quota(socket.user.id) });
    t.emit("encouraged", { from: socket.user.name, message: msg });
    ack({ ok: true, quota: quota(socket.user.id) });
  });

  // pomodoro status, shown on your tile for others
  socket.on("pomo", p => {
    if (!socket.room) return;
    socket.pomo = p && typeof p === "object"
      ? { phase: String(p.phase).slice(0, 10), running: !!p.running, left: Math.max(0, Number(p.left) || 0), at: Date.now() } : null;
    socket.to(socket.room).emit("pomo", { id: socket.id, pomo: socket.pomo });
  });

  socket.on("pomo-done", () => logEvent(socket.user.id, "pomodoro", {}));

  socket.on("profile-updated", () => {
    const u = findById.get(socket.user.id);
    if (!u) return;
    Object.assign(socket.user, { name: u.name, title: u.title || "", bio: u.bio || "" });
    if (socket.room) socket.to(socket.room).emit("peer-updated", peerInfo(socket));
  });

  socket.on("disconnecting", () => {
    if (socket.room) socket.to(socket.room).emit("peer-left", { id: socket.id });
  });
});

server.listen(PORT, () => console.log(`Chaupal running on http://localhost:${PORT}`));
