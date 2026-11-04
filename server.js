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
import crypto from "crypto";

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
for (const [c, d] of [["title", "TEXT DEFAULT ''"], ["bio", "TEXT DEFAULT ''"], ["role", "TEXT DEFAULT 'user'"], ["disabled", "INTEGER DEFAULT 0"], ["visibility", "TEXT DEFAULT 'public'"], ["show_streak", "INTEGER DEFAULT 1"], ["show_activity", "INTEGER DEFAULT 0"], ["daily_goal", "INTEGER DEFAULT 4"], ["show_leaderboard", "INTEGER DEFAULT 1"]])
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
db.exec(`CREATE TABLE IF NOT EXISTS reset_tokens (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, token_hash TEXT NOT NULL, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL)`);
db.exec(`CREATE TABLE IF NOT EXISTS rooms (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, description TEXT DEFAULT '',
  video_allowed INTEGER DEFAULT 1, audio_allowed INTEGER DEFAULT 1, active INTEGER DEFAULT 1, created_by INTEGER, created_at INTEGER)`);
db.exec(`CREATE TABLE IF NOT EXISTS dms (id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL, read_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_dms_to ON dms(to_id, read_at);
CREATE INDEX IF NOT EXISTS idx_dms_pair ON dms(from_id, to_id);`);
db.exec(`CREATE TABLE IF NOT EXISTS blocks (blocker_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (blocker_id, blocked_id));
CREATE TABLE IF NOT EXISTS reports (id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT NOT NULL, ref_id INTEGER, reason TEXT, context TEXT, status TEXT DEFAULT 'open', created_at INTEGER NOT NULL);`);
db.exec(`CREATE TABLE IF NOT EXISTS likes (post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (post_id, user_id));
CREATE TABLE IF NOT EXISTS notifs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, type TEXT NOT NULL, actor_id INTEGER NOT NULL, ref_id INTEGER, text TEXT, created_at INTEGER NOT NULL, read_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_notifs_user ON notifs(user_id, read_at);`);
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

/* ---------- forgot / reset password ---------- */
const hashTok = t => crypto.createHash("sha256").update(t).digest("hex");
const htmlEsc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
async function sendMail(to, subject, html, text) {
  if (!process.env.BREVO_API_KEY || !process.env.MAIL_FROM) { console.log("[mail not configured] To:", to, "|", text); return false; }
  const r = await fetch("https://api.brevo.com/v3/smtp/email", {
    method: "POST",
    headers: { "api-key": process.env.BREVO_API_KEY, "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ sender: { name: "Chaupal", email: process.env.MAIL_FROM }, to: [{ email: to }], subject, htmlContent: html, textContent: text })
  });
  if (!r.ok) console.error("Mail failed:", r.status, (await r.text()).slice(0, 200));
  return r.ok;
}
// always answers "ok" so nobody can use this to find out which emails have accounts
app.post("/api/forgot", limit, wrap(async (req, res) => {
  const e = String((req.body || {}).email || "").trim().toLowerCase();
  const u = EMAIL_RE.test(e) ? findByEmail.get(e) : null;
  if (u && !u.disabled && !db.prepare("SELECT 1 FROM reset_tokens WHERE user_id = ? AND created_at > ?").get(u.id, Date.now() - 120000)) {
    const tok = crypto.randomBytes(32).toString("hex"), now = Date.now();
    db.prepare("DELETE FROM reset_tokens WHERE user_id = ? OR expires_at < ?").run(u.id, now);
    db.prepare("INSERT INTO reset_tokens (user_id, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)").run(u.id, hashTok(tok), now + 30 * 60000, now);
    const base = (process.env.APP_URL || "").replace(/\/$/, "") || req.protocol + "://" + req.get("host");
    const link = `${base}/?reset=${tok}`;
    sendMail(u.email, "Reset your Chaupal password",
      `<p>Hi ${htmlEsc(u.name)},</p><p>Click to choose a new password. The link works for 30 minutes.</p><p><a href="${link}">Reset my password</a></p><p>If you didn't ask for this, ignore this email.</p>`,
      `Hi ${u.name}, open this link to choose a new password (valid 30 minutes): ${link}`).catch(e2 => console.error("Mail error:", e2.message));
  }
  res.json({ ok: true });
}));
app.post("/api/reset", limit, wrap(async (req, res) => {
  const { token, password } = req.body || {};
  if (String(password || "").length < 8) return res.status(400).json({ error: "Password must be at least 8 characters." });
  const row = db.prepare("SELECT * FROM reset_tokens WHERE token_hash = ? AND expires_at > ?").get(hashTok(String(token || "")), Date.now());
  if (!row) return res.status(400).json({ error: "This reset link is invalid or has expired. Request a new one." });
  db.prepare("UPDATE users SET password_hash = ? WHERE id = ?").run(await bcrypt.hash(String(password), 10), row.user_id);
  db.prepare("DELETE FROM reset_tokens WHERE user_id = ?").run(row.user_id);
  res.json({ ok: true });
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

app.get("/api/me", auth, (req, res) => { recordLogin(req.user.id); res.json({ user: publicUser(req.user), quota: quota(req.user.id), streak: streakOf(req.user.id), unread: unreadOf(req.user.id), notifUnread: notifUnread(req.user.id) }); });
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
  const rows = db.prepare("SELECT id, name, title FROM users WHERE disabled = 0 AND visibility != 'private' AND id != ? AND name LIKE ? AND id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id = ?) AND id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id = ?) LIMIT 10").all(req.user.id, q, req.user.id, req.user.id);
  res.json({ users: rows.map(r => ({ ...r, isFollowing: follows(req.user.id, r.id) })) });
});

app.get("/api/users/:id", auth, (req, res) => {
  const o = findById.get(Number(req.params.id));
  if (!o || o.disabled) return res.status(404).json({ error: "User not found." });
  const cnt = col => db.prepare(`SELECT COUNT(*) c FROM follows WHERE ${col} = ?`).get(o.id).c;
  const base = { id: o.id, name: o.name, title: o.title, visibility: o.visibility, followers: cnt("followee_id"), following: cnt("follower_id"), isFollowing: follows(req.user.id, o.id), isMe: o.id === req.user.id, iBlocked: !!db.prepare("SELECT 1 FROM blocks WHERE blocker_id = ? AND blocked_id = ?").get(req.user.id, o.id) };
  if (blockedEither(req.user.id, o.id) && req.user.role !== "admin") return res.json({ ...base, restricted: true, blocked: true });
  if (!canView(req.user, o)) return res.json({ ...base, restricted: true });
  const self = o.id === req.user.id || req.user.role === "admin";
  res.json({ ...base, bio: o.bio, streak: self || o.show_streak ? streakOf(o.id) : null,
    posts: withLikes(db.prepare(POSTQ + "p.user_id = ? ORDER BY p.id DESC LIMIT 20").all(o.id), req.user.id),
    activity: self || o.show_activity ? eventsOf(o.id, 20) : null,
    stats: (() => { const ec = t => db.prepare("SELECT COUNT(*) c FROM events WHERE user_id = ? AND type = ?").get(o.id, t).c; return { focus: ec("pomodoro"), sent: ec("encouraged"), received: ec("encouraged_by"), posts: db.prepare("SELECT COUNT(*) c FROM posts WHERE user_id = ?").get(o.id).c }; })() });
});

app.post("/api/users/:id/follow", auth, (req, res) => {
  const o = findById.get(Number(req.params.id));
  if (!o || o.disabled || o.id === req.user.id) return res.status(400).json({ error: "You can't follow this user." });
  if (blockedEither(req.user.id, o.id)) return res.status(403).json({ error: "You can't follow this user." });
  if (o.visibility === "private") return res.status(403).json({ error: "This profile is private." });
  if (db.prepare("INSERT OR IGNORE INTO follows VALUES (?, ?, ?)").run(req.user.id, o.id, Date.now()).changes){
    logEvent(req.user.id, "followed", { name: o.name });
    const back = follows(o.id, req.user.id);   // they already follow me, so this is a follow-back for them
    notify(o.id, back ? "followback" : "follow", req.user, null, null, req.user.name + (back ? " followed you back" : " started following you"));
  }
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
  res.json({ posts: withLikes(rows.filter(r => (r.uid === me || r.visibility !== "private") && !blockedEither(me, r.uid)), me) });
});
app.delete("/api/posts/:id", auth, (req, res) => {
  const p = db.prepare("SELECT user_id FROM posts WHERE id = ?").get(Number(req.params.id));
  if (!p || (p.user_id !== req.user.id && req.user.role !== "admin")) return res.status(404).json({ error: "Post not found." });
  db.prepare("DELETE FROM posts WHERE id = ?").run(Number(req.params.id));
  res.json({ ok: true });
});

/* ---------- direct messages (1 to 1) ---------- */
const unreadOf = uid => db.prepare("SELECT COUNT(*) c FROM dms WHERE to_id = ? AND read_at IS NULL").get(uid).c;
const dmHits = new Map();
app.get("/api/chats", auth, (req, res) => {
  const me = req.user.id;
  const rows = db.prepare(`SELECT other, MAX(id) mid, SUM(CASE WHEN to_id = ? AND read_at IS NULL THEN 1 ELSE 0 END) unread
    FROM (SELECT id, to_id, read_at, CASE WHEN from_id = ? THEN to_id ELSE from_id END other FROM dms WHERE from_id = ? OR to_id = ?)
    GROUP BY other ORDER BY mid DESC LIMIT 100`).all(me, me, me, me);
  const chats = rows.map(r => {
    const u = findById.get(r.other), m = db.prepare("SELECT body, created_at, from_id FROM dms WHERE id = ?").get(r.mid);
    return u && !u.disabled && !blockedEither(me, u.id) ? { id: u.id, name: u.name, title: u.title || "", last: m.body, at: m.created_at, mine: m.from_id === me, unread: r.unread } : null;
  }).filter(Boolean);
  res.json({ chats, unread: unreadOf(me) });
});
app.post("/api/chats/read-all", auth, (req, res) => {
  db.prepare("UPDATE dms SET read_at = ? WHERE to_id = ? AND read_at IS NULL").run(Date.now(), req.user.id);
  res.json({ ok: true, unread: 0 });
});
app.get("/api/chats/:id", auth, (req, res) => {
  const me = req.user.id, o = findById.get(Number(req.params.id));
  if (!o || o.disabled) return res.status(404).json({ error: "User not found." });
  db.prepare("UPDATE dms SET read_at = ? WHERE to_id = ? AND from_id = ? AND read_at IS NULL").run(Date.now(), me, o.id);
  const messages = db.prepare(`SELECT id, from_id, body, created_at FROM (SELECT * FROM dms WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?) ORDER BY id DESC LIMIT 100) ORDER BY id`).all(me, o.id, o.id, me);
  res.json({ user: { id: o.id, name: o.name, title: o.title || "" }, messages, unread: unreadOf(me) });
});
app.post("/api/chats/:id", auth, (req, res) => {
  const me = req.user.id, o = findById.get(Number(req.params.id));
  if (!o || o.disabled || o.id === me || blockedEither(me, o.id)) return res.status(400).json({ error: "You can't message this user." });
  const body = String((req.body || {}).body || "").trim().slice(0, 1000);
  if (!body) return res.status(400).json({ error: "Write a message first." });
  if (o.visibility === "private" && !db.prepare("SELECT 1 FROM dms WHERE from_id = ? AND to_id = ?").get(o.id, me))
    return res.status(403).json({ error: "This user isn't accepting messages." });
  const now = Date.now(), recent = (dmHits.get(me) || []).filter(t => now - t < 10000);
  if (recent.length >= 15) return res.status(429).json({ error: "You're sending messages too fast. Wait a few seconds." });
  recent.push(now); dmHits.set(me, recent);
  const id = Number(db.prepare("INSERT INTO dms (from_id, to_id, body, created_at) VALUES (?, ?, ?, ?)").run(me, o.id, body, now).lastInsertRowid);
  const msg = { id, from_id: me, body, created_at: now };
  for (const s of io.sockets.sockets.values()) if (s.user.id === o.id && s.handshake.query.dm === "1") s.emit("dm", { ...msg, name: req.user.name });
  res.json({ message: msg });
});

/* ---------- likes and notifications ---------- */
const likeCount = pid => db.prepare("SELECT COUNT(*) c FROM likes WHERE post_id = ?").get(pid).c;
const withLikes = (rows, me) => rows.map(r => ({ ...r, likes: likeCount(r.id), liked: !!db.prepare("SELECT 1 FROM likes WHERE post_id = ? AND user_id = ?").get(r.id, me) }));
const notifUnread = uid => db.prepare("SELECT COUNT(*) c FROM notifs WHERE user_id = ? AND read_at IS NULL").get(uid).c;
function notify(uid, type, actor, ref, text, toastText){
  if (uid === actor.id || blockedEither(uid, actor.id)) return;
  if (db.prepare("SELECT 1 FROM notifs WHERE user_id = ? AND type = ? AND actor_id = ? AND IFNULL(ref_id, 0) = IFNULL(?, 0) AND read_at IS NULL").get(uid, type, actor.id, ref ?? null)) return;   // no repeats while the first is unread
  db.prepare("INSERT INTO notifs (user_id, type, actor_id, ref_id, text, created_at) VALUES (?, ?, ?, ?, ?, ?)").run(uid, type, actor.id, ref ?? null, text || null, Date.now());
  for (const s of io.sockets.sockets.values()) if (s.user.id === uid && s.handshake.query.dm === "1") s.emit("notif", { type, text: toastText });
}
app.post("/api/posts/:id/like", auth, (req, res) => {
  const p = db.prepare("SELECT id, user_id, body FROM posts WHERE id = ?").get(Number(req.params.id));
  if (!p || blockedEither(req.user.id, p.user_id)) return res.status(404).json({ error: "Post not found." });
  if (db.prepare("INSERT OR IGNORE INTO likes VALUES (?, ?, ?)").run(p.id, req.user.id, Date.now()).changes)
    notify(p.user_id, "like", req.user, p.id, p.body.slice(0, 60), req.user.name + " liked your post");
  res.json({ liked: true, likes: likeCount(p.id) });
});
app.delete("/api/posts/:id/like", auth, (req, res) => {
  db.prepare("DELETE FROM likes WHERE post_id = ? AND user_id = ?").run(Number(req.params.id), req.user.id);
  res.json({ liked: false, likes: likeCount(Number(req.params.id)) });
});
app.get("/api/notifs", auth, (req, res) => {
  const me = req.user.id;
  const rows = db.prepare(`SELECT n.id, n.type, n.actor_id, n.ref_id, n.text, n.created_at, n.read_at, u.name FROM notifs n JOIN users u ON u.id = n.actor_id WHERE n.user_id = ? AND u.disabled = 0 ORDER BY n.id DESC LIMIT 50`).all(me)
    .filter(r => !blockedEither(me, r.actor_id))
    .map(r => ({ id: r.id, type: r.type, ref: r.ref_id, text: r.text, created_at: r.created_at, read: !!r.read_at, actor: { id: r.actor_id, name: r.name }, following: follows(me, r.actor_id) }));
  res.json({ notifs: rows, unread: notifUnread(me) });
});
app.post("/api/notifs/read-all", auth, (req, res) => { db.prepare("UPDATE notifs SET read_at = ? WHERE user_id = ? AND read_at IS NULL").run(Date.now(), req.user.id); res.json({ ok: true, unread: 0 }); });
app.post("/api/notifs/:id/read", auth, (req, res) => { db.prepare("UPDATE notifs SET read_at = ? WHERE id = ? AND user_id = ? AND read_at IS NULL").run(Date.now(), Number(req.params.id), req.user.id); res.json({ ok: true, unread: notifUnread(req.user.id) }); });

/* ---------- safety: block and report ---------- */
const blockedEither = (a, b) => !!db.prepare("SELECT 1 FROM blocks WHERE (blocker_id = ? AND blocked_id = ?) OR (blocker_id = ? AND blocked_id = ?)").get(a, b, b, a);
app.get("/api/blocks", auth, (req, res) => res.json({ ids: db.prepare("SELECT blocked_id FROM blocks WHERE blocker_id = ?").all(req.user.id).map(r => r.blocked_id) }));
app.post("/api/block/:id", auth, (req, res) => {
  const me = req.user.id, o = findById.get(Number(req.params.id));
  if (!o || o.id === me) return res.status(400).json({ error: "You can't block this user." });
  db.prepare("INSERT OR IGNORE INTO blocks VALUES (?, ?, ?)").run(me, o.id, Date.now());
  db.prepare("DELETE FROM follows WHERE (follower_id = ? AND followee_id = ?) OR (follower_id = ? AND followee_id = ?)").run(me, o.id, o.id, me);
  res.json({ ok: true });
});
app.delete("/api/block/:id", auth, (req, res) => { db.prepare("DELETE FROM blocks WHERE blocker_id = ? AND blocked_id = ?").run(req.user.id, Number(req.params.id)); res.json({ ok: true }); });
const repHits = new Map();
app.post("/api/report", auth, (req, res) => {
  const me = req.user.id, b = req.body || {}, target = findById.get(Number(b.userId));
  if (!target || target.id === me) return res.status(400).json({ error: "You can't report this user." });
  const kind = b.kind === "post" ? "post" : "user"; let context = "", ref = null;
  if (kind === "post"){
    const p = db.prepare("SELECT id, user_id, body FROM posts WHERE id = ?").get(Number(b.refId));
    if (!p || p.user_id !== target.id) return res.status(400).json({ error: "Post not found." });
    ref = p.id; context = p.body;
  }
  const now = Date.now(), recent = (repHits.get(me) || []).filter(t => now - t < 3600000);
  if (recent.length >= 10) return res.status(429).json({ error: "You've sent a lot of reports. Please try again later." });
  recent.push(now); repHits.set(me, recent);
  db.prepare("INSERT INTO reports (reporter_id, target_id, kind, ref_id, reason, context, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)").run(me, target.id, kind, ref, String(b.reason || "").trim().slice(0, 300), context.slice(0, 280), now);
  res.json({ ok: true });
});

/* ---------- weekly leaderboard (opt-out; private profiles never appear) ---------- */
app.get("/api/leaderboard", auth, (req, res) => {
  const me = req.user.id, since = Date.parse(dkey(Date.now() - 6 * 864e5)) - TZ;   // start of the local day six days ago
  const board = db.prepare(`SELECT e.user_id id, u.name, u.title, COUNT(*) sessions FROM events e JOIN users u ON u.id = e.user_id
      WHERE e.type = 'pomodoro' AND e.created_at >= ? AND u.disabled = 0 AND u.show_leaderboard = 1 AND u.visibility != 'private'
      GROUP BY e.user_id ORDER BY sessions DESC, MIN(e.created_at) ASC LIMIT 200`).all(since)
    .filter(r => r.id === me || !blockedEither(me, r.id))
    .map((r, i) => ({ rank: i + 1, id: r.id, name: r.name, title: r.title || "", sessions: r.sessions, minutes: r.sessions * 25, me: r.id === me }));
  res.json({ top: board.slice(0, 20), me: board.find(b => b.me) || null, total: board.length, optedIn: !!db.prepare("SELECT show_leaderboard s FROM users WHERE id = ?").get(me).s });
});
app.put("/api/leaderboard/optin", auth, (req, res) => {
  db.prepare("UPDATE users SET show_leaderboard = ? WHERE id = ?").run((req.body || {}).on ? 1 : 0, req.user.id); res.json({ ok: true });
});

/* ---------- study dashboard ---------- */
app.get("/api/dashboard", auth, (req, res) => {
  const me = req.user.id, today = dkey(Date.now()), days = [];
  for (let i = 6; i >= 0; i--) days.push(dkey(Date.now() - i * 864e5));
  const per = Object.fromEntries(days.map(d => [d, 0]));
  db.prepare("SELECT created_at FROM events WHERE user_id = ? AND type = 'pomodoro' AND created_at >= ?").all(me, Date.now() - 9 * 864e5).forEach(r => { const k = dkey(r.created_at); if (k in per) per[k]++; });
  const week = days.map(d => ({ day: d, sessions: per[d] }));
  res.json({ goal: db.prepare("SELECT daily_goal g FROM users WHERE id = ?").get(me).g || 4, today: per[today], week, weekTotal: week.reduce((a, d) => a + d.sessions, 0),
    total: db.prepare("SELECT COUNT(*) c FROM events WHERE user_id = ? AND type = 'pomodoro'").get(me).c, minutesPerSession: 25, streak: streakOf(me) });
});
app.put("/api/goal", auth, (req, res) => {
  const g = Math.max(1, Math.min(12, Math.round(Number((req.body || {}).goal) || 4)));
  db.prepare("UPDATE users SET daily_goal = ? WHERE id = ?").run(g, req.user.id); res.json({ goal: g });
});

/* ---------- admin ---------- */
const admin = (req, res, next) => (req.user.role === "admin" ? next() : res.status(403).json({ error: "Admins only." }));
const kick = uid => { for (const s of io.sockets.sockets.values()) if (s.user.id === uid) s.disconnect(true); };
/* ---------- study rooms (created by admins) ---------- */
const genRoomCode = () => { const r = n => Array.from(crypto.randomBytes(n), b => "abcdefghijkmnpqrstuvwxyz"[b % 24]).join(""); return `${r(3)}-${r(4)}-${r(3)}`; };
const roomOut = r => ({ id: r.id, code: r.code, name: r.name, description: r.description, video: !!r.video_allowed, audio: !!r.audio_allowed, active: !!r.active, live: io.sockets.adapter.rooms.get(r.code)?.size || 0 });
app.get("/api/rooms", auth, (_req, res) => res.json({ rooms: db.prepare("SELECT * FROM rooms WHERE active = 1 ORDER BY id DESC").all().map(roomOut) }));
app.get("/api/admin/rooms", auth, admin, (_req, res) => res.json({ rooms: db.prepare("SELECT * FROM rooms ORDER BY id DESC").all().map(roomOut) }));
app.post("/api/admin/rooms", auth, admin, (req, res) => {
  const b = req.body || {}, name = String(b.name || "").trim().slice(0, 60);
  if (!name) return res.status(400).json({ error: "Give the room a name." });
  const code = genRoomCode();
  db.prepare("INSERT INTO rooms (code, name, description, video_allowed, audio_allowed, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
    .run(code, name, String(b.description || "").trim().slice(0, 140), b.video === false ? 0 : 1, b.audio === false ? 0 : 1, req.user.id, Date.now());
  res.json({ room: roomOut(db.prepare("SELECT * FROM rooms WHERE code = ?").get(code)) });
});
app.patch("/api/admin/rooms/:id", auth, admin, (req, res) => {
  const r = db.prepare("SELECT * FROM rooms WHERE id = ?").get(Number(req.params.id));
  if (!r) return res.status(404).json({ error: "Room not found." });
  const b = req.body || {};
  const v = b.video === undefined ? r.video_allowed : b.video ? 1 : 0, a = b.audio === undefined ? r.audio_allowed : b.audio ? 1 : 0, act = b.active === undefined ? r.active : b.active ? 1 : 0;
  db.prepare("UPDATE rooms SET name = ?, description = ?, video_allowed = ?, audio_allowed = ?, active = ? WHERE id = ?")
    .run(String(b.name ?? r.name).trim().slice(0, 60) || r.name, String(b.description ?? r.description).slice(0, 140), v, a, act, r.id);
  const nr = db.prepare("SELECT * FROM rooms WHERE id = ?").get(r.id);
  io.in(r.code).emit("room-settings", { name: nr.name, video: !!nr.video_allowed, audio: !!nr.audio_allowed });
  if (!act) { io.in(r.code).emit("room-closed"); io.in(r.code).disconnectSockets(true); }
  res.json({ room: roomOut(nr) });
});
app.delete("/api/admin/rooms/:id", auth, admin, (req, res) => {
  const r = db.prepare("SELECT * FROM rooms WHERE id = ?").get(Number(req.params.id));
  if (r) { db.prepare("DELETE FROM rooms WHERE id = ?").run(r.id); io.in(r.code).emit("room-closed"); io.in(r.code).disconnectSockets(true); }
  res.json({ ok: true });
});

app.get("/api/admin/reports", auth, admin, (req, res) => {
  res.json({ reports: db.prepare(`SELECT r.id, r.kind, r.reason, r.context, r.status, r.created_at, r.target_id, a.name reporter, t.name target, t.disabled target_disabled
    FROM reports r LEFT JOIN users a ON a.id = r.reporter_id LEFT JOIN users t ON t.id = r.target_id ${req.query.status === "all" ? "" : "WHERE r.status = 'open'"} ORDER BY r.id DESC LIMIT 200`).all() });
});
app.patch("/api/admin/reports/:id", auth, admin, (req, res) => {
  const st = (req.body || {}).status;
  if (!["resolved", "dismissed"].includes(st)) return res.status(400).json({ error: "Bad status." });
  db.prepare("UPDATE reports SET status = ? WHERE id = ?").run(st, Number(req.params.id)); res.json({ ok: true });
});
app.get("/api/admin/users", auth, admin, (_req, res) => {
  const socks = [...io.sockets.sockets.values()];
  res.json({
    users: db.prepare(`SELECT id, email, name, title, role, disabled,
      (SELECT COUNT(*) FROM encouragements WHERE from_id = users.id) sent,
      (SELECT COUNT(*) FROM encouragements WHERE to_id = users.id) received FROM users ORDER BY id`).all(),
    online: [...new Set(socks.map(x => x.user.id))],
    turn: turnReady(), turnMissing: turnConfig().missing,
    live: socks.filter(x => x.room).map(x => ({ room: x.room, name: x.user.name }))
  });
});
app.patch("/api/admin/users/:id", auth, admin, wrap(async (req, res) => {
  const id = Number(req.params.id);
  if (!findById.get(id)) return res.status(404).json({ error: "User not found." });
  const { role, disabled, password } = req.body || {};
  if (id === req.user.id && (role === "user" || disabled)) return res.status(400).json({ error: "You can't demote or disable yourself." });
  if (role) { const nr = role === "admin" ? "admin" : "user"; db.prepare("UPDATE users SET role = ? WHERE id = ?").run(nr, id); for (const s of io.sockets.sockets.values()) if (s.user.id === id) s.user.role = nr; }
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

// reads TURN_URL / TURN_USERNAME / TURN_CREDENTIAL forgivingly: quotes, spaces, a bare hostname, or host:port all work
function turnConfig(){
  const clean = v => String(v || "").trim().replace(/^["']+|["']+$/g, "").trim();
  const url = clean(process.env.TURN_URL), user = clean(process.env.TURN_USERNAME), cred = clean(process.env.TURN_CREDENTIAL);
  const missing = [["TURN_URL", url], ["TURN_USERNAME", user], ["TURN_CREDENTIAL", cred]].filter(x => !x[1]).map(x => x[0]);
  const urls = url.split(/[\s,]+/).filter(Boolean).map(u => u.replace(/^https?:\/\//i, "").replace(/\/+$/, "")).flatMap(u =>
    /^(turns?|stuns?):/i.test(u) ? [u]
    : /^[\w.-]+:\d+$/.test(u) ? ["turn:" + u]
    : ["turn:" + u + ":80", "turn:" + u + ":80?transport=tcp", "turn:" + u + ":443", "turns:" + u + ":443?transport=tcp"]);
  return { ready: !missing.length && urls.length > 0, missing, urls, user, cred };
}
const turnReady = () => turnConfig().ready;
if (process.env.TURN_URL || process.env.TURN_USERNAME || process.env.TURN_CREDENTIAL){
  const c = turnConfig();
  console.log(c.ready ? "TURN relay is ON (" + c.urls.length + " addresses)." : "TURN relay is OFF. Missing: " + c.missing.join(", "));
}
app.get("/api/ice", auth, (req, res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:global.stun.twilio.com:3478" }
  ];
  const c = turnConfig();
  if (c.ready) iceServers.push({ urls: c.urls, username: c.user, credential: c.cred });
  res.json({ iceServers, relay: c.ready, missing: c.missing });
});

app.get("/healthz", (_req, res) => {
  try { db.prepare("SELECT 1").get(); res.json({ ok: true, db: "ok", permanent: !!pool, turn: turnReady(), turnMissing: turnConfig().missing }); }
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
    socket.user = { id: u.id, name: u.name, title: u.title || "", bio: u.bio || "", role: u.role || "user" };
    next();
  } catch { next(new Error("unauthorized")); }
});

const peerInfo = s => ({
  id: s.id, userId: s.user.id, name: s.user.name, title: s.user.title, bio: s.user.bio, goal: s.goal || "", sessions: s.sessions || 0, cheers: s.cheers || 0,
  pomo: s.pomo ? { ...s.pomo, left: s.pomo.running ? Math.max(0, s.pomo.left - (Date.now() - s.pomo.at)) : s.pomo.left } : null
});
/* group timer: one shared Pomodoro clock per room, kept by the server so everyone stays in sync */
const GTS = Number(process.env.GT_SCALE || 1), GT_MS = { focus: 25 * 60000 * GTS, short: 5 * 60000 * GTS, long: 15 * 60000 * GTS };
const GT = new Map();
const gtLeft = g => g.running ? Math.max(0, g.left - (Date.now() - g.at)) : g.left;
const gtOut = room => { const g = GT.get(room); return g ? { host: g.host, hostName: g.hostName, phase: g.phase, running: g.running, left: gtLeft(g), done: g.done } : null; };
function gtArm(room){ const g = GT.get(room); if (!g) return; clearTimeout(g.timer); if (g.running) g.timer = setTimeout(() => gtEnd(room), gtLeft(g)); }
function gtEnd(room){
  const g = GT.get(room); if (!g) return; const was = g.phase;
  if (was === "focus"){ g.done++; g.phase = g.done % 4 === 0 ? "long" : "short"; } else g.phase = "focus";
  g.left = GT_MS[g.phase]; g.at = Date.now(); g.running = true; gtArm(room);
  io.in(room).emit("rt-state", { ...gtOut(room), completed: was });
}
const ROOM_RE = /^[a-z]{3}-[a-z]{4}-[a-z]{3}$/;

io.on("connection", socket => {
  socket.on("join", async (room, ack) => {
    if (typeof ack !== "function") return;
    if (!ROOM_RE.test(room)) return ack({ error: "Invalid meeting code." });
    if (socket.room) return ack({ error: "Already in a meeting." });
    const rm = db.prepare("SELECT * FROM rooms WHERE code = ?").get(room);
    if (rm && !rm.active) return ack({ error: "This room has been closed by the admin." });
    const members = await io.in(room).fetchSockets();
    if (members.length >= MAX_ROOM_SIZE) return ack({ error: `This meeting is full (${MAX_ROOM_SIZE} people max).` });
    socket.join(room);
    socket.room = room;
    logEvent(socket.user.id, "joined_room", { room });
    socket.to(room).emit("peer-joined", peerInfo(socket));
    ack({ peers: members.map(peerInfo), timer: gtOut(room), settings: rm ? { name: rm.name, video: !!rm.video_allowed, audio: !!rm.audio_allowed } : null });
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
    t.cheers = (t.cheers || 0) + 1;
    notify(t.user.id, "cheer", socket.user, Math.floor(Date.now() / 1000), msg, socket.user.name + " sent you encouragement");
    t.to(t.room).emit("peer-updated", peerInfo(t));
    ack({ ok: true, quota: quota(socket.user.id) });
  });

  // pomodoro status, shown on your tile for others
  socket.on("pomo", p => {
    if (!socket.room) return;
    socket.pomo = p && typeof p === "object"
      ? { phase: String(p.phase).slice(0, 10), running: !!p.running, left: Math.max(0, Number(p.left) || 0), at: Date.now() } : null;
    socket.to(socket.room).emit("pomo", { id: socket.id, pomo: socket.pomo });
  });

  // admin controls: allow/stop someone's mic or camera, or remove them (admins only, never against other admins)
  socket.on("admin-action", ({ to, action } = {}) => {
    const t = io.sockets.sockets.get(to);
    if (socket.user.role !== "admin" || !t || !socket.room || t.room !== socket.room || t.user.role === "admin") return;
    if (!["mic-off", "mic-on", "cam-off", "cam-on", "remove"].includes(action)) return;
    t.emit("forced", { action });
    if (action === "remove") setTimeout(() => t.disconnect(true), 300);
  });

  socket.on("rt-start", () => {
    if (!socket.room || GT.has(socket.room)) return;
    GT.set(socket.room, { host: socket.user.id, hostName: socket.user.name, phase: "focus", running: false, left: GT_MS.focus, at: Date.now(), done: 0, timer: null });
    io.in(socket.room).emit("rt-state", gtOut(socket.room));
  });
  socket.on("rt-act", ({ action } = {}) => {
    const room = socket.room, g = room && GT.get(room);
    if (!g || (g.host !== socket.user.id && socket.user.role !== "admin")) return;
    if (action === "start" && !g.running){ g.running = true; g.at = Date.now(); gtArm(room); }
    else if (action === "pause" && g.running){ g.left = gtLeft(g); g.running = false; clearTimeout(g.timer); }
    else if (action === "reset"){ g.running = false; clearTimeout(g.timer); g.left = GT_MS[g.phase]; }
    else if (action === "skip"){ return gtEnd(room); }
    else if (action === "stop"){ clearTimeout(g.timer); GT.delete(room); return io.in(room).emit("rt-state", null); }
    else return;
    io.in(room).emit("rt-state", gtOut(room));
  });
  socket.on("disconnect", () => {   // if the host leaves, the next person takes over; an empty room clears its timer
    const room = socket.room, g = room && GT.get(room); if (!g) return;
    const ids = [...(io.sockets.adapter.rooms.get(room) || [])].map(id => io.sockets.sockets.get(id)).filter(Boolean);
    if (!ids.length){ clearTimeout(g.timer); GT.delete(room); return; }
    if (!ids.some(x => x.user.id === g.host)){ g.host = ids[0].user.id; g.hostName = ids[0].user.name; io.in(room).emit("rt-state", gtOut(room)); }
  });
  socket.on("pomo-done", () => {
    logEvent(socket.user.id, "pomodoro", {});
    socket.sessions = (socket.sessions || 0) + 1;
    if (socket.room) socket.to(socket.room).emit("peer-updated", peerInfo(socket));
  });
  // what you are working on, shown under your name on your tile
  socket.on("goal", g => {
    socket.goal = String(g || "").trim().slice(0, 80);
    if (socket.room) socket.to(socket.room).emit("peer-updated", peerInfo(socket));
  });

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
