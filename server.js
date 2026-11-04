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
for (const [c, d] of [["title", "TEXT DEFAULT ''"], ["bio", "TEXT DEFAULT ''"], ["role", "TEXT DEFAULT 'user'"], ["disabled", "INTEGER DEFAULT 0"], ["visibility", "TEXT DEFAULT 'public'"], ["show_streak", "INTEGER DEFAULT 1"], ["show_activity", "INTEGER DEFAULT 0"], ["daily_goal", "INTEGER DEFAULT 4"], ["show_leaderboard", "INTEGER DEFAULT 1"],
  ["avatar", "TEXT DEFAULT ''"], ["cover", "TEXT DEFAULT ''"], ["photo_v", "INTEGER DEFAULT 0"], ["pronouns", "TEXT DEFAULT ''"], ["age", "INTEGER DEFAULT 0"], ["country", "TEXT DEFAULT ''"],
  ["relationship", "TEXT DEFAULT ''"], ["looking_for", "TEXT DEFAULT '[]'"], ["interests", "TEXT DEFAULT '[]'"], ["instagram", "TEXT DEFAULT ''"], ["linkedin", "TEXT DEFAULT ''"], ["last_seen", "INTEGER DEFAULT 0"], ["settings", "TEXT DEFAULT '{}'"], ["dm_privacy", "TEXT DEFAULT 'everyone'"]])
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
if (!db.prepare("PRAGMA table_info(rooms)").all().some(x => x.name === "vibe")) db.exec("ALTER TABLE rooms ADD COLUMN vibe TEXT DEFAULT 'pomodoro'");
db.exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)");
db.exec(`CREATE TABLE IF NOT EXISTS dms (id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL, read_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_dms_to ON dms(to_id, read_at);
CREATE INDEX IF NOT EXISTS idx_dms_pair ON dms(from_id, to_id);`);
db.exec(`CREATE TABLE IF NOT EXISTS blocks (blocker_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (blocker_id, blocked_id));
CREATE TABLE IF NOT EXISTS reports (id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT NOT NULL, ref_id INTEGER, reason TEXT, context TEXT, status TEXT DEFAULT 'open', created_at INTEGER NOT NULL);`);
db.exec(`CREATE TABLE IF NOT EXISTS likes (post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (post_id, user_id));
CREATE TABLE IF NOT EXISTS notifs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, type TEXT NOT NULL, actor_id INTEGER NOT NULL, ref_id INTEGER, text TEXT, created_at INTEGER NOT NULL, read_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_notifs_user ON notifs(user_id, read_at);
CREATE TABLE IF NOT EXISTS comments (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);
CREATE TABLE IF NOT EXISTS pins (pinner_id INTEGER NOT NULL, pinned_id INTEGER NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (pinner_id, pinned_id));
CREATE INDEX IF NOT EXISTS idx_pins_pinned ON pins(pinned_id);
CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, text TEXT NOT NULL, due TEXT DEFAULT '', done INTEGER DEFAULT 0, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks(user_id);`);
const findByEmail = db.prepare("SELECT * FROM users WHERE email = ?");
const findById = db.prepare("SELECT id, email, name, title, bio, role, disabled, visibility, show_streak, show_activity, (avatar != '') has_avatar, (cover != '') has_cover, photo_v, pronouns, age, country, relationship, looking_for, interests, instagram, linkedin, last_seen, dm_privacy, show_leaderboard FROM users WHERE id = ?");
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
app.use("/api/me/photo", express.json({ limit: "2mb" }));   // compressed profile / cover pictures
app.use(express.json({ limit: "10kb" }));
// browser security headers on every response.
// FRAME_ANCESTORS (optional) lists sites allowed to show the app in a frame, e.g. a preview panel; default: nobody
const FRAME_ANCESTORS = String(process.env.FRAME_ANCESTORS || "").trim();
app.use((req, res, next) => {
  if (!FRAME_ANCESTORS) res.set("X-Frame-Options", "DENY");
  res.set({
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Permissions-Policy": "camera=(self), microphone=(self), display-capture=(self), geolocation=(), payment=(), usb=()"
  });
  if (req.secure) res.set("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  next();
});
const cspFor = nonce => [
  "default-src 'self'",
  `script-src 'self' 'nonce-${nonce}' https://cdn.jsdelivr.net 'wasm-unsafe-eval'`,
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src 'self' https://fonts.gstatic.com data:",
  "img-src 'self' data: blob:",
  "media-src 'self' data: blob:",
  "connect-src 'self' wss: https://cdn.jsdelivr.net https://storage.googleapis.com",
  "worker-src 'self' blob:",
  "manifest-src 'self'",
  "object-src 'none'", "base-uri 'self'", "form-action 'self'", `frame-ancestors ${FRAME_ANCESTORS || "'none'"}`
].join("; ");
// Find the front-end folder even if the upload put it somewhere unexpected
const staticDir = [path.join(__dirname, "public"), path.join(__dirname, "chaupal-app", "public")]
  .find(d => fs.existsSync(path.join(d, "index.html")));
const indexFile = [path.join(__dirname, "index.html"), staticDir && path.join(staticDir, "index.html")].find(f => f && fs.existsSync(f));
let indexCache = { at: 0, html: "" };
function sendIndex(_req, res){
  const st = fs.statSync(indexFile);
  if (st.mtimeMs !== indexCache.at) indexCache = { at: st.mtimeMs, html: fs.readFileSync(indexFile, "utf8") };
  const nonce = crypto.randomBytes(16).toString("base64");
  res.set({ "Content-Security-Policy": cspFor(nonce), "Cache-Control": "no-cache" }).type("html")
    .send(indexCache.html.replace(/<script(?=[\s>])/g, `<script nonce="${nonce}"`));
}
if (indexFile){ app.get("/", sendIndex); app.get("/index.html", sendIndex); }
if (staticDir) app.use(express.static(staticDir, { index: false }));
app.get("/sw.js", (_req, res) => res.type("application/javascript").set("Cache-Control", "no-cache").sendFile(path.join(__dirname, "sw.js")));
app.get("/", (_req, res) => {
  const files = fs.readdirSync(__dirname).filter(f => f !== "node_modules" && !f.startsWith(".env")).join(", ");
  res.status(500).type("text").send(
    "Chaupal server is running, but index.html was not found.\n" +
    "Create the file public/index.html in your GitHub repo.\n\n" +
    "Files next to server.js: " + files
  );
});

const sign = u => jwt.sign({ id: u.id, name: u.name }, JWT_SECRET, { expiresIn: "7d" });
const jsonArr = s => { try { const a = JSON.parse(s || "[]"); return Array.isArray(a) ? a : []; } catch { return []; } };
// photos of public users have plain links; photos of private / followers-only users get a signed link only for allowed viewers
const imgSig = (owner, kind, viewer, exp) => crypto.createHmac("sha256", JWT_SECRET).update(`img:${owner}:${kind}:${viewer}:${exp}`).digest("base64url").slice(0, 32);
const mayViewPhoto = (viewer, ownerId, vis) => viewer.id === ownerId || viewer.role === "admin" || (!blockedEither(viewer.id, ownerId) && canView(viewer, { id: ownerId, visibility: vis }));
function photoUrl(u, kind, viewer){
  if (!(kind === "avatar" ? (u.has_avatar ?? !!u.avatar) : (u.has_cover ?? !!u.cover))) return "";
  const base = `/api/img/${u.id}/${kind}?v=${u.photo_v || 0}`;
  const vis = u.visibility ?? db.prepare("SELECT visibility FROM users WHERE id = ?").get(u.id)?.visibility;
  if (!vis || vis === "public") return base;
  if (!viewer || !mayViewPhoto(viewer, u.id, vis)) return "";
  const exp = (Math.floor(Date.now() / 864e5) + 2) * 864e5;   // good until the end of tomorrow (UTC) so browsers can cache it
  return `${base}&u=${viewer.id}&e=${exp}&s=${imgSig(u.id, kind, viewer.id, exp)}`;
}
const profileFields = (u, viewer = u) => ({ avatar_url: photoUrl(u, "avatar", viewer), cover_url: photoUrl(u, "cover", viewer), pronouns: u.pronouns || "", age: u.age || 0, country: u.country || "", relationship: u.relationship || "",
  looking_for: jsonArr(u.looking_for), interests: jsonArr(u.interests), instagram: u.instagram || "", linkedin: u.linkedin || "" });
const publicUser = u => ({ id: u.id, name: u.name, email: u.email, title: u.title || "", bio: u.bio || "", role: u.role || "user", visibility: u.visibility || "public", show_streak: u.show_streak ?? 1, show_activity: u.show_activity ?? 0, dm_privacy: u.dm_privacy || "everyone", show_leaderboard: u.show_leaderboard ?? 1, ...profileFields(u) });

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
const RELS = ["Single", "In a relationship", "Engaged", "Married", "It's complicated", "Prefer not to say"];
const LOOKING = ["Accountability partner(s)", "Study buddy", "A focus group/community", "Professional networking", "Friends", "Mentorship"];
// accepts a full https link or a plain handle; anything else (e.g. javascript:) is dropped
const cleanSocial = v => { const s = String(v || "").trim().slice(0, 120); if (/^https:\/\/[^\s"<>]+$/i.test(s)) return s; const h = s.replace(/^@/, ""); return /^[\w.\-]{1,60}$/.test(h) ? h : ""; };
app.put("/api/me", auth, (req, res) => {
  const b = req.body || {};
  const { name, title, bio, visibility, show_streak, show_activity } = b;
  const n = String(name || "").trim().slice(0, 40) || req.user.name;
  db.prepare("UPDATE users SET name = ?, title = ?, bio = ? WHERE id = ?")
    .run(n, TITLES.includes(title) ? title : "", String(bio || "").trim().slice(0, 600), req.user.id);
  if (visibility !== undefined) db.prepare("UPDATE users SET visibility = ?, show_streak = ?, show_activity = ? WHERE id = ?")
    .run(["public", "followers", "private"].includes(visibility) ? visibility : "public", show_streak ? 1 : 0, show_activity ? 1 : 0, req.user.id);
  const age = Math.round(Number(b.age) || 0);
  const interests = [...new Set((Array.isArray(b.interests) ? b.interests : []).map(x => String(x).trim().slice(0, 24)).filter(Boolean))].slice(0, 15);
  db.prepare("UPDATE users SET pronouns = ?, age = ?, country = ?, relationship = ?, looking_for = ?, interests = ?, instagram = ?, linkedin = ? WHERE id = ?").run(
    String(b.pronouns || "").trim().slice(0, 20), age >= 13 && age <= 120 ? age : 0, /^[A-Z]{2}$/.test(String(b.country || "")) ? b.country : "",
    RELS.includes(b.relationship) ? b.relationship : "", JSON.stringify((Array.isArray(b.looking_for) ? b.looking_for : []).filter(x => LOOKING.includes(x))),
    JSON.stringify(interests), cleanSocial(b.instagram), cleanSocial(b.linkedin), req.user.id);
  res.json({ user: publicUser(findById.get(req.user.id)) });
});
app.put("/api/privacy", auth, (req, res) => {
  const b = req.body || {};
  if ((b.visibility || "public") !== req.user.visibility) db.prepare("UPDATE users SET photo_v = ? WHERE id = ?").run(Date.now(), req.user.id);   // new photo links, so old cached ones stop matching
  db.prepare("UPDATE users SET visibility = ?, show_streak = ?, show_activity = ?, show_leaderboard = ?, dm_privacy = ? WHERE id = ?").run(
    ["public", "followers", "private"].includes(b.visibility) ? b.visibility : "public", b.show_streak ? 1 : 0, b.show_activity ? 1 : 0, b.show_leaderboard ? 1 : 0,
    ["everyone", "following", "nobody"].includes(b.dm_privacy) ? b.dm_privacy : "everyone", req.user.id);
  res.json({ user: publicUser(findById.get(req.user.id)) });
});

/* ---------- app settings (video, pins, troubleshooting, pomodoro) ---------- */
const SET_DEF = { pomo_focus: 25, pomo_short: 5, pomo_long: 15, pomo_rounds: 4, pomo_sound: true, pomo_auto: false,
  tiles: 4, mirror_self: true, mirror_others: false, self_view: true,
  hide_pin_notifs: false, auto_pinback: false, hide_pin_counts: false, hide_my_pins: false, sort_pinned: true,
  disable_messages: false, disable_pinning: false, low_res: false };
const SET_NUM = { pomo_focus: [1, 120], pomo_short: [1, 60], pomo_long: [1, 90], pomo_rounds: [1, 12], tiles: [4, 12] };
function userSettings(uid){
  let s = {}; try { s = JSON.parse(db.prepare("SELECT settings s FROM users WHERE id = ?").get(uid)?.s || "{}"); } catch {}
  return { ...SET_DEF, ...s };
}
function cleanSettings(b){
  const out = {};
  for (const [k, d] of Object.entries(SET_DEF)){
    if (b[k] === undefined) continue;
    if (typeof d === "boolean") out[k] = !!b[k];
    else { const [lo, hi] = SET_NUM[k], n = Math.round(Number(b[k])); if (Number.isFinite(n)) out[k] = Math.min(hi, Math.max(lo, n)); }
  }
  if (out.tiles !== undefined && ![4, 8, 12].includes(out.tiles)) out.tiles = 4;
  return out;
}
app.get("/api/settings", auth, (req, res) => res.json({ settings: userSettings(req.user.id) }));
app.put("/api/settings", auth, (req, res) => {
  const s = { ...userSettings(req.user.id), ...cleanSettings(req.body || {}) };
  db.prepare("UPDATE users SET settings = ? WHERE id = ?").run(JSON.stringify(s), req.user.id);
  for (const k of io.sockets.sockets.values()) if (k.user.id === req.user.id){ k.settings = s; if (k.room) k.to(k.room).emit("peer-updated", peerInfo(k)); }
  res.json({ settings: s });
});

/* ---------- tasks ---------- */
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;
const taskOut = t => ({ id: t.id, text: t.text, due: t.due || "", done: !!t.done, created_at: t.created_at });
app.get("/api/tasks", auth, (req, res) => res.json({ tasks: db.prepare("SELECT * FROM tasks WHERE user_id = ? ORDER BY done, CASE WHEN due = '' THEN 1 ELSE 0 END, due, id DESC LIMIT 300").all(req.user.id).map(taskOut) }));
app.post("/api/tasks", auth, (req, res) => {
  const b = req.body || {}, text = String(b.text || "").trim().slice(0, 200);
  if (!text) return res.status(400).json({ error: "Write a task first." });
  if (db.prepare("SELECT COUNT(*) c FROM tasks WHERE user_id = ?").get(req.user.id).c >= 300) return res.status(429).json({ error: "You have 300 tasks. Delete some first." });
  const id = db.prepare("INSERT INTO tasks (user_id, text, due, created_at) VALUES (?, ?, ?, ?)").run(req.user.id, text, DAY_RE.test(b.due || "") ? b.due : "", Date.now()).lastInsertRowid;
  res.json({ task: taskOut(db.prepare("SELECT * FROM tasks WHERE id = ?").get(id)) });
});
app.patch("/api/tasks/:id", auth, (req, res) => {
  const t = db.prepare("SELECT * FROM tasks WHERE id = ? AND user_id = ?").get(Number(req.params.id), req.user.id);
  if (!t) return res.status(404).json({ error: "Task not found." });
  const b = req.body || {};
  const text = b.text === undefined ? t.text : String(b.text).trim().slice(0, 200) || t.text;
  const due = b.due === undefined ? t.due : DAY_RE.test(b.due || "") ? b.due : "";
  const done = b.done === undefined ? t.done : b.done ? 1 : 0;
  db.prepare("UPDATE tasks SET text = ?, due = ?, done = ? WHERE id = ?").run(text, due, done, t.id);
  if (done && !t.done) logEvent(req.user.id, "task_done", { text: text.slice(0, 60) });
  res.json({ task: taskOut(db.prepare("SELECT * FROM tasks WHERE id = ?").get(t.id)) });
});
app.delete("/api/tasks/:id", auth, (req, res) => { db.prepare("DELETE FROM tasks WHERE id = ? AND user_id = ?").run(Number(req.params.id), req.user.id); res.json({ ok: true }); });

// a focus session finished outside a room (floating timer)
const pomoHits = new Map();
app.post("/api/pomo-done", auth, (req, res) => {
  const last = pomoHits.get(req.user.id) || 0;
  if (Date.now() - last < 60000) return res.json({ ok: false });
  pomoHits.set(req.user.id, Date.now()); logEvent(req.user.id, "pomodoro", {}); res.json({ ok: true });
});
const pinCount = uid => db.prepare("SELECT COUNT(*) c FROM pins WHERE pinned_id = ?").get(uid).c;

app.get("/api/profile-options", auth, (_req, res) => res.json({ relationships: RELS, looking: LOOKING }));

/* ---------- profile & cover pictures (stored in the database, so they survive Render restarts via the Postgres snapshot) ---------- */
const IMG_RE = /^data:image\/(jpeg|png|webp);base64,([A-Za-z0-9+/=]+)$/;
const IMG_MAX = { avatar: 300 * 1024, cover: 700 * 1024 };
app.post("/api/me/photo", auth, (req, res) => {
  const { kind, data } = req.body || {};
  if (!["avatar", "cover"].includes(kind)) return res.status(400).json({ error: "Unknown picture type." });
  const d = String(data || "");
  if (d && !IMG_RE.test(d)) return res.status(400).json({ error: "Use a JPG, PNG or WebP picture." });
  if (d.length * 0.75 > IMG_MAX[kind]) return res.status(413).json({ error: "That picture is too large. Try a smaller one." });
  db.prepare(`UPDATE users SET ${kind} = ?, photo_v = ? WHERE id = ?`).run(d, Date.now(), req.user.id);
  const u = findById.get(req.user.id);
  emitAll("profile-photo", { uid: u.id });
  res.json({ user: publicUser(u) });
});
app.get("/api/img/:id/:kind", (req, res) => {
  if (!["avatar", "cover"].includes(req.params.kind)) return res.status(404).end();
  const id = Number(req.params.id), kind = req.params.kind;
  const r = db.prepare(`SELECT ${kind} d, visibility FROM users WHERE id = ? AND disabled = 0`).get(id);
  const m = r && IMG_RE.exec(r.d || "");
  if (!m) return res.status(404).end();
  if (r.visibility !== "public"){
    const vid = Number(req.query.u), exp = Number(req.query.e), sig = String(req.query.s || "");
    const good = vid > 0 && exp > Date.now() && /^[A-Za-z0-9_-]{32}$/.test(sig) && crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(imgSig(id, kind, vid, exp)));
    const viewer = good && findById.get(vid);
    if (!viewer || viewer.disabled || !mayViewPhoto(viewer, id, r.visibility)) return res.status(403).end();
    res.set("Cache-Control", "private, max-age=3600");
  } else res.set("Cache-Control", "public, max-age=86400");
  res.type("image/" + m[1]).send(Buffer.from(m[2], "base64"));
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
const POSTQ = "SELECT p.id, p.body, p.created_at, u.id uid, u.name, u.title, u.visibility, (u.avatar != '') has_avatar, u.photo_v FROM posts p JOIN users u ON u.id = p.user_id WHERE u.disabled = 0 AND ";

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
  const base = { id: o.id, name: o.name, title: o.title, visibility: o.visibility, followers: cnt("followee_id"), following: cnt("follower_id"), isFollowing: follows(req.user.id, o.id), followsMe: follows(o.id, req.user.id), isMe: o.id === req.user.id, iBlocked: !!db.prepare("SELECT 1 FROM blocks WHERE blocker_id = ? AND blocked_id = ?").get(req.user.id, o.id) };
  if (blockedEither(req.user.id, o.id) && req.user.role !== "admin") return res.json({ ...base, restricted: true, blocked: true });
  if (!canView(req.user, o)) return res.json({ ...base, restricted: true });
  Object.assign(base, profileFields(o, req.user), { online: online.has(o.id), last_seen: online.has(o.id) ? Date.now() : o.last_seen || 0 });
  const self = o.id === req.user.id || req.user.role === "admin";
  const showAct = self || o.show_activity;
  const week = Date.now() - 7 * 864e5;
  const left = showAct ? db.prepare("SELECT data, created_at FROM events WHERE user_id = ? AND type = 'left_room' ORDER BY id DESC LIMIT 60").all(o.id).map(e => ({ ...JSON.parse(e.data || "{}"), at: e.created_at })) : null;
  const sessions = left && { recent: left.slice(0, 8), weekRoom: left.filter(e => e.at >= week).reduce((a, e) => a + (e.ms || 0), 0), weekCam: left.filter(e => e.at >= week).reduce((a, e) => a + (e.cam || 0), 0) };
  const partners = db.prepare("SELECT u.id, u.name, u.title, u.visibility, (u.avatar != '') has_avatar, u.photo_v FROM follows a JOIN follows b ON b.follower_id = a.followee_id AND b.followee_id = a.follower_id JOIN users u ON u.id = a.followee_id WHERE a.follower_id = ? AND u.disabled = 0 LIMIT 30").all(o.id)
    .map(p => ({ id: p.id, name: p.name, title: p.title || "", avatar_url: photoUrl(p, "avatar", req.user), online: online.has(p.id) }));
  res.json({ ...base, bio: o.bio, streak: self || o.show_streak ? streakOf(o.id) : null,
    posts: withLikes(db.prepare(POSTQ + "p.user_id = ? ORDER BY p.id DESC LIMIT 20").all(o.id), req.user),
    activity: showAct ? eventsOf(o.id, 20) : null, sessions, partners,
    stats: (() => { const ec = t => db.prepare("SELECT COUNT(*) c FROM events WHERE user_id = ? AND type = ?").get(o.id, t).c; return { pins: userSettings(o.id).hide_my_pins && !self ? null : pinCount(o.id), focus: ec("pomodoro"), sent: ec("encouraged"), received: ec("encouraged_by"), posts: db.prepare("SELECT COUNT(*) c FROM posts WHERE user_id = ?").get(o.id).c }; })() });
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
    followChanged(req.user.id, o.id);
  }
  res.json({ ok: true });
});
app.delete("/api/users/:id/follow", auth, (req, res) => {
  if (db.prepare("DELETE FROM follows WHERE follower_id = ? AND followee_id = ?").run(req.user.id, Number(req.params.id)).changes) followChanged(req.user.id, Number(req.params.id));
  res.json({ ok: true });
});
// remove someone who follows me
app.delete("/api/followers/:id", auth, (req, res) => {
  if (db.prepare("DELETE FROM follows WHERE follower_id = ? AND followee_id = ?").run(Number(req.params.id), req.user.id).changes) followChanged(Number(req.params.id), req.user.id);
  res.json({ ok: true });
});
app.get("/api/users/:id/follows", auth, (req, res) => {
  const o = findById.get(Number(req.params.id));
  if (!o || o.disabled) return res.status(404).json({ error: "User not found." });
  if (!canView(req.user, o)) return res.status(403).json({ error: "This list is hidden." });
  const followers = req.query.type !== "following";
  const rows = db.prepare(`SELECT u.id, u.name, u.title, u.visibility, (u.avatar != '') has_avatar, u.photo_v FROM follows f JOIN users u ON u.id = ${followers ? "f.follower_id" : "f.followee_id"} WHERE ${followers ? "f.followee_id" : "f.follower_id"} = ? AND u.disabled = 0 ORDER BY f.created_at DESC LIMIT 200`).all(o.id);
  res.json({ users: rows.filter(r => !blockedEither(req.user.id, r.id)).map(r => ({ id: r.id, name: r.name, title: r.title || "", avatar_url: photoUrl(r, "avatar", req.user), online: online.has(r.id), isFollowing: follows(req.user.id, r.id), isMe: r.id === req.user.id })) });
});

app.post("/api/posts", auth, (req, res) => {
  const body = String((req.body || {}).body || "").trim().slice(0, 1000);
  if (!body) return res.status(400).json({ error: "Write something first." });
  if (db.prepare("SELECT COUNT(*) c FROM posts WHERE user_id = ? AND created_at >= ?").get(req.user.id, dayStart()).c >= 20)
    return res.status(429).json({ error: "You've reached today's limit of 20 posts." });
  const id = db.prepare("INSERT INTO posts (user_id, body, created_at) VALUES (?, ?, ?)").run(req.user.id, body, Date.now()).lastInsertRowid;
  logEvent(req.user.id, "posted", { post: Number(id) });
  emitNewPost(req.user.id, Number(id));
  res.json({ ok: true });
});
app.get("/api/feed", auth, (req, res) => {
  const me = req.user.id;
  const rows = db.prepare(POSTQ + "(p.user_id = ? OR p.user_id IN (SELECT followee_id FROM follows WHERE follower_id = ?)) ORDER BY p.id DESC LIMIT 50").all(me, me);
  res.json({ posts: withLikes(rows.filter(r => (r.uid === me || r.visibility !== "private") && !blockedEither(me, r.uid)), req.user) });
});
app.delete("/api/posts/:id", auth, (req, res) => {
  const p = db.prepare("SELECT user_id FROM posts WHERE id = ?").get(Number(req.params.id));
  if (!p || (p.user_id !== req.user.id && req.user.role !== "admin")) return res.status(404).json({ error: "Post not found." });
  db.prepare("DELETE FROM posts WHERE id = ?").run(Number(req.params.id));
  db.prepare("DELETE FROM comments WHERE post_id = ?").run(Number(req.params.id));
  db.prepare("DELETE FROM likes WHERE post_id = ?").run(Number(req.params.id));
  emitAll("post-del", { id: Number(req.params.id) });
  res.json({ ok: true });
});

/* ---------- comments ---------- */
const commentCount = pid => db.prepare("SELECT COUNT(*) c FROM comments WHERE post_id = ?").get(pid).c;
const postFor = (viewer, pid) => {
  const p = db.prepare("SELECT p.id, p.user_id, p.body FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ? AND u.disabled = 0").get(pid);
  return p && !blockedEither(viewer.id, p.user_id) && canView(viewer, findById.get(p.user_id)) ? p : null;
};
app.get("/api/posts/:id/comments", auth, (req, res) => {
  const p = postFor(req.user, Number(req.params.id));
  if (!p) return res.status(404).json({ error: "Post not found." });
  const rows = db.prepare("SELECT c.id, c.body, c.created_at, u.id uid, u.name, u.visibility, (u.avatar != '') has_avatar, u.photo_v FROM comments c JOIN users u ON u.id = c.user_id WHERE c.post_id = ? AND u.disabled = 0 ORDER BY c.id LIMIT 200").all(p.id);
  res.json({ comments: rows.filter(r => !blockedEither(req.user.id, r.uid)).map(r => ({ id: r.id, body: r.body, created_at: r.created_at, uid: r.uid, name: r.name, av: photoUrl({ id: r.uid, has_avatar: r.has_avatar, photo_v: r.photo_v, visibility: r.visibility }, "avatar", req.user), canDelete: r.uid === req.user.id || p.user_id === req.user.id || req.user.role === "admin" })) });
});
app.post("/api/posts/:id/comments", auth, (req, res) => {
  const p = postFor(req.user, Number(req.params.id));
  if (!p) return res.status(404).json({ error: "Post not found." });
  const body = String((req.body || {}).body || "").trim().slice(0, 280);
  if (!body) return res.status(400).json({ error: "Write a comment first." });
  db.prepare("INSERT INTO comments (post_id, user_id, body, created_at) VALUES (?, ?, ?, ?)").run(p.id, req.user.id, body, Date.now());
  notify(p.user_id, "comment", req.user, p.id, body.slice(0, 60), req.user.name + " commented on your post");
  emitPostUpdate(p.id, p.user_id);
  res.json({ ok: true, comments: commentCount(p.id) });
});
app.delete("/api/comments/:id", auth, (req, res) => {
  const c = db.prepare("SELECT c.id, c.post_id, c.user_id, p.user_id owner FROM comments c JOIN posts p ON p.id = c.post_id WHERE c.id = ?").get(Number(req.params.id));
  if (!c || (c.user_id !== req.user.id && c.owner !== req.user.id && req.user.role !== "admin")) return res.status(404).json({ error: "Comment not found." });
  db.prepare("DELETE FROM comments WHERE id = ?").run(c.id);
  emitPostUpdate(c.post_id, c.owner);
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
  const repliedBefore = !!db.prepare("SELECT 1 FROM dms WHERE from_id = ? AND to_id = ?").get(o.id, me);
  if (!repliedBefore && (o.visibility === "private" || o.dm_privacy === "nobody" || (o.dm_privacy === "following" && !follows(o.id, me))))
    return res.status(403).json({ error: "This user isn't accepting messages from you." });
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
const withLikes = (rows, viewer) => rows.map(r => ({ ...r, av: photoUrl({ id: r.uid, has_avatar: r.has_avatar, photo_v: r.photo_v, visibility: r.visibility }, "avatar", viewer), likes: likeCount(r.id), comments: commentCount(r.id), liked: !!db.prepare("SELECT 1 FROM likes WHERE post_id = ? AND user_id = ?").get(r.id, viewer.id) }));
const notifUnread = uid => db.prepare("SELECT COUNT(*) c FROM notifs WHERE user_id = ? AND read_at IS NULL").get(uid).c;
function notify(uid, type, actor, ref, text, toastText){
  if (uid === actor.id || blockedEither(uid, actor.id)) return;
  if (db.prepare("SELECT 1 FROM notifs WHERE user_id = ? AND type = ? AND actor_id = ? AND IFNULL(ref_id, 0) = IFNULL(?, 0) AND read_at IS NULL").get(uid, type, actor.id, ref ?? null)) return;   // no repeats while the first is unread
  db.prepare("INSERT INTO notifs (user_id, type, actor_id, ref_id, text, created_at) VALUES (?, ?, ?, ?, ?, ?)").run(uid, type, actor.id, ref ?? null, text || null, Date.now());
  for (const s of io.sockets.sockets.values()) if (s.user.id === uid && s.handshake.query.dm === "1") s.emit("notif", { type, text: toastText });
}
app.post("/api/posts/:id/like", auth, (req, res) => {
  const p = postFor(req.user, Number(req.params.id));
  if (!p) return res.status(404).json({ error: "Post not found." });
  if (db.prepare("INSERT OR IGNORE INTO likes VALUES (?, ?, ?)").run(p.id, req.user.id, Date.now()).changes){
    notify(p.user_id, "like", req.user, p.id, p.body.slice(0, 60), req.user.name + " liked your post");
    emitPostUpdate(p.id, p.user_id);
  }
  res.json({ liked: true, likes: likeCount(p.id) });
});
app.delete("/api/posts/:id/like", auth, (req, res) => {
  const pid = Number(req.params.id);
  if (db.prepare("DELETE FROM likes WHERE post_id = ? AND user_id = ?").run(pid, req.user.id).changes){
    const p = db.prepare("SELECT user_id FROM posts WHERE id = ?").get(pid);
    if (p) emitPostUpdate(pid, p.user_id);
  }
  res.json({ liked: false, likes: likeCount(pid) });
});

/* ---------- real-time: posts, likes, comments, follows, presence (sent over the app-wide "dm" socket) ---------- */
const online = new Map();   // userId -> number of open tabs
const appSockets = () => [...io.sockets.sockets.values()].filter(s => s.handshake.query.dm === "1");
function emitAll(ev, data, ownerId){ for (const s of appSockets()) if (!ownerId || s.user.id === ownerId || !blockedEither(s.user.id, ownerId)) s.emit(ev, data); }
function emitNewPost(owner, id){
  const o = findById.get(owner);
  for (const s of appSockets()){
    if (s.user.id !== owner && (blockedEither(s.user.id, owner) || !canView(s.user, o))) continue;
    s.emit("post-new", { id, uid: owner, inFeed: s.user.id === owner || follows(s.user.id, owner) });
  }
}
const emitPostUpdate = (id, owner) => emitAll("post-update", { id, likes: likeCount(id), comments: commentCount(id) }, owner);
function followChanged(a, b){
  const c = (col, id) => db.prepare(`SELECT COUNT(*) c FROM follows WHERE ${col} = ?`).get(id).c;
  for (const uid of [a, b]) emitAll("follow-update", { uid, followers: c("followee_id", uid), following: c("follower_id", uid) });
}
const touchSeen = uid => db.prepare("UPDATE users SET last_seen = ? WHERE id = ?").run(Date.now(), uid);
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
/* room vibes: each type runs its own always-on shared timer, synced to the wall clock so every room of that vibe is in step */
const VIBES = {
  pomodoro: { label: "Pomodoro", focus: 25, short: 5, long: 15, rounds: 4, blurb: "Classic 25 / 5 rounds with a long break every 4." },
  deep: { label: "Deep focus", focus: 50, short: 10, long: 30, rounds: 3, blurb: "Long 50-minute blocks for hard problems." },
  chill: { label: "Chill", focus: 30, short: 10, long: 20, rounds: 4, blurb: "Relaxed pace, easy breaks, cameras optional." },
  cram: { label: "Cram", focus: 90, short: 15, long: 30, rounds: 2, blurb: "90-minute sprints before the exam." }
};
const vibeMs = v => ({ focus: VIBES[v].focus * 60000 * GTS, short: VIBES[v].short * 60000 * GTS, long: VIBES[v].long * 60000 * GTS });
function vibeState(v, t = Date.now()){
  const V = VIBES[v], ms = vibeMs(v), seq = [];
  for (let i = 0; i < V.rounds; i++) seq.push("focus", i === V.rounds - 1 ? "long" : "short");
  const cycle = seq.reduce((a, p) => a + ms[p], 0);
  let pos = t % cycle;
  for (let i = 0; i < seq.length; i++){
    if (pos < ms[seq[i]]) return { phase: seq[i], left: ms[seq[i]] - pos, done: Math.ceil(i / 2) };
    pos -= ms[seq[i]];
  }
  return { phase: "focus", left: ms.focus, done: 0 };
}
const roomVibe = r => VIBES[r.vibe] ? r.vibe : "pomodoro";
const roomOut = r => { const v = roomVibe(r), st = vibeState(v); return { id: r.id, code: r.code, name: r.name, description: r.description, video: !!r.video_allowed, audio: !!r.audio_allowed, active: !!r.active, live: io.sockets.adapter.rooms.get(r.code)?.size || 0,
  vibe: v, vibeLabel: VIBES[v].label, vibeBlurb: VIBES[v].blurb, plan: { focus: VIBES[v].focus, short: VIBES[v].short, long: VIBES[v].long, rounds: VIBES[v].rounds }, timer: { phase: st.phase, left: st.left } }; };
// one starter room per vibe, created once
if (!db.prepare("SELECT 1 FROM meta WHERE k = 'vibes_seeded'").get()){
  const names = { pomodoro: ["Pomodoro Room", "25 minutes on, 5 off. Study together on one clock."], deep: ["Deep Focus Room", "Quiet 50-minute blocks. Mics off, heads down."], chill: ["Chill Study Lounge", "Easy pace and friendly faces."], cram: ["Cram Room", "Exam tomorrow? 90-minute sprints."] };
  for (const [v, [n, d]] of Object.entries(names))
    if (!db.prepare("SELECT 1 FROM rooms WHERE vibe = ? AND name = ?").get(v, n))
      db.prepare("INSERT INTO rooms (code, name, description, video_allowed, audio_allowed, created_at, vibe) VALUES (?, ?, ?, 1, ?, ?, ?)").run(genRoomCode(), n, d, v === "deep" ? 0 : 1, Date.now(), v);
  db.prepare("INSERT INTO meta (k, v) VALUES ('vibes_seeded', '1')").run();
}
app.get("/api/rooms", auth, (_req, res) => res.json({ rooms: db.prepare("SELECT * FROM rooms WHERE active = 1 ORDER BY id DESC").all().map(roomOut) }));
app.get("/api/admin/rooms", auth, admin, (_req, res) => res.json({ rooms: db.prepare("SELECT * FROM rooms ORDER BY id DESC").all().map(roomOut) }));
app.post("/api/admin/rooms", auth, admin, (req, res) => {
  const b = req.body || {}, name = String(b.name || "").trim().slice(0, 60);
  if (!name) return res.status(400).json({ error: "Give the room a name." });
  const code = genRoomCode();
  db.prepare("INSERT INTO rooms (code, name, description, video_allowed, audio_allowed, created_by, created_at, vibe) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    .run(code, name, String(b.description || "").trim().slice(0, 140), b.video === false ? 0 : 1, b.audio === false ? 0 : 1, req.user.id, Date.now(), VIBES[b.vibe] ? b.vibe : "pomodoro");
  res.json({ room: roomOut(db.prepare("SELECT * FROM rooms WHERE code = ?").get(code)) });
});
app.patch("/api/admin/rooms/:id", auth, admin, (req, res) => {
  const r = db.prepare("SELECT * FROM rooms WHERE id = ?").get(Number(req.params.id));
  if (!r) return res.status(404).json({ error: "Room not found." });
  const b = req.body || {};
  const v = b.video === undefined ? r.video_allowed : b.video ? 1 : 0, a = b.audio === undefined ? r.audio_allowed : b.audio ? 1 : 0, act = b.active === undefined ? r.active : b.active ? 1 : 0;
  db.prepare("UPDATE rooms SET name = ?, description = ?, video_allowed = ?, audio_allowed = ?, active = ?, vibe = ? WHERE id = ?")
    .run(String(b.name ?? r.name).trim().slice(0, 60) || r.name, String(b.description ?? r.description).slice(0, 140), v, a, act, VIBES[b.vibe] ? b.vibe : roomVibe(r), r.id);
  const nr = db.prepare("SELECT * FROM rooms WHERE id = ?").get(r.id);
  if (roomVibe(nr) !== roomVibe(r) && GT.has(r.code)){ clearTimeout(GT.get(r.code).timer); GT.delete(r.code); startVibe(r.code, roomVibe(nr)); io.in(r.code).emit("rt-state", gtOut(r.code)); }
  io.in(r.code).emit("room-settings", { name: nr.name, video: !!nr.video_allowed, audio: !!nr.audio_allowed, vibe: roomVibe(nr), vibeLabel: VIBES[roomVibe(nr)].label });
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
const turnReady = () => turnConfig().ready || meteredOn();
const cleanEnv = v => String(v || "").trim().replace(/^["']+|["']+$/g, "").trim();
const meteredOn = () => !!(cleanEnv(process.env.METERED_API_KEY) && cleanEnv(process.env.METERED_DOMAIN));
let meteredCache = { at: 0, list: null }, meteredError = "";
// Metered.ca: fetches fresh TURN credentials with your API key (cached 30 min)
async function meteredServers(){
  if (!meteredOn()) return null;
  if (meteredCache.list && Date.now() - meteredCache.at < 30 * 60000) return meteredCache.list;
  const dom = cleanEnv(process.env.METERED_DOMAIN).replace(/^https?:\/\//i, "").replace(/\/.*$/, "");
  try {
    const r = await fetch(`https://${dom}/api/v1/turn/credentials?apiKey=${encodeURIComponent(cleanEnv(process.env.METERED_API_KEY))}`, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const list = await r.json();
    if (!Array.isArray(list) || !list.length) throw new Error("Metered returned no servers: " + JSON.stringify(list).slice(0, 120));
    meteredCache = { at: Date.now(), list }; meteredError = "";
  } catch (e) { meteredError = e.message; console.error("Metered TURN fetch failed:", e.message, "- check METERED_DOMAIN (e.g. yourapp.metered.live) and METERED_API_KEY"); }
  return meteredCache.list;
}
if (process.env.TURN_URL || process.env.TURN_USERNAME || process.env.TURN_CREDENTIAL){
  const c = turnConfig();
  console.log(c.ready ? "TURN relay is ON (" + c.urls.length + " addresses)." : "TURN relay is OFF. Missing: " + c.missing.join(", "));
}
if (meteredOn()) console.log("Metered TURN relay is ON (API key mode).");
app.get("/api/ice", auth, wrap(async (req, res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:global.stun.twilio.com:3478" }
  ];
  const c = turnConfig(), m = await meteredServers();
  if (m) iceServers.push(...m.filter(s => !/^stuns?:/.test([].concat(s.urls)[0] || "")));
  if (c.ready) iceServers.push({ urls: c.urls, username: c.user, credential: c.cred });
  const source = m ? "metered" : c.ready ? "static" : "none";
  res.json({ iceServers, relay: !!m || c.ready, source, missing: m || c.ready ? [] : c.missing,
    ...(req.user.role === "admin" ? { meteredOn: meteredOn(), meteredError: meteredOn() ? meteredError : "" } : {}) });
}));

app.get("/healthz", (_req, res) => {
  try { db.prepare("SELECT 1").get(); res.json({ ok: true, db: "ok", permanent: !!pool, turn: turnReady(), turnMissing: turnConfig().missing }); }
  catch (e) { res.status(500).json({ ok: false, db: String(e.message) }); }
});

// any unhandled route error (including bad JSON bodies) -> JSON, never an HTML page
app.use((err, _req, res, _next) => {
  console.error("Request error:", err);
  if (err.type === "entity.parse.failed") return res.status(400).json({ error: "Bad request." });
  if (err.type === "entity.too.large") return res.status(413).json({ error: "That is too large. Try a smaller picture or shorter text." });
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

const sockSettings = s => s.settings || (s.settings = userSettings(s.user.id));
const peerInfo = s => ({
  id: s.id, userId: s.user.id, pins: sockSettings(s).hide_my_pins ? null : pinCount(s.user.id), noPin: sockSettings(s).disable_pinning, name: s.user.name, title: s.user.title, bio: s.user.bio, goal: s.goal || "", sessions: s.sessions || 0, cheers: s.cheers || 0,
  pomo: s.pomo ? { ...s.pomo, left: s.pomo.running ? Math.max(0, s.pomo.left - (Date.now() - s.pomo.at)) : s.pomo.left } : null
});
/* group timer: one shared Pomodoro clock per room, kept by the server so everyone stays in sync */
const GTS = Number(process.env.GT_SCALE || 1), GT_MS = { focus: 25 * 60000 * GTS, short: 5 * 60000 * GTS, long: 15 * 60000 * GTS };
const GT = new Map();
const gtLeft = g => g.running ? Math.max(0, g.left - (Date.now() - g.at)) : g.left;
const gtOut = room => { const g = GT.get(room); return g ? { host: g.host, hostName: g.hostName, phase: g.phase, running: g.running, left: gtLeft(g), done: g.done, ...(g.vibe ? { vibe: g.vibe, label: VIBES[g.vibe].label, ms: vibeMs(g.vibe), rounds: VIBES[g.vibe].rounds } : {}) } : null; };
function startVibe(room, v){
  const st = vibeState(v);
  GT.set(room, { vibe: v, host: null, hostName: VIBES[v].label, phase: st.phase, running: true, left: st.left, at: Date.now(), done: st.done, timer: null });
  gtArm(room);
}
function gtArm(room){ const g = GT.get(room); if (!g) return; clearTimeout(g.timer); if (g.running) g.timer = setTimeout(() => gtEnd(room), gtLeft(g)); }
function gtEnd(room){
  const g = GT.get(room); if (!g) return; const was = g.phase;
  if (g.vibe){
    const st = vibeState(g.vibe, Date.now() + 500);
    Object.assign(g, { phase: st.phase, left: st.left - 500, done: st.done, at: Date.now(), running: true }); gtArm(room);
    return io.in(room).emit("rt-state", { ...gtOut(room), completed: was });
  }
  if (was === "focus"){ g.done++; g.phase = g.done % 4 === 0 ? "long" : "short"; } else g.phase = "focus";
  g.left = GT_MS[g.phase]; g.at = Date.now(); g.running = true; gtArm(room);
  io.in(room).emit("rt-state", { ...gtOut(room), completed: was });
}
const ROOM_RE = /^[a-z]{3}-[a-z]{4}-[a-z]{3}$/;

io.on("connection", socket => {
  if (socket.handshake.query.dm === "1"){   // app-wide connection: track who is online
    const uid = socket.user.id, n = (online.get(uid) || 0) + 1;
    online.set(uid, n); touchSeen(uid);
    if (n === 1) emitAll("presence", { uid, online: true, last_seen: Date.now() }, uid);
    socket.on("disconnect", () => {
      const left = (online.get(uid) || 1) - 1;
      if (left > 0) return online.set(uid, left);
      online.delete(uid); touchSeen(uid);
      emitAll("presence", { uid, online: false, last_seen: Date.now() }, uid);
    });
  }
  // camera on/off, used to measure "video on" time for the profile
  socket.on("cam", on => {
    if (!socket.room) return;
    if (on && !socket.camSince) socket.camSince = Date.now();
    if (!on && socket.camSince){ socket.camMs = (socket.camMs || 0) + Date.now() - socket.camSince; socket.camSince = 0; }
  });
  socket.on("join", async (room, ack) => {
    if (typeof ack !== "function") return;
    if (!ROOM_RE.test(room)) return ack({ error: "Invalid meeting code." });
    if (socket.room) return ack({ error: "Already in a meeting." });
    const rm = db.prepare("SELECT * FROM rooms WHERE code = ?").get(room);
    if (rm && !rm.active) return ack({ error: "This room has been closed by the admin." });
    const members = await io.in(room).fetchSockets();
    if (members.length >= MAX_ROOM_SIZE) return ack({ error: `This meeting is full (${MAX_ROOM_SIZE} people max).` });
    socket.join(room);
    if (rm && !GT.has(room)) startVibe(room, roomVibe(rm));
    socket.room = room; socket.joinedAt = Date.now(); socket.camMs = 0; socket.camSince = 0;
    logEvent(socket.user.id, "joined_room", { room });
    socket.to(room).emit("peer-joined", peerInfo(socket));
    ack({ peers: members.map(peerInfo), myPins: db.prepare("SELECT pinned_id id FROM pins WHERE pinner_id = ?").all(socket.user.id).map(r => r.id),
      following: db.prepare("SELECT followee_id id FROM follows WHERE follower_id = ?").all(socket.user.id).map(r => r.id), timer: gtOut(room), settings: rm ? { name: rm.name, video: !!rm.video_allowed, audio: !!rm.audio_allowed, vibe: roomVibe(rm), vibeLabel: VIBES[roomVibe(rm)].label } : null });
  });

  // relay SDP offers/answers and ICE candidates between two peers in the same room
  socket.on("signal", ({ to, data } = {}) => {
    const target = io.sockets.sockets.get(to);
    if (!target || !socket.room || target.room !== socket.room) return;
    target.emit("signal", { from: socket.id, name: socket.user.name, data });
  });

  // pin someone in the room (a "you're doing great, I see you" marker); optional auto pin-back
  socket.on("pin", ({ to, on } = {}, ack) => {
    if (typeof ack !== "function") return;
    const t = io.sockets.sockets.get(to);
    if (!t || !socket.room || t.room !== socket.room || t.user.id === socket.user.id) return ack({ error: "Pick someone else in this room." });
    if (!on){ db.prepare("DELETE FROM pins WHERE pinner_id = ? AND pinned_id = ?").run(socket.user.id, t.user.id); io.in(socket.room).emit("peer-updated", peerInfo(t)); return ack({ ok: true, on: false }); }
    if (sockSettings(socket).disable_pinning) return ack({ error: "Pinning is turned off in your settings." });
    if (sockSettings(t).disable_pinning) return ack({ error: t.user.name + " has turned pinning off." });
    if (blockedEither(socket.user.id, t.user.id)) return ack({ error: "You can't pin this person." });
    const added = db.prepare("INSERT OR IGNORE INTO pins VALUES (?, ?, ?)").run(socket.user.id, t.user.id, Date.now()).changes;
    if (added){
      logEvent(socket.user.id, "pinned", { name: t.user.name });
      const mutual = !!db.prepare("SELECT 1 FROM pins WHERE pinner_id = ? AND pinned_id = ?").get(t.user.id, socket.user.id);
      if (!sockSettings(t).hide_pin_notifs) t.emit("pinned", { from: socket.user.name, id: socket.id, mutual });
      if (!mutual && sockSettings(t).auto_pinback && !sockSettings(socket).disable_pinning){
        db.prepare("INSERT OR IGNORE INTO pins VALUES (?, ?, ?)").run(t.user.id, socket.user.id, Date.now());
        t.emit("pin-mark", { id: socket.id, userId: socket.user.id, on: true, name: socket.user.name });
        if (!sockSettings(socket).hide_pin_notifs) socket.emit("pinned", { from: t.user.name, id: t.id, mutual: true, auto: true });
        io.in(socket.room).emit("peer-updated", peerInfo(socket));
      }
    }
    io.in(socket.room).emit("peer-updated", peerInfo(t));
    ack({ ok: true, on: true });
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
    if (!g || g.vibe || (g.host !== socket.user.id && socket.user.role !== "admin")) return;
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
    if (g.vibe) return;
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
    if (!socket.room) return;
    socket.to(socket.room).emit("peer-left", { id: socket.id });
    const ms = Date.now() - socket.joinedAt, cam = (socket.camMs || 0) + (socket.camSince ? Date.now() - socket.camSince : 0);
    if (ms > 60000) logEvent(socket.user.id, "left_room", { room: socket.room, ms, cam: Math.min(cam, ms) });
  });
});

server.listen(PORT, () => console.log(`Chaupal running on http://localhost:${PORT}`));
// optional second port for the API only (used by the Emergent preview; not needed on Render)
if (process.env.API_PORT) http.createServer(app).listen(Number(process.env.API_PORT));
