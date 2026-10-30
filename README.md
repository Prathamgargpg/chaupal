# Chaupal – free video meetings (Socket.IO + WebRTC)

Sign up / sign in, create a meeting link, video + audio, screen share, chat.

## How it works
- **Socket.IO** (Node server) is the signaling channel: it relays WebRTC offers, answers and ICE candidates, and chat.
- **WebRTC** carries the video and audio directly between browsers (mesh, best for 2–6 people).
- **Auth:** email + password. Passwords are hashed with bcrypt, sessions are JWTs, and the Socket.IO connection is rejected without a valid token.
- **Storage:** SQLite file for user accounts.

## Run it on your computer
```bash
npm install
cp .env.example .env      # set JWT_SECRET to a long random string
node --env-file=.env server.js
```
Open http://localhost:3000, create an account, start a meeting. Camera access works on `localhost` without HTTPS. To test with two people on one machine, use two different browsers.

## Put it on GitHub
Create an empty repo named `chaupal` on github.com, then:
```bash
git init
git add .
git commit -m "Chaupal video meetings"
git branch -M main
git remote add origin https://github.com/Prathamgargpg/chaupal.git
git push -u origin main
```

## Deploy free on Render
1. render.com → New → **Blueprint** → connect the `chaupal` repo. It reads `render.yaml` and generates `JWT_SECRET` for you.
2. Wait for the build. Your app is at `https://chaupal.onrender.com` (or similar). HTTPS is included, which browsers require for camera access.
3. Share the meeting link.

### Free-tier limits to know
- The free service sleeps after a period of inactivity, so the first visit can take up to a minute.
- The free service has no permanent disk. The SQLite file is wiped when the service restarts or redeploys, so accounts disappear. For real use, either add a persistent disk and point `DB_PATH` at it, or move the `users` table to a free hosted database (Neon Postgres, Turso, Supabase). Only the few `db` lines in `server.js` change.
- Meetings in progress end if the server restarts.

## When people can't connect
Some office and mobile networks block direct WebRTC. Add a TURN relay: create a free account at metered.ca (or similar), then set `TURN_URL`, `TURN_USERNAME` and `TURN_CREDENTIAL` in your Render environment. The app picks them up automatically.

## Scaling past ~6 people
Mesh WebRTC sends your video to everyone separately, so it gets heavy. For larger meetings, replace the peer connections with an SFU such as LiveKit (free cloud tier). Login, UI and chat stay as they are.

## Files
- `server.js` – Express, auth API, Socket.IO signaling
- `public/index.html` – the whole front end
- `render.yaml` – one-click Render config
- `.env.example` – settings
