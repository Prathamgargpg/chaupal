// Minimal service worker: lets the app be installed and shows the app shell if the network drops.
// It always asks the network first, so updates appear right after a deploy.
const CACHE = "chaupal-shell-v1";
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", e => e.waitUntil(self.clients.claim()));
self.addEventListener("fetch", e => {
  const r = e.request, u = new URL(r.url);
  if (r.method !== "GET" || u.origin !== location.origin || u.pathname.startsWith("/api") || u.pathname.startsWith("/socket.io")) return;
  e.respondWith(
    fetch(r).then(res => { if (res.ok && (r.mode === "navigate" || /\.(png|webmanifest)$/.test(u.pathname))) { const c = res.clone(); caches.open(CACHE).then(ca => ca.put(r, c)); } return res; })
      .catch(() => caches.match(r).then(m => m || caches.match("/")))
  );
});
