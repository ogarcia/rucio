// Rucio service worker — installability + fast shell load.
//
// The panel is useless without the daemon, so this is NOT a real offline app:
// API traffic (/api/*, including the /api/ws WebSocket) is never cached and
// always hits the network. We only cache the static shell (index.html, the
// hashed wasm/JS/CSS, icons) so the app launches instantly and survives a brief
// network blip. Trunk hashes the wasm/JS filenames, so we cache at runtime
// rather than precaching a fixed list.
const CACHE = 'rucio-shell-v1';

self.addEventListener('install', () => {
  // Activate this worker as soon as it finishes installing.
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    // Drop caches from previous versions.
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

// Path prefix the app is mounted under (e.g. "/" or "/rucio/"). The worker is
// registered relative to <base href>, so its scope already carries the prefix;
// derive everything from it instead of hardcoding "/".
const SCOPE = new URL(self.registration.scope).pathname;

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  // Never touch the REST API or the live WebSocket — always go to the network.
  if (url.pathname.startsWith(SCOPE + 'api/')) return;

  event.respondWith((async () => {
    const cache = await caches.open(CACHE);

    // Navigations: network-first so a redeploy is picked up immediately, with
    // the cached shell as the offline fallback. The shell is keyed by the
    // mount root (SCOPE), not "/".
    if (req.mode === 'navigate') {
      try {
        const fresh = await fetch(req);
        cache.put(SCOPE, fresh.clone());
        return fresh;
      } catch {
        return (await cache.match(SCOPE)) || (await cache.match(req)) || Response.error();
      }
    }

    // Static assets (hashed wasm/JS/CSS, icons): stale-while-revalidate.
    const cached = await cache.match(req);
    const network = fetch(req)
      .then((resp) => {
        if (resp && resp.ok) cache.put(req, resp.clone());
        return resp;
      })
      .catch(() => cached);
    return cached || network;
  })());
});
