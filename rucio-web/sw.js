// Rucio service worker — installability + fast asset load.
//
// The panel is useless without the daemon, so this is NOT a real offline app:
// API traffic (/api/*, including the /api/ws WebSocket) is never cached and
// always hits the network. We only cache the hashed static assets (wasm/JS/CSS,
// icons) so the app launches instantly and survives a brief network blip; trunk
// hashes the filenames, so we cache at runtime rather than precaching a list.
//
// Top-level navigations are deliberately NOT intercepted. When the panel sits
// behind HTTP auth on the reverse proxy, a service-worker-issued fetch that
// comes back 401 never triggers the browser's credentials dialog in Firefox —
// the prompt only fires for native navigations — so the user gets stuck on a
// bare "401 Authorization Required" that a normal reload can't clear (only a
// SW-bypassing hard reload does). Letting the document load natively keeps auth
// working, at the only cost of an offline shell we explicitly don't want.
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

  // Let the browser handle top-level navigations natively so the HTTP auth
  // dialog works behind a reverse proxy (see the header comment).
  if (req.mode === 'navigate') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  // Never touch the REST API or the live WebSocket — always go to the network.
  if (url.pathname.startsWith(SCOPE + 'api/')) return;

  event.respondWith((async () => {
    const cache = await caches.open(CACHE);

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
