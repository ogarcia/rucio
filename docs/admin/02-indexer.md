# DHT indexer

The **DHT indexer** is an optional role built into `rucio-bootstrap` (requires
the `indexer` build feature, included in the `latest-bootstrap` container image).

The node captures every `ADD_PROVIDER` announcement it receives from the
Kademlia DHT — i.e. every time a peer publishes a file to the network — and
records the hash and the announcing peer in a local SQLite database.  It then
contacts the announcing peer to fetch the file's name and size via the manifest
protocol (**enrichment**), so the search API can match on human-readable names
rather than raw hashes.

The indexer does not download, store, or serve any file content.

---

## Running the indexer

When `rucio-bootstrap` is built with the `indexer` feature (as in the
`latest-bootstrap` container image), the indexer **runs by default** — that is
the whole point of that build. On first run the SQLite database is created
automatically at `~/.local/share/rucio-bootstrap/index.db`. There is nothing to
turn on.

### Running as a plain bootstrap node instead

To run an `indexer`-feature build as a plain bootstrap node (no capturing, no
search API), disable the role:

```sh
rucio-bootstrap --no-index
```

…or set it in `~/.config/rucio-bootstrap/config.toml`:

```toml
[indexer]
enabled = false
```

The `--no-index` flag overrides `indexer.enabled` for that invocation only; the
config file is not modified. On a build *without* the `indexer` feature the flag
does not exist and the node is always a plain bootstrap.

---

## Configuration

### `[indexer]` section

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | Run the indexer at startup (on an `indexer`-feature build). Set to `false`, or pass `--no-index`, for a plain bootstrap node. |
| `db` | `~/.local/share/rucio-bootstrap/index.db` | SQLite database path. Created automatically. |
| `api_listen` | `127.0.0.1:3003` | Bind address for the REST search API. Change to `0.0.0.0:3003` to expose it on the network. |
| `api_token` | *(unset)* | Bearer token protecting the `/api/v1/admin/*` endpoints. **Admin endpoints are disabled when this is unset.** |
| `retention_days` | `30` | Delete records not refreshed within this many days. 0 = keep forever. |
| `enrich` | `true` | Contact announcing peers to resolve file name and size. Disable with `false` or `--no-enrich` to index hashes only. |
| `identity_count` | `0` | Number of **additional** Kademlia identities to spawn. See [Multi-identity](#multi-identity). |

#### Generating an API token

The token is a static bearer secret with no expiry — anyone holding it can call
the admin endpoints. Use a long, random value (don't pick a memorable string):

```sh
openssl rand -hex 32        # 64 hex chars (256 bits) — recommended
openssl rand -base64 24     # shorter, still 192 bits of entropy
# no openssl handy?
head -c 32 /dev/urandom | base64
```

Prefer passing it through the `RUCIO_BOOTSTRAP_API_TOKEN` environment variable
(or a secrets manager) over writing it into `config.toml`, so it never lands in
a file or backup in clear text. To rotate it, set a new value and restart the
node. Treat the admin API as private regardless: keep `api_listen` on a trusted
network rather than the public internet.

### Full example

```toml
[node]
identity = "/var/lib/rucio-bootstrap/identity.key"
listen   = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

[indexer]
enabled        = true
db             = "/var/lib/rucio-bootstrap/index.db"
api_listen     = "0.0.0.0:3003"
api_token      = "change-me"
retention_days = 30
enrich         = true
identity_count = 3
```

### CLI flags (indexer)

| Flag | Env variable | Overrides |
|---|---|---|
| `--no-index` | — | `indexer.enabled` (forces off) |
| `--index-db <PATH>` | `RUCIO_BOOTSTRAP_INDEX_DB` | `indexer.db` |
| `--api-listen <ADDR>` | `RUCIO_BOOTSTRAP_API_LISTEN` | `indexer.api_listen` |
| `--api-token <TOKEN>` | `RUCIO_BOOTSTRAP_API_TOKEN` | `indexer.api_token` |
| `--retention-days <N>` | `RUCIO_BOOTSTRAP_RETENTION_DAYS` | `indexer.retention_days` |
| `--no-enrich` | — | forces `indexer.enrich = false` |
| `--identity-count <N>` | `RUCIO_BOOTSTRAP_IDENTITY_COUNT` | `indexer.identity_count` |

---

## Web search interface

When the indexer is running it also serves a small, human-facing search site
on the same `api_listen` address — a search engine for the network, much like
DuckDuckGo or Google:

- **`/`** — a landing page with the Rucio logo and a search box.
- **`/search?q=…`** — results, with a compact header that repeats the search
  box and a sort selector (newest, oldest, most sources, largest). Each
  result shows the file name (or the hash, if not enriched yet), its size, how
  many peers provide it (a colour-coded chip — green when well-seeded, red for
  a single source), when it was last seen, and the canonical `rucio:` magnet
  link to paste into a client.

It is **server-rendered with no JavaScript** and reuses the very same query as
[`GET /api/v1/search`](#get-apiv1search), so the page and the API never drift
apart. The site and the public API need no authentication (only the
`/api/v1/admin/*` endpoints do), so it is safe to expose read-only — point a
browser at `http://<api_listen>/`.

To make it a public search portal, bind `api_listen` to `0.0.0.0:3003` (or put
a reverse proxy with TLS in front of it). Keep it on a trusted network only if
you also set an `api_token`, since the admin endpoints share the port.

---

## REST API

The indexer exposes a REST API when running.  Interactive API documentation
is available at `http://<api_listen>/api/docs`.

### `GET /health`

Public endpoint.  Returns HTTP 200 with basic status information.

```json
{
  "status": "ok",
  "uptime_secs": 3600
}
```

### `GET /api/v1/search`

Public endpoint.  Returns the most recently announced results first.

The query `q` is matched two ways:

- as a hex **prefix** of the content hash (a single whitespace-free token);
- against the indexed **file name**, split into whitespace-separated terms that
  must *all* appear as substrings. Matching is case- and accent-insensitive
  (folded the same way as the Rucio network), so word separators (dots, dashes,
  underscores) don't matter and `camion` finds `Camión...`. `ghost in the shell`
  matches `Ghost.in.the.Shell.ARISE...`. The `ñ` is treated as a distinct letter
  and not folded.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `q` | string | `""` | Search query (empty returns all records). |
| `sort` | string | `newest` | Order: `newest` (newest first), `oldest`, `providers` (most sources — availability, also accepts `relevance`), `size` (largest first). |
| `limit` | integer | `50` | Maximum results per page (clamped to 1–500). |
| `offset` | integer | `0` | Pagination offset. |

```sh
# Multi-word name search (all terms must match)
curl "http://localhost:3003/api/v1/search?q=ghost+in+the+shell"

# Single word
curl "http://localhost:3003/api/v1/search?q=ubuntu"

# Search by hash prefix
curl "http://localhost:3003/api/v1/search?q=aabbccdd"

# Paginate
curl "http://localhost:3003/api/v1/search?q=&limit=50&offset=100"
```

**Response:**

```json
[
  {
    "hash": "aabbccdd...",
    "name": "ubuntu-24.04-desktop.iso",
    "size": 5368709120,
    "providers": 3,
    "first_seen": 1716800000,
    "last_seen": 1716886400
  }
]
```

`name` and `size` are `null` for hashes that have not been enriched yet.

### `GET /api/v1/records`

Public endpoint.  Returns all records in the index (most recent first),
paginated.  Same parameters and response shape as `/api/v1/search` without
the `q` filter.

### `GET /api/v1/admin/stats`

**Requires `Authorization: Bearer <token>` header.**  Returns aggregate
counters over the whole index.

```sh
curl -H "Authorization: Bearer change-me" \
     http://localhost:3003/api/v1/admin/stats
```

```json
{
  "total_records": 42150,
  "distinct_hashes": 18920,
  "distinct_providers": 3140,
  "enriched_files": 12080,
  "oldest": 1714000000,
  "newest": 1716886400
}
```

### `POST /api/v1/admin/prune`

**Requires `Authorization: Bearer <token>` header.**  Immediately deletes
all records whose `last_seen` timestamp is older than `retention_days` days
(as configured).  Returns the number of rows deleted.

```sh
curl -X POST -H "Authorization: Bearer change-me" \
     http://localhost:3003/api/v1/admin/prune
```

```json
{ "deleted": 512 }
```

> **Note:** pruning also runs automatically once at startup and then once
> every 24 hours.  The manual endpoint is for on-demand cleanup.

---

## Enrichment

When `enrich = true` (the default), each time a new hash is seen the indexer
dials the announcing peer and requests its **manifest** — a small metadata
record containing the file name, total size, and chunk layout.  The name and
size are stored in the `files` table and returned by the search API.

Enrichment is **best-effort**: if the announcing peer is unreachable or does
not respond, only the hash and provider are recorded.  The indexer does not
retry failed enrichments.

To index hashes only (faster, no outgoing connections to peers):

```toml
[indexer]
enrich = false
```

or pass `--no-enrich` at startup.

---

## Multi-identity

In Kademlia, a node only receives `ADD_PROVIDER` announcements for hashes
that are *close* to its own Peer ID in the 256-bit keyspace.  A single
identity therefore only indexes a fraction of the network's content.

Setting `identity_count = N` spawns **N additional identities** alongside
the primary, each with a different Peer ID and therefore a different position
in the keyspace.  Together they cover a larger fraction of the DHT.

```toml
[indexer]
identity_count = 3   # primary + 3 extra = 4 identities total
```

Each extra identity:
- Gets its own key file next to the primary: `identity-1.key`, `identity-2.key`, …
- Listens on an **ephemeral TCP port** (assigned by the OS at startup; no
  static port needed beyond 4321 for the primary).
- Bootstraps from the same `node.bootstrap_peers` as the primary.
- Sends captured provider records to the same shared database.

Key files are generated automatically on first use and reused on subsequent
restarts, so the extra Peer IDs are stable across restarts.

### Choosing `identity_count`

| Value | Coverage (approximate) | Notes |
|---|---|---|
| 0 (default) | 1/N of the DHT (N = total DHT size) | Suitable for small networks |
| 3 | ~4× more than a single identity | Good starting point for a public indexer |
| 7 | ~8× | Higher coverage; each identity adds RAM and a Kademlia routing table |

There is no hard limit, but each additional identity consumes a small amount
of memory (one libp2p swarm per identity).  Values above 15–20 are unlikely
to be useful in practice.

---

## Container deployment with the indexer

```sh
podman run -d \
  --name rucio-bootstrap \
  --restart unless-stopped \
  -p 4321:4321 \
  -p 3003:3003 \
  -e RUCIO_BOOTSTRAP_API_LISTEN=0.0.0.0:3003 \
  -e RUCIO_BOOTSTRAP_API_TOKEN=changeme \
  -v rucio-bootstrap-data:/var/lib/rucio \
  ghcr.io/ogarcia/rucio:latest-bootstrap
```

The `latest-bootstrap` image runs the indexer by default — no flag needed.
Pass `--no-index` (or set `indexer.enabled = false` in the config file inside
the volume) to run it as a plain bootstrap node instead.

> Port 3003 serves both the web search interface (`/`) and the REST API.  If
> you only want it accessible from localhost, omit `-p 3003:3003` and use
> `docker exec` or an SSH tunnel to reach it.

### Systemd with the indexer

Add to the `[Service]` section of the unit file from
[01 — Bootstrap node](01-bootstrap-node.md):

```ini
ExecStart=/usr/local/bin/rucio-bootstrap
Environment=RUCIO_BOOTSTRAP_API_LISTEN=127.0.0.1:3003
Environment=RUCIO_BOOTSTRAP_API_TOKEN=changeme
Environment=RUCIO_BOOTSTRAP_INDEX_DB=/var/lib/rucio-bootstrap/index.db
```

An `indexer`-feature build indexes by default, so `ExecStart` needs no flag;
add `--no-index` if you want a plain bootstrap node instead. The environment
variables (or the equivalent keys in `/etc/rucio-bootstrap/config.toml`)
configure the REST API and database path.

---

## Database

The index is stored in a single SQLite file in WAL journal mode.

| Table | Description |
|---|---|
| `provider_records` | One row per `(hash, provider)` pair with first/last-seen timestamps |
| `files` | One row per enriched hash with name and size |

### Manual inspection

```sh
sqlite3 ~/.local/share/rucio-bootstrap/index.db \
  "SELECT hash, name, providers, datetime(last_seen,'unixepoch')
   FROM (
     SELECT pr.hash, f.name, COUNT(*) AS providers, MAX(pr.last_seen) AS last_seen
     FROM provider_records pr LEFT JOIN files f ON f.hash = pr.hash
     GROUP BY pr.hash
   )
   ORDER BY last_seen DESC
   LIMIT 20;"
```

### Backup

```sh
# Online backup (safe while the indexer is running)
sqlite3 ~/.local/share/rucio-bootstrap/index.db \
  ".backup /path/to/index-backup.db"
```
