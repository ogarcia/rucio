# Resource-usage statistics

The **stats** role records what a `rucio-bootstrap` node consumes — concurrent
peers and connections, CPU, memory, machine traffic, load — into a small SQLite
database, and (optionally) serves a web dashboard that turns those numbers into
the hardware a bootstrap node needs.

There are two build features:

- **`stats`** — recording only. Pulls just SQLite; no web server. A headless
  node writes a snapshot once a minute and nothing else.
- **`stats-web`** — recording **plus** the `/stats` dashboard and its JSON API,
  served on the [shared HTTP server](02-indexer.md#api-section). Included in the
  `latest-bootstrap` container image.

The stats role does not download, store, or serve any file content, and does not
affect the DHT.

Most figures are read from Linux `/proc`. On a non-Linux host those fields are
recorded as `NULL` (peer and connection counts still work); this is deliberate —
a bootstrap node realistically runs on Linux, so no cross-platform metrics
dependency is pulled in.

---

## Running it

On a `stats`- or `stats-web`-feature build (as in the `latest-bootstrap` image)
recording **runs by default**. On first run the database is created
automatically at `~/.local/share/rucio-bootstrap/stats.db`. There is nothing to
turn on.

To disable it for one invocation:

```sh
rucio-bootstrap --no-stats
```

…or in `~/.config/rucio-bootstrap/config.toml`:

```toml
[stats]
enabled = false
```

`--no-stats` overrides `stats.enabled` for that invocation only; the config file
is not modified. On a build *without* the `stats` feature the flag does not
exist and nothing is recorded.

---

## Configuration

### `[stats]` section

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | Record snapshots at startup (on a `stats`-feature build). Set to `false`, or pass `--no-stats`, to record nothing. |
| `db` | `~/.local/share/rucio-bootstrap/stats.db` | SQLite database path. Created automatically. |
| `retention_days` | `90` | Delete samples older than this many days. Samples are tiny (one per minute), so a long history is cheap. |

The dashboard and JSON API are served on the shared HTTP server — its bind
address and port are the [`[api]` section](02-indexer.md#api-section)
(`127.0.0.1:3003` by default), the same port the indexer uses.

### CLI flags (stats)

| Flag | Env variable | Overrides |
|---|---|---|
| `--no-stats` | — | forces `stats.enabled = false` |
| `--stats-db <PATH>` | `RUCIO_BOOTSTRAP_STATS_DB` | `stats.db` |
| `--stats-retention-days <N>` | `RUCIO_BOOTSTRAP_STATS_RETENTION_DAYS` | `stats.retention_days` |

### Full example

```toml
[node]
identity = "/var/lib/rucio-bootstrap/identity.key"
listen   = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

[api]
listen = "0.0.0.0:3003"

[stats]
enabled        = true
db             = "/var/lib/rucio-bootstrap/stats.db"
retention_days = 90
```

---

## What is recorded

One row is written to the `samples` table every 60 seconds. Each row holds:

| Group | Fields | Source |
|---|---|---|
| Network | `connected_peers`, `connections` (concurrent), `conns_opened`, `conns_closed` (churn since the last sample) | the node itself |
| CPU | `cpu_ms` — process CPU time consumed in the interval | `/proc/self/stat` |
| Memory | `rss_kb`, `peak_rss_kb`, `mem_available_kb` | `/proc/self/status`, `/proc/meminfo` |
| Process | `threads`, `open_fds` | `/proc/self/status`, `/proc/self/fd` |
| Machine traffic | `net_rx_bytes`, `net_tx_bytes` — bytes over every non-loopback interface in the interval (**the metric a VPS bills on**) | `/proc/net/dev` |
| Load | `load1`, `load5`, `load15` | `/proc/loadavg` |

Counters (CPU, traffic) are stored as the **delta over the interval**, so summing
a column gives the total for a period. A one-row `host_info` table records the
box the node ran on (CPU count, total RAM, kernel, hostname).

---

## Dashboard

With a `stats-web` build the node serves a small, no-JavaScript dashboard at
**`/stats`** on the shared HTTP server. It shows, for a selectable time window
(1h / 24h / 7d / 30d / All):

- a **host card** — the machine's CPU count, RAM and kernel;
- a **search index card** — when this node also runs the [indexer](02-indexer.md),
  its counters (files indexed, enriched share, providers, provider records, and
  how far back the index reaches);
- **stat tiles** — the peaks that size hardware (peak concurrent peers and
  connections, peak memory and what fraction of RAM it is, peak/average CPU as a
  percentage of one core, total and per-day traffic with a monthly projection,
  peak load, peak open files and threads);
- a **suggested instance** — a rough heuristic (RAM = peak memory ×2, vCPU =
  peak core-equivalent, traffic projected to 30 days) as a starting point.

Point a browser at `http://<api.listen>/stats`. Like the indexer's search site
it is safe to expose read-only.

---

## REST API

The same aggregates are available as JSON. Interactive documentation for every
role on the node is at `http://<api.listen>/api/docs`; the generic
`GET /health` probe is documented in the [indexer guide](02-indexer.md#get-health).

All read-only statistics live under `/api/v1/stats/*`: resource usage
(`/resources`, `/host`, below) and — when the indexer role is running — the
search-index counters at
[`/api/v1/stats/index`](02-indexer.md#get-apiv1statsindex).

### `GET /api/v1/stats/resources`

Public endpoint. Aggregate resource usage over a window.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `window` | integer | `0` | Window in seconds to aggregate over. `0` = all recorded history. |

```sh
# Last 24 hours
curl "http://localhost:3003/api/v1/stats/resources?window=86400"
```

```json
{
  "window_secs": 86400,
  "samples": 1440,
  "span_secs": 86340,
  "peak_peers": 62,
  "peak_connections": 88,
  "conns_opened": 5100,
  "conns_closed": 5040,
  "peak_rss_kb": 130000,
  "avg_cpu_pct": 3.2,
  "peak_cpu_pct": 15.0,
  "net_rx_bytes": 4200000000,
  "net_tx_bytes": 2600000000,
  "peak_load1": 0.55,
  "peak_open_fds": 96,
  "peak_threads": 11
}
```

`avg_cpu_pct` / `peak_cpu_pct` are percentages of **one core** (150 = one and a
half cores). Peaks and the `/proc`-derived fields are `null` before any sample
exists, or on non-Linux hosts.

### `GET /api/v1/stats/host`

Public endpoint. The box the node is running on. Returns `404` before the first
sample cycle has recorded the host facts.

```json
{
  "captured_at": 1716886400,
  "hostname": "bootstrap-1",
  "kernel": "6.1.0-21-amd64",
  "num_cpus": 4,
  "mem_total_kb": 8035712
}
```

---

## Sizing the hardware (and asking a sponsor)

The point of recording this is to run a bootstrap node long enough to know what
it actually needs, then provision (or request) the right machine — no more, no
less. After a representative period (a week is usually plenty), read the peaks:

- **RAM** follows `peak_rss_kb` — leave headroom above the peak.
- **CPU** follows `peak_cpu_pct` — one core is 100.
- **Traffic** is what a VPS bills on: sum `net_rx_bytes + net_tx_bytes` over a
  known span and project to a month.

```sh
# Monthly machine traffic projection from the recorded samples
sqlite3 ~/.local/share/rucio-bootstrap/stats.db "
  SELECT (SUM(net_rx_bytes + net_tx_bytes) * 1.0
          / (MAX(ts) - MIN(ts)) * 86400 * 30) / 1e9 AS gb_per_month
  FROM samples;"
```

The dashboard's *suggested instance* line does this for you. These concrete
numbers are also what makes the case to an infrastructure sponsor: a specific,
small ask ("a node needs ~1 vCPU / 1 GB RAM and ~X GB/month") is far more
convincing than a vague one.

---

## Container deployment

The `latest-bootstrap` image records stats and serves the panel by default — no
flag needed. It shares port 3003 with the indexer:

```sh
podman run -d \
  --name rucio-bootstrap \
  --restart unless-stopped \
  -p 4321:4321 \
  -p 3003:3003 \
  -e RUCIO_BOOTSTRAP_API_LISTEN=0.0.0.0:3003 \
  -v rucio-bootstrap-data:/var/lib/rucio \
  ghcr.io/ogarcia/rucio:latest-bootstrap
```

The dashboard is then at `http://<host>:3003/stats`. Pass `--no-stats` (or set
`stats.enabled = false` in the config inside the volume) to record nothing.

### Systemd

The environment variables (or the equivalent config keys) extend the unit file
from [01 — Bootstrap node](01-bootstrap-node.md):

```ini
Environment=RUCIO_BOOTSTRAP_STATS_DB=/var/lib/rucio-bootstrap/stats.db
```

Recording runs by default on a `stats`-feature build, so `ExecStart` needs no
flag; add `--no-stats` to turn it off.

---

## Database

The stats are stored in a single SQLite file in WAL journal mode.

| Table | Description |
|---|---|
| `samples` | One row per 60-second snapshot |
| `host_info` | A single row describing the machine (CPU count, RAM, kernel, hostname) |

### Manual inspection

```sh
# Peak concurrent peers and RSS over the last day
sqlite3 ~/.local/share/rucio-bootstrap/stats.db "
  SELECT MAX(connected_peers) AS peak_peers,
         MAX(peak_rss_kb)     AS peak_rss_kb,
         MAX(load1)           AS peak_load1
  FROM samples
  WHERE ts > strftime('%s','now') - 86400;"
```

### Backup

```sh
# Online backup (safe while the node is running)
sqlite3 ~/.local/share/rucio-bootstrap/stats.db \
  ".backup /path/to/stats-backup.db"
```
