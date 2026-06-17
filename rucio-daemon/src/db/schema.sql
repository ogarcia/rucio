-- Rucio daemon database schema
-- Pre-stable: drop and recreate the DB file if this changes.
-- All hashes are stored as 32-byte BLOB (BLAKE3).
-- Timestamps are Unix seconds (INTEGER).

-- ---------------------------------------------------------------------------
-- shared_dirs
-- Directories registered for sharing and watched for changes.
-- The download_dir is inserted on startup with protected=1 and cannot be
-- removed by the user.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shared_dirs (
    id          INTEGER PRIMARY KEY,
    path        TEXT    NOT NULL UNIQUE,  -- absolute path, no trailing slash
    protected   INTEGER NOT NULL DEFAULT 0,  -- 1 = cannot be removed by user
    added_at    INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- shared_files
-- Files that this node is actively sharing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shared_files (
    id          INTEGER PRIMARY KEY,
    root_hash   BLOB    NOT NULL UNIQUE,   -- 32 bytes, canonical file id
    name        TEXT    NOT NULL,
    size        INTEGER NOT NULL,          -- bytes
    mime_type   TEXT,
    path        TEXT    NOT NULL,          -- absolute path on disk
    chunk_size  INTEGER NOT NULL DEFAULT 4194304,  -- 4 MiB
    added_at    INTEGER NOT NULL,          -- Unix seconds
    mtime       INTEGER NOT NULL DEFAULT 0 -- file mtime in Unix seconds, change signal for the rescan
);

-- Look up a share by its on-disk path. The watcher and the startup rescan do
-- this once per file (to re-index or drop it). Without the index each lookup
-- is a full table scan, making a rescan of a large share O(files squared).
CREATE INDEX IF NOT EXISTS idx_shared_files_path ON shared_files(path);

-- No per-chunk hash table: with bao verified streaming a chunk is verified as
-- a slice against the file's root_hash, and chunk_count is derived as
-- ceil(size / chunk_size). The Merkle tree (outboard) lives as a regenerable
-- sidecar file, not in the DB.

-- ---------------------------------------------------------------------------
-- pins
-- Manually pinned content: a root hash the user wants kept available on this
-- node (fetched if missing, then shared and re-provided). This row is the user
-- intent, distinct from an incidental share. Pinned content is sacred -- the
-- future cooperative-mirror reconcile never evicts it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pins (
    root_hash   BLOB    PRIMARY KEY,    -- 32 bytes, BLAKE3
    collection  TEXT,                   -- publishing collection label, NULL = uncollected. Distinct from download categories (which vanish when the download row is removed)
    added_at    INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- pin_subscriptions (cooperative pinning)
-- Peers whose published pin-set we mirror. quota_bytes is the hard ceiling of
-- disk we devote to mirroring this peer (best-effort up to it, never beyond).
-- last_version is the pin-set version we last synced, so we skip unchanged ones.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pin_subscriptions (
    peer_id        TEXT    PRIMARY KEY,   -- libp2p PeerId (base58)
    quota_bytes    INTEGER NOT NULL,      -- max bytes we mirror for this peer
    follow_all     INTEGER NOT NULL DEFAULT 1,  -- 1 = mirror the whole peer; 0 = only the collections listed in pin_subscription_collections
    last_version   INTEGER NOT NULL DEFAULT 0,
    last_synced_at INTEGER NOT NULL DEFAULT 0,
    added_at       INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- pin_subscription_collections (cooperative pinning)
-- The set of a peer's collections we follow when follow_all = 0. Free-text
-- labels chosen by the publisher; the special label '' (empty string) means
-- "the publisher's uncollected pins". Ignored entirely while follow_all = 1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pin_subscription_collections (
    peer_id     TEXT    NOT NULL REFERENCES pin_subscriptions(peer_id) ON DELETE CASCADE,
    collection  TEXT    NOT NULL,         -- '' = the peer's uncollected pins
    PRIMARY KEY (peer_id, collection)
);

-- ---------------------------------------------------------------------------
-- subscription_seen_collections (cooperative pinning)
-- The distinct collections a peer advertises in its pin-set, refreshed on every
-- sync from the FULL set before any follow-scope filtering. This is what the UI
-- offers as available collections, so a subscriber can discover and pick
-- collections even when follow_all = 0 and nothing is being mirrored yet.
-- '' = the peer's uncollected pins.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subscription_seen_collections (
    peer_id     TEXT    NOT NULL REFERENCES pin_subscriptions(peer_id) ON DELETE CASCADE,
    collection  TEXT    NOT NULL,
    PRIMARY KEY (peer_id, collection)
);

-- ---------------------------------------------------------------------------
-- mirror_pins (cooperative pinning)
-- Content we mirror on behalf of a subscription. A root hash may be wanted by
-- several subscriptions (composite key). state is one of: 'wanted' (selected,
-- to fetch/keep), 'skipped' (over quota, intentionally not mirrored), or
-- 'cancelled' (the user opted out of this file -- materialised each sync from
-- mirror_optouts; not fetched, not counted against quota). A hash is retained on
-- disk while it is a manual pin OR 'wanted' by >=1 subscription.
-- ON DELETE CASCADE: removing a subscription drops its mirror rows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mirror_pins (
    root_hash   BLOB    NOT NULL,         -- 32 bytes, BLAKE3
    peer_id     TEXT    NOT NULL REFERENCES pin_subscriptions(peer_id) ON DELETE CASCADE,
    name        TEXT,
    size        INTEGER NOT NULL DEFAULT 0,
    state       TEXT    NOT NULL DEFAULT 'wanted',
    collection  TEXT,                     -- the publisher's collection for this pin, NULL = uncollected. One pin has exactly one collection, so this stays an attribute (not part of the key)
    added_at    INTEGER NOT NULL,
    PRIMARY KEY (root_hash, peer_id)
);

CREATE INDEX IF NOT EXISTS idx_mirror_pins_peer ON mirror_pins(peer_id);

-- ---------------------------------------------------------------------------
-- mirror_owned (cooperative pinning)
-- Hashes whose local copy exists ONLY because the reconcile fetched it to
-- mirror a subscription (the user did not already hold it). This is the
-- discriminator that lets eviction delete mirror content without ever touching
-- the user's own downloads or shares. A row is added when the reconcile decides
-- to fetch a missing wanted hash, and removed when that content is evicted.
-- Eviction also requires the file to live under pin_dir as a second guard.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mirror_owned (
    root_hash   BLOB    PRIMARY KEY,    -- 32 bytes, BLAKE3
    added_at    INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- mirror_optouts (cooperative pinning)
-- Files the user explicitly cancelled from a subscription's mirror. This is the
-- durable record of "don't mirror this hash from this peer": the reconcile skips
-- it, the file shows as 'cancelled', and it survives clearing download history,
-- pin-set version changes, and the publisher un-pinning then re-pinning. It is
-- removed only when the user re-requests the file, or via ON DELETE CASCADE when
-- the subscription is removed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mirror_optouts (
    peer_id     TEXT    NOT NULL REFERENCES pin_subscriptions(peer_id) ON DELETE CASCADE,
    root_hash   BLOB    NOT NULL,         -- 32 bytes, BLAKE3
    added_at    INTEGER NOT NULL,
    PRIMARY KEY (peer_id, root_hash)
);

-- ---------------------------------------------------------------------------
-- categories
-- Optional download categories. A category may pin its own download_dir so the
-- user can route downloads to different folders; download_dir NULL means the
-- category uses the global storage.download_dir. A category directory is shared
-- and protected exactly like the global download_dir.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS categories (
    id             INTEGER PRIMARY KEY,
    name           TEXT    NOT NULL UNIQUE,
    download_dir   TEXT,                       -- absolute path, no trailing slash; NULL = use global
    color          TEXT,                       -- badge colour as a hex string, e.g. #3b82f6; NULL = UI default
    match_keywords TEXT,                       -- '|'-separated substrings; a new download whose name contains one is auto-filed here
    added_at       INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- downloads
-- Files being downloaded (or queued).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS downloads (
    id              INTEGER PRIMARY KEY,
    root_hash       BLOB    NOT NULL UNIQUE,
    name            TEXT    NOT NULL,
    total_size      INTEGER NOT NULL,
    dest_path       TEXT    NOT NULL,      -- final destination on disk
    status          TEXT    NOT NULL DEFAULT 'queued',
    -- 'finding_providers' | 'queued' | 'downloading' | 'stalled' | 'paused' | 'completed' | 'error' | 'cancelled'
    bytes_done      INTEGER NOT NULL DEFAULT 0,
    error_msg       TEXT,
    category_id     INTEGER REFERENCES categories(id) ON DELETE SET NULL,  -- NULL = global download_dir
    added_at        INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- emule_downloads
-- eMule (ed2k) downloads.  Completely separate from the libp2p downloads table
-- so the eMule subsystem can be removed without touching downloads at all.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS emule_downloads (
    id          INTEGER PRIMARY KEY,
    ed2k_hash   BLOB    NOT NULL UNIQUE,  -- 16 bytes MD4, canonical identifier
    name        TEXT    NOT NULL,
    total_size  INTEGER NOT NULL,
    ed2k_link   TEXT    NOT NULL,         -- original link string for resume
    status      TEXT    NOT NULL DEFAULT 'finding_providers',
    -- 'finding_providers' | 'downloading' | 'completed' | 'error' | 'cancelled'
    bytes_done  INTEGER NOT NULL DEFAULT 0,
    dest_path   TEXT    NOT NULL DEFAULT '',
    error_msg   TEXT,
    category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,  -- NULL = global download_dir
    added_at    INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- download_chunks
-- Per-chunk state for an in-progress download.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS download_chunks (
    id              INTEGER PRIMARY KEY,
    download_id     INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    idx             INTEGER NOT NULL,
    size            INTEGER NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'pending',
    -- 'pending' | 'downloading' | 'done'
    UNIQUE (download_id, idx)
);

CREATE INDEX IF NOT EXISTS idx_dl_chunks_status ON download_chunks(download_id, status);

-- ---------------------------------------------------------------------------
-- emule_shared_files
-- Files downloaded from eMule that we keep serving to the Kad network after the
-- download finishes (good-citizen seeding). Decoupled from emule_downloads on
-- purpose: clearing the completed-downloads list must NOT stop sharing. A file
-- is shared until it is modified or removed on disk (size/mtime change),
-- detected at startup and via the filesystem watcher.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS emule_shared_files (
    id          INTEGER PRIMARY KEY,
    ed2k_hash   BLOB    NOT NULL UNIQUE,  -- 16 bytes MD4, canonical identifier
    name        TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    path        TEXT    NOT NULL,         -- absolute path of the final file on disk
    mtime       INTEGER NOT NULL,         -- file mtime in Unix seconds (change signal)
    hashset     BLOB    NOT NULL DEFAULT X'',  -- ed2k part-hash set, 16 bytes per part (empty for single-part files)
    added_at    INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- metrics
-- Single-row table holding cumulative lifetime counters.
-- Updated periodically from the in-memory session counters.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton
    uploaded_bytes      INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes    INTEGER NOT NULL DEFAULT 0,
    chunks_served       INTEGER NOT NULL DEFAULT 0,
    chunks_received     INTEGER NOT NULL DEFAULT 0,
    chunks_rejected     INTEGER NOT NULL DEFAULT 0
);

-- Ensure the singleton row exists from the start.
INSERT OR IGNORE INTO metrics (id) VALUES (1);

-- ---------------------------------------------------------------------------
-- known_peers
-- Peers seen on the network, kept as a hint cache (not authoritative).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS known_peers (
    id          INTEGER PRIMARY KEY,
    peer_id     TEXT    NOT NULL UNIQUE,   -- libp2p PeerId (base58)
    addrs       TEXT    NOT NULL,          -- JSON array of multiaddrs
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    high_id     INTEGER NOT NULL DEFAULT 1 -- 1 = HighID, 0 = LowID
);

-- ---------------------------------------------------------------------------
-- notifications
-- In-app notification centre records (download finished, indexing done, ...).
-- Generic on purpose so the same rows can later feed outbound webhooks.
-- Retention is bounded by the insert path, not the schema.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notifications (
    id          INTEGER PRIMARY KEY,
    kind        TEXT    NOT NULL,          -- download, system
    title       TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    ref_key     TEXT,                      -- optional resource reference (e.g. blake3 hex)
    created_at  INTEGER NOT NULL,
    read        INTEGER NOT NULL DEFAULT 0 -- 1 = seen, 0 = unread
);

CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at);
