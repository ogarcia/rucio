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
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    NOT NULL UNIQUE,  -- absolute path, no trailing slash
    protected   INTEGER NOT NULL DEFAULT 0,  -- 1 = cannot be removed by user
    added_at    INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- shared_files
-- Files that this node is actively sharing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shared_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    root_hash   BLOB    NOT NULL UNIQUE,   -- 32 bytes, canonical file id
    name        TEXT    NOT NULL,
    size        INTEGER NOT NULL,          -- bytes
    mime_type   TEXT,
    path        TEXT    NOT NULL,          -- absolute path on disk
    chunk_size  INTEGER NOT NULL DEFAULT 4194304,  -- 4 MiB
    added_at    INTEGER NOT NULL,          -- Unix seconds
    mtime       INTEGER NOT NULL DEFAULT 0 -- file mtime in Unix seconds, change signal for the rescan
);

-- ---------------------------------------------------------------------------
-- chunks
-- Individual chunks that belong to a shared file.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chunks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shared_file_id  INTEGER NOT NULL REFERENCES shared_files(id) ON DELETE CASCADE,
    idx             INTEGER NOT NULL,      -- 0-indexed position within file
    hash            BLOB    NOT NULL,      -- 32 bytes
    size            INTEGER NOT NULL,      -- bytes
    UNIQUE (shared_file_id, idx)
);

CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(hash);

-- ---------------------------------------------------------------------------
-- downloads
-- Files being downloaded (or queued).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS downloads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    root_hash       BLOB    NOT NULL UNIQUE,
    name            TEXT    NOT NULL,
    total_size      INTEGER NOT NULL,
    dest_path       TEXT    NOT NULL,      -- final destination on disk
    status          TEXT    NOT NULL DEFAULT 'queued',
    -- 'queued' | 'downloading' | 'paused' | 'completed' | 'error'
    bytes_done      INTEGER NOT NULL DEFAULT 0,
    error_msg       TEXT,
    added_at        INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- emule_downloads
-- eMule (ed2k) downloads.  Completely separate from the libp2p downloads table
-- so the eMule subsystem can be removed without touching downloads at all.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS emule_downloads (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ed2k_hash   BLOB    NOT NULL UNIQUE,  -- 16 bytes MD4, canonical identifier
    name        TEXT    NOT NULL,
    total_size  INTEGER NOT NULL,
    ed2k_link   TEXT    NOT NULL,         -- original link string for resume
    status      TEXT    NOT NULL DEFAULT 'finding_providers',
    -- 'finding_providers' | 'downloading' | 'completed' | 'error' | 'cancelled'
    bytes_done  INTEGER NOT NULL DEFAULT 0,
    dest_path   TEXT    NOT NULL DEFAULT '',
    error_msg   TEXT,
    added_at    INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- download_chunks
-- Per-chunk state for an in-progress download.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS download_chunks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id     INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    idx             INTEGER NOT NULL,
    hash            BLOB    NOT NULL,
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
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ed2k_hash   BLOB    NOT NULL UNIQUE,  -- 16 bytes MD4, canonical identifier
    name        TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    path        TEXT    NOT NULL,         -- absolute path of the final file on disk
    mtime       INTEGER NOT NULL,         -- file mtime in Unix seconds (change signal)
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
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id     TEXT    NOT NULL UNIQUE,   -- libp2p PeerId (base58)
    addrs       TEXT    NOT NULL,          -- JSON array of multiaddrs
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    high_id     INTEGER NOT NULL DEFAULT 1 -- 1 = HighID, 0 = LowID
);
