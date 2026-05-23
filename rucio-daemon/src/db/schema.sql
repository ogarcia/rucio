-- Rucio daemon database schema
-- Pre-stable: drop and recreate the DB file if this changes.
-- All hashes are stored as 32-byte BLOB (BLAKE3).
-- Timestamps are Unix seconds (INTEGER).

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
    added_at    INTEGER NOT NULL           -- Unix seconds
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
