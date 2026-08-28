//! Passive DHT indexer role (SPEC phase 5, role 2).
//!
//! Records the `ADD_PROVIDER` announcements captured by `rucio-net` (via
//! [`NodeEvent::ProviderRecord`](rucio_net::NodeEvent)) into SQLite and exposes
//! a search/admin REST API. Compiled in only with the `web` feature; when built
//! that way it runs by default and is turned off with `--no-index`.
//!
//! When enrichment is enabled, each newly seen hash is resolved to a file name
//! and size by requesting the manifest from the announcing peer, so the search
//! API can match on names rather than just hashes.
//!
//! # Storage model & scaling (read before "fixing" a full provider store)
//!
//! There are **two independent stores** here, and conflating them causes
//! confusion:
//!
//! 1. **SQLite** (this module, `db.rs`) — the *search index*. Every captured
//!    `ADD_PROVIDER` is persisted here and this is what the REST `/search`
//!    serves. It is on disk, pruned by `last_seen`, and **not** bounded by any
//!    Kademlia limit. This scales with disk and is the authoritative index.
//!
//! 2. **Kademlia `MemoryStore`** (in `rucio-net`) — the in-RAM DHT store used
//!    to answer `GET_PROVIDERS` like any DHT server. Captured records are also
//!    re-inserted here (see `task.rs`, the `AddProvider` handler) so the node
//!    keeps serving them. It is bounded by `BehaviourConfig::kad_max_records`
//!    (1M for a bootstrap/indexer role; 100k for a client).
//!
//! When the MemoryStore fills, `add_provider` fails and we log it once at
//! `debug` ("Could not store captured provider record"); the record still
//! lands in SQLite, so **search is unaffected** — only this node's ability to
//! *re-serve* that record over the DHT from RAM is lost. That is not a single
//! point of failure: the file's actual providers still answer `GET_PROVIDERS`
//! themselves; the bootstrap re-serving is a courtesy, not the source of truth.
//!
//! ## If you see "weird things" at scale
//!
//! Symptoms might be: high RAM on the bootstrap, the debug "Could not store
//! captured provider record" line appearing, or the node not re-serving some
//! providers over the DHT. In rough order of cost, the options are:
//!
//! 1. **Raise the cap** — `kad_max_records` is just a RAM ceiling; bump it.
//! 2. **Run several bootstrap/indexer nodes** — they split the keyspace, so no
//!    single node must hold everything in RAM.
//! 3. **Last resort: a disk-backed `RecordStore`** over this SQLite DB instead
//!    of the `MemoryStore`. This was considered and deliberately *not* done
//!    because libp2p's `RecordStore` trait is **synchronous** (so it would
//!    block the swarm reactor on disk I/O, or force `rusqlite`), its
//!    `records()`/`provided()` iterators are walked on every republication
//!    cycle (materialising millions of rows), and it must faithfully replicate
//!    record TTLs, `max_providers_per_key`, and XOR-distance ordering. It is
//!    several days of work with real performance/correctness risk, and is only
//!    worth it if measurement proves options 1–2 insufficient.

mod api;
mod db;
mod web;

// Re-exported so the shared database opener applies the index schema, and the
// stats panel renders the index counters, without reaching into `db` internals.
pub use db::{Db, Stats, apply_schema};

/// Aggregate index counters, for the shared `/stats` panel to display.
pub async fn index_stats(db: &Db) -> Result<Stats> {
    db::stats(db).await
}

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use libp2p::PeerId;
use libp2p::request_response::OutboundRequestId;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use rucio_core::protocol::manifest::{ManifestRequest, ManifestResponse};
use rucio_net::NodeCmd;

/// Runtime options for the indexer role.
pub struct IndexerOpts {
    /// The shared database pool (see [`crate::db`]).
    pub db: crate::db::Db,
    pub retention_days: i64,
    /// Resolve file name/size from each announcing peer's manifest.
    pub enrich: bool,
    /// Channel to the node task, used to request manifests for enrichment.
    pub node_cmd: mpsc::Sender<NodeCmd>,
}

/// A running indexer: owns the DB pool and drives enrichment + retention. Its
/// HTTP routes are mounted onto the shared server (see [`Indexer::api_router`]).
pub struct Indexer {
    db: db::Db,
    enrich: bool,
    retention_days: i64,
    node_cmd: mpsc::Sender<NodeCmd>,
    /// Outstanding manifest requests, mapping the request id back to the hash
    /// being enriched.
    pending: Mutex<HashMap<OutboundRequestId, String>>,
    /// Hashes currently being enriched, to avoid duplicate in-flight requests
    /// when many providers announce the same hash.
    inflight: Mutex<HashSet<String>>,
}

impl Indexer {
    /// Start the daily retention sweep over the shared pool. The schema is
    /// already applied by [`crate::db::open`]; the HTTP API is served by the
    /// shared server (see [`Indexer::api_router`] / [`Indexer::api_doc`]).
    pub async fn start(opts: IndexerOpts) -> Result<Self> {
        let db = opts.db;
        info!(
            retention_days = opts.retention_days,
            enrich = opts.enrich,
            "Indexer enabled"
        );

        // Retention sweep: prune once at startup (interval's first tick is
        // immediate) then once a day.
        let rdb = db.clone();
        let days = opts.retention_days;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(24 * 3600));
            loop {
                tick.tick().await;
                match db::prune(&rdb, days).await {
                    Ok(n) if n > 0 => info!(deleted = n, "Indexer retention sweep"),
                    Ok(_) => {}
                    Err(e) => warn!("Indexer retention sweep failed: {e}"),
                }
            }
        });

        Ok(Self {
            db,
            enrich: opts.enrich,
            retention_days: opts.retention_days,
            node_cmd: opts.node_cmd,
            pending: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashSet::new()),
        })
    }

    /// The indexer's HTTP routes (web UI + JSON API), ready to merge onto the
    /// shared server. `token` guards the admin endpoints (`None` disables them).
    /// `stats_panel` tells the search pages whether to link to the `/stats`
    /// panel served by the sibling stats role.
    pub fn api_router(&self, token: Option<String>, stats_panel: bool) -> axum::Router {
        api::router(api::AppState {
            db: self.db.clone(),
            token,
            retention_days: self.retention_days,
            stats_panel,
        })
    }

    /// The indexer's OpenAPI spec, for the shared server's merged docs.
    pub fn api_doc() -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    /// Record an observed provider announcement, then (if enabled) kick off
    /// enrichment of the hash.
    pub async fn record(&self, hash: &[u8], provider: &PeerId) {
        let hash_hex = hex::encode(hash);
        if let Err(e) = db::upsert(&self.db, &hash_hex, &provider.to_string()).await {
            warn!("Indexer upsert failed: {e}");
            return;
        }
        if self.enrich {
            self.maybe_enrich(hash, &hash_hex, *provider).await;
        }
    }

    /// Request the manifest from `provider` to learn the file's name and size,
    /// unless the hash is already enriched or a request is already in flight.
    async fn maybe_enrich(&self, hash: &[u8], hash_hex: &str, provider: PeerId) {
        let root_hash: [u8; 32] = match hash.try_into() {
            Ok(h) => h,
            Err(_) => return, // not a 32-byte rucio root hash
        };
        match db::has_file(&self.db, hash_hex).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                warn!("has_file failed: {e}");
                return;
            }
        }
        // Claim the hash; bail if another provider already triggered it.
        if !self.inflight.lock().unwrap().insert(hash_hex.to_string()) {
            return;
        }

        let (id_tx, id_rx) = oneshot::channel();
        let cmd = NodeCmd::RequestManifest {
            peer: provider,
            request: ManifestRequest { root_hash },
            id_tx,
        };
        if self.node_cmd.send(cmd).await.is_err() {
            self.inflight.lock().unwrap().remove(hash_hex);
            return;
        }
        match id_rx.await {
            Ok(req_id) => {
                self.pending
                    .lock()
                    .unwrap()
                    .insert(req_id, hash_hex.to_string());
            }
            Err(_) => {
                // Node task dropped the sender (manifest protocol unavailable).
                self.inflight.lock().unwrap().remove(hash_hex);
            }
        }
    }

    /// Handle a manifest response correlated to an enrichment request.
    pub async fn on_manifest(&self, request_id: OutboundRequestId, response: ManifestResponse) {
        let Some(hash_hex) = self.pending.lock().unwrap().remove(&request_id) else {
            return;
        };
        self.inflight.lock().unwrap().remove(&hash_hex);
        if let ManifestResponse::Ok {
            name, total_size, ..
        } = response
        {
            match db::upsert_file(&self.db, &hash_hex, &name, total_size as i64).await {
                Ok(()) => info!(hash = %hash_hex, %name, size = total_size, "Enriched hash"),
                Err(e) => warn!("file upsert failed: {e}"),
            }
        }
    }
}
