//! Live statistics for *outgoing* transfers (peers downloading from us).
//!
//! The download-side counterpart is [`crate::live_stats`]; this is the upload
//! view. Both rucio chunk serving and the eMule upload server publish here, and
//! a sampler in the main loop turns the cumulative byte counters into a smoothed
//! per-peer rate every second. The `GET /api/v1/uploads` handler and the
//! `UploadProgress` WebSocket event read a snapshot.
//!
//! Lifetime of an entry differs per network:
//! - **eMule** sessions are explicit: [`UploadObserver::upload_started`] returns
//!   a guard that removes the entry on drop, so a row lives exactly as long as
//!   the TCP upload session.
//! - **rucio** chunk serving is fire-and-forget (one short-lived task per
//!   chunk), so there is no session to bound the row. Entries are keyed by
//!   `(peer, file)` — successive chunks accumulate into one row — and the
//!   sampler prunes a rucio row once it has seen no byte activity for
//!   [`IDLE_SECS`].
//!
//! All operations are short and synchronous, so the map is guarded by a
//! `std::sync::Mutex` (never held across an `.await`); this lets the eMule
//! session guard remove its entry from `Drop` without an async context.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use libp2p::PeerId;

use rucio_core::api::uploads::{ActiveUpload, UploadNetwork};

use crate::metrics::SpeedWindow;

/// A rucio upload row is pruned after this many seconds without byte activity.
/// Comfortably longer than the gap between chunks of an active transfer, so it
/// only fires on a genuinely finished or stalled peer.
const IDLE_SECS: u64 = 15;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Identity of an upload row. rucio rows aggregate per `(peer, file)`; eMule
/// rows are per connection (the `SocketAddr` carries the ephemeral port, so two
/// connections never collide).
#[derive(Clone, PartialEq, Eq, Hash)]
struct UploadKey {
    network: UploadNetwork,
    peer: String,
    file_hash: String,
}

struct UploadEntry {
    file_name: Option<String>,
    /// Cumulative bytes sent — written lock-free from the upload hot path,
    /// read by the sampler.
    bytes_sent: Arc<AtomicU64>,
    started_at: u64,
    /// Smoothed rate, refreshed each `sample()`.
    rate_bps: u64,
    // Sampler bookkeeping.
    window: SpeedWindow,
    last_sampled_bytes: u64,
    last_activity: u64,
}

/// Shared registry of active uploads. Stored as `Arc<UploadRegistry>` in
/// `AppState` and cloned into the rucio transfer engine and the eMule upload
/// observer.
pub struct UploadRegistry {
    inner: Mutex<HashMap<UploadKey, UploadEntry>>,
}

impl UploadRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Record bytes served to a rucio peer for a file. Creates the row on first
    /// contact (the caller supplies the resolved `name` only then) and
    /// accumulates into it thereafter. rucio has no session handle, so the row
    /// is touched directly and reaped later by [`Self::sample`] on inactivity.
    pub fn record_rucio(
        &self,
        peer: PeerId,
        file_hash: &[u8; 32],
        name: Option<String>,
        bytes: u64,
    ) {
        let key = UploadKey {
            network: UploadNetwork::Rucio,
            peer: peer.to_base58(),
            file_hash: hex::encode(file_hash),
        };
        let mut map = self.inner.lock().unwrap();
        let now = now_secs();
        let entry = map.entry(key).or_insert_with(|| UploadEntry {
            file_name: name,
            bytes_sent: Arc::new(AtomicU64::new(0)),
            started_at: now,
            rate_bps: 0,
            window: SpeedWindow::new(),
            last_sampled_bytes: 0,
            last_activity: now,
        });
        entry.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        entry.last_activity = now;
    }

    /// Add bytes to an existing rucio row, returning `true` if it existed.
    /// Returns `false` without inserting when the `(peer, file)` row is absent,
    /// letting the caller resolve the file name (one DB hit) before calling
    /// [`Self::record_rucio`] — so the hot path stays DB-free after first chunk.
    pub fn add_bytes_rucio(&self, peer: PeerId, file_hash: &[u8; 32], bytes: u64) -> bool {
        let key = UploadKey {
            network: UploadNetwork::Rucio,
            peer: peer.to_base58(),
            file_hash: hex::encode(file_hash),
        };
        let mut map = self.inner.lock().unwrap();
        match map.get_mut(&key) {
            Some(e) => {
                e.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                e.last_activity = now_secs();
                true
            }
            None => false,
        }
    }

    /// Byte counter (`bytes_sent`) of an existing rucio row, or `None` if the
    /// `(peer, file)` row doesn't exist yet. The net codec increments this
    /// directly as it paces the chunk onto the wire, so the per-peer rate reads
    /// as a flat stream. Bumps `last_activity` so an active transfer isn't reaped.
    pub fn rucio_sink_existing(
        &self,
        peer: PeerId,
        file_hash: &[u8; 32],
    ) -> Option<Arc<AtomicU64>> {
        let key = UploadKey {
            network: UploadNetwork::Rucio,
            peer: peer.to_base58(),
            file_hash: hex::encode(file_hash),
        };
        let mut map = self.inner.lock().unwrap();
        map.get_mut(&key).map(|e| {
            e.last_activity = now_secs();
            Arc::clone(&e.bytes_sent)
        })
    }

    /// Create the rucio `(peer, file)` row (resolving its `name` once on first
    /// contact) and return its `bytes_sent` counter for the net codec to fill.
    pub fn rucio_sink_create(
        &self,
        peer: PeerId,
        file_hash: &[u8; 32],
        name: Option<String>,
    ) -> Arc<AtomicU64> {
        let key = UploadKey {
            network: UploadNetwork::Rucio,
            peer: peer.to_base58(),
            file_hash: hex::encode(file_hash),
        };
        let mut map = self.inner.lock().unwrap();
        let now = now_secs();
        let entry = map.entry(key).or_insert_with(|| UploadEntry {
            file_name: name,
            bytes_sent: Arc::new(AtomicU64::new(0)),
            started_at: now,
            rate_bps: 0,
            window: SpeedWindow::new(),
            last_sampled_bytes: 0,
            last_activity: now,
        });
        Arc::clone(&entry.bytes_sent)
    }

    /// Advance the per-row speed windows and prune finished rucio rows. Call
    /// once per second from the main loop.
    pub fn sample(&self) {
        let now = now_secs();
        let mut map = self.inner.lock().unwrap();
        map.retain(|key, e| {
            let total = e.bytes_sent.load(Ordering::Relaxed);
            let delta = total.saturating_sub(e.last_sampled_bytes);
            e.last_sampled_bytes = total;
            if delta > 0 {
                e.last_activity = now;
            }
            e.window.add(delta);
            e.rate_bps = e.window.tick();
            // eMule rows are removed by their session guard; only rucio rows
            // (fire-and-forget, no guard) are pruned on inactivity here.
            !(key.network == UploadNetwork::Rucio
                && now.saturating_sub(e.last_activity) >= IDLE_SECS)
        });
    }

    /// Snapshot of all active uploads, sorted fastest-first then largest-first.
    pub fn snapshot(&self) -> Vec<ActiveUpload> {
        let map = self.inner.lock().unwrap();
        let mut out: Vec<ActiveUpload> = map
            .iter()
            .map(|(key, e)| ActiveUpload {
                network: key.network,
                peer: key.peer.clone(),
                file_hash: key.file_hash.clone(),
                file_name: e.file_name.clone(),
                bytes_sent: e.bytes_sent.load(Ordering::Relaxed),
                rate_bps: e.rate_bps,
                started_at: e.started_at,
            })
            .collect();
        out.sort_by(|a, b| {
            b.rate_bps
                .cmp(&a.rate_bps)
                .then_with(|| b.bytes_sent.cmp(&a.bytes_sent))
        });
        out
    }

    /// Number of distinct files currently being served (to one or more peers) —
    /// the upload-side analogue of one active download per file, so it counts
    /// the same way regardless of how many peer connections each file has.
    pub fn active_file_count(&self) -> usize {
        let map = self.inner.lock().unwrap();
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for key in map.keys() {
            seen.insert(key.file_hash.as_str());
        }
        seen.len()
    }

    /// Number of active upload connections right now: one per peer/file transfer
    /// row (a peer pulling two files counts twice; two peers pulling one file
    /// count twice). The connection-level counterpart to [`Self::active_file_count`].
    pub fn active_connection_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Insert (or reuse) an eMule upload row and return its byte counter. The
    /// row is removed when the returned key is dropped via [`Self::remove`].
    #[cfg(feature = "emule-compat")]
    fn insert_emule(
        &self,
        peer: std::net::SocketAddr,
        hash: [u8; 16],
        name: &str,
    ) -> (UploadKey, Arc<AtomicU64>) {
        let key = UploadKey {
            network: UploadNetwork::Emule,
            peer: peer.to_string(),
            file_hash: hex::encode(hash),
        };
        let mut map = self.inner.lock().unwrap();
        let now = now_secs();
        let entry = map.entry(key.clone()).or_insert_with(|| UploadEntry {
            file_name: Some(name.to_string()),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            started_at: now,
            rate_bps: 0,
            window: SpeedWindow::new(),
            last_sampled_bytes: 0,
            last_activity: now,
        });
        (key, Arc::clone(&entry.bytes_sent))
    }

    #[cfg(feature = "emule-compat")]
    fn remove(&self, key: &UploadKey) {
        self.inner.lock().unwrap().remove(key);
    }
}

impl Default for UploadRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── eMule observer bridge ───────────────────────────────────────────────────

#[cfg(feature = "emule-compat")]
mod emule_bridge {
    use super::{Arc, AtomicU64, Ordering, UploadKey, UploadRegistry};
    use rucio_emule::transfer::{UploadObserver, UploadSession};
    use std::net::SocketAddr;

    /// Bridges the [`UploadObserver`] trait from `rucio-emule` into the registry
    /// so the eMule upload server can publish without depending on the daemon.
    /// Carries an `Arc<UploadRegistry>` so each session guard can hold a strong
    /// reference and deregister itself on drop.
    #[derive(Clone)]
    pub struct EmuleUploadObserver(pub Arc<UploadRegistry>);

    impl UploadObserver for EmuleUploadObserver {
        fn upload_started(
            &self,
            peer: SocketAddr,
            hash: [u8; 16],
            name: &str,
        ) -> Box<dyn UploadSession> {
            let (key, bytes) = self.0.insert_emule(peer, hash, name);
            Box::new(EmuleUploadGuard {
                registry: Arc::clone(&self.0),
                key,
                bytes,
            })
        }
    }

    /// Per-session handle held by the eMule upload task. `add_bytes` feeds the
    /// registry; `Drop` removes the row when the session ends.
    struct EmuleUploadGuard {
        registry: Arc<UploadRegistry>,
        key: UploadKey,
        bytes: Arc<AtomicU64>,
    }

    impl UploadSession for EmuleUploadGuard {
        fn add_bytes(&self, bytes: u64) {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    impl Drop for EmuleUploadGuard {
        fn drop(&mut self) {
            self.registry.remove(&self.key);
        }
    }
}

#[cfg(feature = "emule-compat")]
pub use emule_bridge::EmuleUploadObserver;

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn a_peer(byte: u8) -> PeerId {
        // Deterministic distinct PeerIds without RNG: hash a fixed key seed.
        let kp = libp2p::identity::Keypair::ed25519_from_bytes([byte; 32]).unwrap();
        kp.public().to_peer_id()
    }

    #[test]
    fn add_bytes_rucio_is_false_until_the_row_exists() {
        let reg = UploadRegistry::new();
        let peer = a_peer(1);
        let hash = [7u8; 32];
        // No row yet — caller must resolve the name and insert.
        assert!(!reg.add_bytes_rucio(peer, &hash, 100));
        reg.record_rucio(peer, &hash, Some("file.bin".into()), 100);
        // Now the row exists and further chunks accumulate without a DB hit.
        assert!(reg.add_bytes_rucio(peer, &hash, 50));

        let snap = reg.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].bytes_sent, 150);
        assert_eq!(snap[0].file_name.as_deref(), Some("file.bin"));
        assert_eq!(snap[0].network, UploadNetwork::Rucio);
    }

    #[test]
    fn distinct_peers_and_files_get_distinct_rows() {
        let reg = UploadRegistry::new();
        reg.record_rucio(a_peer(1), &[1u8; 32], None, 10);
        reg.record_rucio(a_peer(2), &[1u8; 32], None, 20);
        reg.record_rucio(a_peer(1), &[2u8; 32], None, 30);
        assert_eq!(reg.snapshot().len(), 3);
    }

    #[test]
    fn sample_turns_activity_into_a_rate_then_decays() {
        let reg = UploadRegistry::new();
        let peer = a_peer(1);
        let hash = [9u8; 32];
        reg.record_rucio(peer, &hash, None, 5_000);
        // First sample seals the second's bytes into the window.
        reg.sample();
        let r1 = reg.snapshot()[0].rate_bps;
        assert!(r1 > 0, "rate should be positive after activity, got {r1}");
        // No further bytes: the rolling average decays as empty buckets seal in.
        for _ in 0..6 {
            reg.sample();
        }
        assert_eq!(reg.snapshot()[0].rate_bps, 0);
    }

    #[test]
    fn snapshot_is_sorted_fastest_first() {
        let reg = UploadRegistry::new();
        reg.record_rucio(a_peer(1), &[1u8; 32], None, 1_000);
        reg.record_rucio(a_peer(2), &[2u8; 32], None, 9_000);
        reg.sample();
        let snap = reg.snapshot();
        // Peer 2 moved more bytes in the window → higher rate → listed first.
        assert!(snap[0].rate_bps >= snap[1].rate_bps);
        assert_eq!(snap[0].bytes_sent, 9_000);
    }
}
