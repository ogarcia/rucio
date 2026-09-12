//! Privacy-preserving gauge of interest in our published pin-set.
//!
//! When a peer subscribes to us it pulls our pin-set periodically (every few
//! minutes, `pinset_reconcile_tick`). We cannot — and by design do not want to —
//! tell the user *who* follows them: the pin-set request is answered statelessly
//! and the requester is anonymous. But we can honestly surface *how many*
//! distinct peers have fetched our pin-set recently, as a "someone out there
//! cares about your pins" signal.
//!
//! This registry is entirely in memory (no DB, nothing on disk): peer ids are
//! held only to de-duplicate the count and never leave the node — only the
//! aggregate number is exposed. Entries age out after [`PROBE_WINDOW_SECS`], so
//! the count means "peers that probed within the window" and the map stays
//! bounded to the active set. Pruning is opportunistic (on record), never a
//! periodic reaper.
//!
//! It resets on restart, which is fine: a genuine subscriber re-probes on its
//! next reconcile tick, so the count refills within one cycle.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use libp2p::PeerId;

/// A peer that stops probing ages out of the count after this window. The
/// per-subscriber probe interval is ~3 min (`pinset_reconcile_tick`), so a day
/// comfortably survives restarts, laptop sleep and brief disconnects while
/// still meaning "recently interested".
pub const PROBE_WINDOW_SECS: u64 = 24 * 3600;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// In-memory set of `peer -> last probe time`. Stored as `Arc<PinsetProbes>` in
/// `AppState` and touched by the pin-set request handler.
pub struct PinsetProbes {
    inner: Mutex<HashMap<PeerId, u64>>,
}

impl PinsetProbes {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Record that `peer` fetched our pin-set. Prunes entries older than the
    /// window in the same pass, so the map never grows past the active set
    /// without needing a separate reaper.
    pub fn record(&self, peer: PeerId) {
        let now = now_secs();
        let mut map = self.inner.lock().unwrap();
        map.insert(peer, now);
        map.retain(|_, &mut last| now.saturating_sub(last) < PROBE_WINDOW_SECS);
    }

    /// Distinct peers that probed our pin-set within the window.
    pub fn count(&self) -> usize {
        let now = now_secs();
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|&&last| now.saturating_sub(last) < PROBE_WINDOW_SECS)
            .count()
    }
}

impl Default for PinsetProbes {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_peer(byte: u8) -> PeerId {
        let kp = libp2p::identity::Keypair::ed25519_from_bytes([byte; 32]).unwrap();
        kp.public().to_peer_id()
    }

    #[test]
    fn counts_distinct_peers_and_dedups_repeat_probes() {
        let probes = PinsetProbes::new();
        probes.record(a_peer(1));
        probes.record(a_peer(2));
        // The same peer probing again does not inflate the count.
        probes.record(a_peer(1));
        assert_eq!(probes.count(), 2);
    }

    #[test]
    fn empty_registry_counts_zero() {
        assert_eq!(PinsetProbes::new().count(), 0);
    }

    #[test]
    fn stale_entries_age_out_of_the_count() {
        let probes = PinsetProbes::new();
        // Inject a peer whose last probe is older than the window.
        {
            let mut map = probes.inner.lock().unwrap();
            map.insert(a_peer(1), now_secs().saturating_sub(PROBE_WINDOW_SECS + 60));
        }
        assert_eq!(probes.count(), 0);
        // A fresh probe from another peer is counted; recording also prunes the
        // stale entry, so the count stays at exactly one.
        probes.record(a_peer(2));
        assert_eq!(probes.count(), 1);
        assert_eq!(probes.inner.lock().unwrap().len(), 1);
    }
}
