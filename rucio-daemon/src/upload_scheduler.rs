//! Upload VIP scheduler: HighID requesters are served before LowID.
//!
//! HighID peers (globally reachable, can re-share) get the bandwidth first; a
//! LowID upload defers while any HighID chunk is being written. Unlike a
//! plain counter this tracks outstanding HighID writes **per peer**, so a
//! duplicate or late completion signal (we emit one on send, on failure, and on
//! a dropped channel) can't drive the count negative — `finished` is a no-op
//! once a peer's count reaches zero.
//!
//! The bandwidth is consumed in the network layer (the transfer codec paces the
//! write), so the daemon can't time the write itself: `started` is called when
//! a HighID chunk is handed off and `finished` when the node task reports the
//! write reached a terminal state (`NodeEvent::ChunkSent`).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use libp2p::PeerId;
use tokio::sync::Notify;

/// The longest a LowID upload waits behind HighID before it is let through
/// regardless — bounds LowID starvation under sustained HighID load while still
/// giving HighID the lion's share.
const MAX_LOWID_WAIT: Duration = Duration::from_secs(10);

pub struct UploadScheduler {
    /// Per-peer count of HighID chunk writes started but not yet completed.
    outstanding: Mutex<HashMap<PeerId, usize>>,
    /// Sum of `outstanding`, read lock-free by `wait_for_lowid_turn`.
    total: AtomicUsize,
    /// Woken when `total` drops to zero so parked LowID waiters re-check.
    lowid_wake: Notify,
}

impl Default for UploadScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl UploadScheduler {
    pub fn new() -> Self {
        Self {
            outstanding: Mutex::new(HashMap::new()),
            total: AtomicUsize::new(0),
            lowid_wake: Notify::new(),
        }
    }

    /// A HighID chunk write is starting (handed off to the network layer).
    pub fn highid_started(&self, peer: PeerId) {
        let mut m = self.outstanding.lock().unwrap();
        *m.entry(peer).or_insert(0) += 1;
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    /// A chunk write to `peer` reached a terminal state. No-op unless we are
    /// tracking an outstanding HighID write for it, so duplicate/late signals
    /// are safe.
    pub fn chunk_finished(&self, peer: PeerId) {
        let mut m = self.outstanding.lock().unwrap();
        if let Some(c) = m.get_mut(&peer)
            && *c > 0
        {
            *c -= 1;
            if *c == 0 {
                m.remove(&peer);
            }
            // Releasing the last HighID write wakes parked LowID waiters.
            if self.total.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.lowid_wake.notify_waiters();
            }
        }
    }

    /// Block a LowID upload until no HighID write is in flight, or until
    /// [`MAX_LOWID_WAIT`] elapses (the floor). Returns immediately when idle.
    pub async fn wait_for_lowid_turn(&self) {
        let deadline = tokio::time::Instant::now() + MAX_LOWID_WAIT;
        loop {
            // Arm before reading so a release between the two isn't missed.
            let notified = self.lowid_wake.notified();
            if self.total.load(Ordering::Acquire) == 0 {
                return;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return; // floor reached — take a turn anyway
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_peer(b: u8) -> PeerId {
        libp2p::identity::Keypair::ed25519_from_bytes([b; 32])
            .unwrap()
            .public()
            .to_peer_id()
    }

    #[tokio::test]
    async fn lowid_returns_immediately_when_no_highid() {
        let s = UploadScheduler::new();
        s.wait_for_lowid_turn().await; // idle → no wait
    }

    #[tokio::test]
    async fn lowid_proceeds_once_highid_finishes() {
        let s = std::sync::Arc::new(UploadScheduler::new());
        let p = a_peer(1);
        s.highid_started(p);
        let s2 = std::sync::Arc::clone(&s);
        let w = tokio::spawn(async move { s2.wait_for_lowid_turn().await });
        s.chunk_finished(p);
        w.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lowid_admitted_by_floor_under_sustained_highid() {
        let s = UploadScheduler::new();
        s.highid_started(a_peer(1)); // never finished
        s.wait_for_lowid_turn().await; // floor lets it through
    }

    #[test]
    fn duplicate_finish_is_a_noop() {
        let s = UploadScheduler::new();
        let p = a_peer(1);
        s.highid_started(p);
        s.chunk_finished(p);
        s.chunk_finished(p); // extra signal must not underflow
        assert_eq!(s.total.load(Ordering::Relaxed), 0);
    }
}
