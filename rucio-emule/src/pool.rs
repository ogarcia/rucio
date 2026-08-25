//! A4AF-lite: a shared, per-peer eMule connection pool.
//!
//! Multiple concurrent downloads that share the same sources (e.g. 10 episodes)
//! all funnel through ONE warm TCP connection per peer. When a download finishes
//! its slices from a fast peer, the still-slot-holding connection is handed to
//! the next download's worker, which switches it to its own file
//! ([`Session::switch_file`]) and is granted an immediate `OP_ACCEPTUPLOADREQ`
//! (eMule's A4AF path) — no disconnect, no 29-minute reask cooldown, no ban.
//!
//! Access to a peer is serialized by a per-peer async mutex, so exactly one
//! worker (across all downloads) uses a peer at a time. A warm connection is only
//! ever kept once a slot has been granted, so reusing/switching it is always the
//! instant-accept path.

use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use tracing::debug;

use crate::Ed2kHash;
use crate::transfer::{DownloadEvent, DownloadOptions, Session};

/// One warm [`Session`] per peer, shared across all downloads.
pub struct PeerConnPool {
    peers: Mutex<HashMap<SocketAddrV4, Arc<PeerSlot>>>,
    /// How long a parked warm connection may sit unused before it is closed — a
    /// warm connection pins the peer's upload slot, so we must not squat it once
    /// no download needs the peer any more.
    idle_timeout: Duration,
}

/// Per-peer serialization point. The tokio mutex guarantees a single user at a
/// time; `None` = cold (no connection parked), `Some` = a warm connection.
struct PeerSlot {
    conn: Arc<tokio::sync::Mutex<Option<PooledConn>>>,
}

struct PooledConn {
    session: Session,
    /// The file the warm session currently holds a slot for; lets a reuse for the
    /// same file skip a redundant `switch_file`.
    current_hash: Ed2kHash,
    /// When the connection was last returned to the pool (drives idle eviction).
    last_used: Instant,
}

/// Exclusive lease on one peer's connection. Held while a worker uses the peer;
/// dropping it returns the (still warm) connection to the pool for the next
/// download to pick up.
pub struct PeerLease {
    peer: SocketAddrV4,
    idle_timeout: Duration,
    guard: tokio::sync::OwnedMutexGuard<Option<PooledConn>>,
}

impl PeerConnPool {
    pub fn new(idle_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            peers: Mutex::new(HashMap::new()),
            idle_timeout,
        })
    }

    fn slot(&self, peer: SocketAddrV4) -> Arc<PeerSlot> {
        self.peers
            .lock()
            .unwrap()
            .entry(peer)
            .or_insert_with(|| {
                Arc::new(PeerSlot {
                    conn: Arc::new(tokio::sync::Mutex::new(None)),
                })
            })
            .clone()
    }

    /// Non-blocking exclusive checkout. Returns `None` when another worker (in any
    /// download) currently holds this peer — the caller then skips it and tries a
    /// different source, exactly as it already skips sources still in cooldown.
    pub fn try_checkout(&self, peer: SocketAddrV4) -> Option<PeerLease> {
        let slot = self.slot(peer);
        Arc::clone(&slot.conn)
            .try_lock_owned()
            .ok()
            .map(|guard| PeerLease {
                peer,
                idle_timeout: self.idle_timeout,
                guard,
            })
    }

    /// Blocking (FIFO) checkout. Kept for completeness; the worker loop uses
    /// [`Self::try_checkout`] so it never blocks on a busy peer.
    pub async fn checkout(&self, peer: SocketAddrV4) -> PeerLease {
        let slot = self.slot(peer);
        let guard = Arc::clone(&slot.conn).lock_owned().await;
        PeerLease {
            peer,
            idle_timeout: self.idle_timeout,
            guard,
        }
    }

    /// Close warm connections idle longer than `idle_timeout` (good-citizen: stop
    /// squatting a peer's upload slot) and prune cold entries. Peers currently in
    /// use (lease held) are skipped — a successful `try_lock` proves no lease is
    /// active, so removing the entry can never orphan one.
    pub fn reap_idle(&self) {
        let mut peers = self.peers.lock().unwrap();
        let idle = self.idle_timeout;
        peers.retain(|_peer, slot| match slot.conn.try_lock() {
            Ok(mut guard) => match guard.as_ref() {
                // Warm but stale → close (drops Session, logs summary) and prune.
                Some(c) if c.last_used.elapsed() > idle => {
                    *guard = None;
                    false
                }
                Some(_) => true, // warm and recently used → keep
                None => false,   // cold → prune
            },
            Err(_) => true, // an active lease holds it → keep
        });
    }

    #[cfg(test)]
    fn peer_count(&self) -> usize {
        self.peers.lock().unwrap().len()
    }
}

/// Spawn the background reaper that periodically closes idle warm connections.
pub fn spawn_reaper(pool: Arc<PeerConnPool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        loop {
            tick.tick().await;
            pool.reap_idle();
        }
    })
}

impl PeerLease {
    /// Ready a session for `opts.hash`: reuse the warm connection (switching the
    /// file via A4AF when it currently holds a different one) or, when cold /
    /// stale / a warm switch fails, establish a fresh connection. On `Ok`, call
    /// [`Self::session`] to drive the transfer.
    pub async fn acquire_for_file<F>(
        &mut self,
        opts: &DownloadOptions,
        on_event: &mut F,
    ) -> Result<()>
    where
        F: FnMut(DownloadEvent),
    {
        enum Decide {
            ReuseAsIs,
            Switch,
            Reconnect,
        }
        let decision = match self.guard.as_ref() {
            // A warm connection idle past the timeout: the peer has likely dropped
            // our idle slot, so reconnect rather than trust it.
            Some(c) if c.last_used.elapsed() > self.idle_timeout => Decide::Reconnect,
            Some(c) if c.current_hash.as_bytes() == opts.hash.as_bytes() => Decide::ReuseAsIs,
            Some(_) => Decide::Switch,
            None => Decide::Reconnect,
        };

        match decision {
            Decide::ReuseAsIs => Ok(()),
            Decide::Switch => {
                let res = {
                    let c = self.guard.as_mut().unwrap();
                    c.session
                        .switch_file(
                            opts.hash,
                            opts.file_size,
                            opts.max_queue_waits,
                            &mut *on_event,
                        )
                        .await
                };
                match res {
                    Ok(()) => {
                        let c = self.guard.as_mut().unwrap();
                        c.current_hash = opts.hash;
                        c.last_used = Instant::now();
                        Ok(())
                    }
                    Err(e) => {
                        debug!(peer = %self.peer, error = %e, "warm A4AF switch failed — reconnecting");
                        *self.guard = None;
                        self.reconnect(opts, on_event).await
                    }
                }
            }
            Decide::Reconnect => {
                *self.guard = None;
                self.reconnect(opts, on_event).await
            }
        }
    }

    async fn reconnect<F>(&mut self, opts: &DownloadOptions, on_event: &mut F) -> Result<()>
    where
        F: FnMut(DownloadEvent),
    {
        let session = Session::connect(self.peer, opts, on_event).await?;
        *self.guard = Some(PooledConn {
            session,
            current_hash: opts.hash,
            last_used: Instant::now(),
        });
        Ok(())
    }

    /// `&mut` to the ready session. Panics if called before a successful
    /// [`Self::acquire_for_file`] (a cold lease has no session).
    pub fn session(&mut self) -> &mut Session {
        &mut self
            .guard
            .as_mut()
            .expect("session() on a cold lease; call acquire_for_file first")
            .session
    }

    /// Mark the connection dead (a real transport error). The next checkout will
    /// reconnect from scratch instead of trusting the broken stream.
    pub fn discard(&mut self) {
        *self.guard = None;
    }

    /// Whether a warm connection is currently parked in this lease.
    pub fn is_warm(&self) -> bool {
        self.guard.is_some()
    }
}

impl Drop for PeerLease {
    fn drop(&mut self) {
        // Stamp the idle clock; the connection stays warm & parked for the next
        // download. The tokio guard releases as this drops, handing the peer over.
        if let Some(c) = self.guard.as_mut() {
            c.last_used = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::mock;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    use tokio::net::TcpListener;

    fn opts_for(hash: [u8; 16], size: u64) -> DownloadOptions {
        DownloadOptions {
            hash: Ed2kHash::from_bytes(hash),
            file_size: size,
            op_timeout: Duration::from_secs(5),
            max_queue_waits: 2,
            peer_hash: None, // mock speaks plain; no obfuscation
            ..Default::default()
        }
    }

    fn sample(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed))
            .collect()
    }

    async fn download_to_vec(lease: &mut PeerLease, size: u64) -> Vec<u8> {
        let mut out = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let n = lease
            .session()
            .download_range(0, size, None, &mut out, &mut |_| {}, || false)
            .await
            .unwrap();
        assert_eq!(n, size);
        out.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut got = Vec::new();
        out.read_to_end(&mut got).await.unwrap();
        got
    }

    /// try_checkout is exclusive per peer: a second checkout while a lease is held
    /// returns None, and succeeds again once the lease drops. No I/O involved.
    #[tokio::test]
    async fn try_checkout_is_exclusive_per_peer() {
        let pool = PeerConnPool::new(Duration::from_secs(60));
        let peer: SocketAddrV4 = "127.0.0.1:1234".parse().unwrap();
        let lease = pool.try_checkout(peer).expect("first checkout");
        assert!(pool.try_checkout(peer).is_none(), "held peer is exclusive");
        drop(lease);
        assert!(
            pool.try_checkout(peer).is_some(),
            "freed peer checks out again"
        );
    }

    /// The heart of A4AF: two downloads share one peer; the second reuses the WARM
    /// connection (switch_file), so the peer is connected to exactly once.
    #[tokio::test]
    async fn pool_reuses_warm_connection_across_downloads() {
        let (hash_a, hash_b) = ([0xa1u8; 16], [0xb2u8; 16]);
        let data_a = sample(40_000, 3);
        let data_b = sample(60_000, 7);
        let mut files = HashMap::new();
        files.insert(hash_a, data_a.clone());
        files.insert(hash_b, data_b.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let accepts = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(mock::serve(listener, files, 1, accepts.clone()));

        let pool = PeerConnPool::new(Duration::from_secs(60));

        // Download A — cold, so the pool connects.
        let mut lease = pool.try_checkout(peer).unwrap();
        lease
            .acquire_for_file(&opts_for(hash_a, data_a.len() as u64), &mut |_| {})
            .await
            .unwrap();
        assert_eq!(
            download_to_vec(&mut lease, data_a.len() as u64).await,
            data_a
        );
        drop(lease); // returns the warm connection to the pool

        // Download B — warm, so the pool switches the file on the same connection.
        let mut lease2 = pool.try_checkout(peer).unwrap();
        assert!(
            lease2.is_warm(),
            "connection was kept warm after download A"
        );
        lease2
            .acquire_for_file(&opts_for(hash_b, data_b.len() as u64), &mut |_| {})
            .await
            .unwrap();
        assert_eq!(
            download_to_vec(&mut lease2, data_b.len() as u64).await,
            data_b
        );
        drop(lease2);

        assert_eq!(
            accepts.load(Ordering::Relaxed),
            1,
            "both downloads served over a single connection (A4AF), no reconnect"
        );
        drop(pool); // close the warm connection so the mock's accept loop ends
        let _ = server.await;
    }

    /// A discarded (broken) session forces a fresh connection on the next acquire.
    #[tokio::test]
    async fn broken_session_is_discarded_and_reconnected() {
        let hash_a = [0xa1u8; 16];
        let data_a = sample(30_000, 5);
        let mut files = HashMap::new();
        files.insert(hash_a, data_a.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let accepts = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(mock::serve(listener, files, 2, accepts.clone()));

        let pool = PeerConnPool::new(Duration::from_secs(60));
        let opts = opts_for(hash_a, data_a.len() as u64);

        let mut lease = pool.try_checkout(peer).unwrap();
        lease.acquire_for_file(&opts, &mut |_| {}).await.unwrap(); // accept #1
        lease.discard(); // simulate a transport failure
        assert!(!lease.is_warm());
        lease.acquire_for_file(&opts, &mut |_| {}).await.unwrap(); // reconnect → accept #2
        assert!(lease.is_warm());
        drop(lease);

        assert_eq!(
            accepts.load(Ordering::Relaxed),
            2,
            "discard forced a reconnect"
        );
        drop(pool);
        let _ = server.await;
    }

    /// The reaper closes an idle warm connection and prunes its entry.
    #[tokio::test]
    async fn reaper_closes_idle_warm_connection() {
        let hash_a = [0xa1u8; 16];
        let data_a = sample(20_000, 9);
        let mut files = HashMap::new();
        files.insert(hash_a, data_a.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let accepts = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(mock::serve(listener, files, 1, accepts.clone()));

        // Zero idle timeout: any parked connection is immediately reapable.
        let pool = PeerConnPool::new(Duration::from_secs(0));
        let mut lease = pool.try_checkout(peer).unwrap();
        lease
            .acquire_for_file(&opts_for(hash_a, data_a.len() as u64), &mut |_| {})
            .await
            .unwrap();
        drop(lease); // park warm
        assert_eq!(pool.peer_count(), 1);

        pool.reap_idle();
        assert_eq!(
            pool.peer_count(),
            0,
            "idle warm connection reaped and pruned"
        );
        let _ = server.await;
    }
}
