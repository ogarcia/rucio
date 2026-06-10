//! Cooperative pinning: serving our pin-set over `/rucio/pinset/1.0.0`.
//!
//! Our published pin-set is the set of manually pinned hashes we currently
//! *have* (i.e. that are present as shares). We don't advertise a pin whose
//! content we couldn't actually serve. The exchange is authenticated by the
//! libp2p connection, so the response carries no signature.

use libp2p::PeerId;
use rucio_core::protocol::pinset::{PinsetEntry, PinsetRequest, PinsetResponse};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::db;
use crate::node::messages::NodeCmd;

/// Build our pin-set from the `pins` table, including only pins whose content is
/// present as a share (so peers don't try to fetch what we can't serve).
pub async fn build_pinset(db: &db::Db) -> PinsetResponse {
    let pins = match db::pins::list(db).await {
        Ok(p) => p,
        Err(e) => return PinsetResponse::Error(e.to_string()),
    };
    let mut entries: Vec<PinsetEntry> = Vec::with_capacity(pins.len());
    for pin in pins {
        let Ok(hash) = <[u8; 32]>::try_from(pin.root_hash.as_slice()) else {
            continue;
        };
        // Only offer pins we actually hold (present as a share).
        if let Ok(Some(share)) = db::shares::get_by_hash(db, &hash).await {
            entries.push(PinsetEntry {
                root_hash: hash,
                size: share.size as u64,
                name: share.name,
            });
        }
    }
    let version = PinsetResponse::fingerprint(&entries);
    PinsetResponse::Ok { version, entries }
}

/// Answer an inbound pin-set request: build our pin-set and respond. Spawned so
/// the node task's event loop isn't blocked on the DB.
pub fn serve_pinset(db: &db::Db, cmd_tx: &Sender<NodeCmd>, channel_id: u64) {
    let db = db.clone();
    let cmd_tx = cmd_tx.clone();
    tokio::spawn(async move {
        let response = build_pinset(&db).await;
        let _ = cmd_tx
            .send(NodeCmd::RespondPinset {
                channel_id,
                response,
            })
            .await;
    });
}

// ---------------------------------------------------------------------------
// Reconcile: pull subscribed peers' pin-sets and mirror them within quota.
// ---------------------------------------------------------------------------

/// A wanted mirror entry we don't hold yet — the reconcile asks the main loop
/// to start a download for each, routed to `pin_dir` (see `transfer.rs`).
#[derive(Debug, Clone)]
pub struct FetchItem {
    pub root_hash: [u8; 32],
    pub name: String,
}

/// Ask every subscribed peer for its current pin-set. Responses arrive
/// asynchronously as `NodeEvent::PinsetReceived` and are handled by
/// [`on_pinset_received`]. Best-effort: peers we can't reach are simply retried
/// on the next reconcile tick.
pub async fn request_all_pinsets(db: &db::Db, cmd_tx: &Sender<NodeCmd>) {
    let subs = match db::pin_subscriptions::list(db).await {
        Ok(s) => s,
        Err(e) => {
            warn!("reconcile: cannot list subscriptions: {e}");
            return;
        }
    };
    for sub in subs {
        let Ok(peer) = sub.peer_id.parse::<PeerId>() else {
            warn!(peer = %sub.peer_id, "reconcile: invalid peer id in subscription");
            continue;
        };
        // Warm the routing table so `send_request` can dial peers we're not
        // connected to (no-op for already-connected LAN peers).
        let _ = cmd_tx.send(NodeCmd::DiscoverPeer { peer }).await;
        // We correlate the response by its `peer`, not by request id, so the
        // returned id is discarded.
        let (id_tx, _id_rx) = oneshot::channel();
        let _ = cmd_tx
            .send(NodeCmd::RequestPinset {
                peer,
                request: PinsetRequest,
                id_tx,
            })
            .await;
    }
}

/// Apply a peer's pin-set: select within quota, persist the mirror set, record
/// the synced version, and return the wanted entries we still need to fetch.
///
/// Selection is smallest-first so one huge pin can't crowd out many useful
/// small ones; entries that don't fit the quota are kept on record as
/// `skipped` (visible to the user) rather than silently dropped.
pub async fn on_pinset_received(
    db: &db::Db,
    peer: PeerId,
    response: PinsetResponse,
    now: u64,
) -> Vec<FetchItem> {
    let peer_str = peer.to_string();
    let sub = match db::pin_subscriptions::get(db, &peer_str).await {
        Ok(Some(s)) => s,
        // Unsubscribed between the request and the response — nothing to do.
        Ok(None) => return Vec::new(),
        Err(e) => {
            warn!(peer = %peer_str, "reconcile: subscription lookup failed: {e}");
            return Vec::new();
        }
    };

    let (version, mut entries) = match response {
        PinsetResponse::Ok { version, entries } => (version, entries),
        PinsetResponse::Error(e) => {
            warn!(peer = %peer_str, "reconcile: peer returned a pin-set error: {e}");
            return Vec::new();
        }
    };

    // Unchanged since last sync: just refresh the synced-at timestamp.
    if version as i64 == sub.last_version {
        let _ = db::pin_subscriptions::set_synced(db, &peer_str, version as i64, now).await;
        return Vec::new();
    }

    // Greedy smallest-first selection under the quota.
    entries.sort_by_key(|e| e.size);
    let quota = sub.quota_bytes.max(0) as u64;
    let mut used: u64 = 0;
    let mut mirror = Vec::with_capacity(entries.len());
    for e in &entries {
        let fits = quota > 0 && used.saturating_add(e.size) <= quota;
        let state = if fits {
            used += e.size;
            db::mirror_pins::STATE_WANTED
        } else {
            db::mirror_pins::STATE_SKIPPED
        };
        mirror.push(db::mirror_pins::MirrorEntry {
            root_hash: e.root_hash,
            name: Some(e.name.clone()),
            size: e.size as i64,
            state: state.to_string(),
        });
    }

    if let Err(e) = db::mirror_pins::set_for_peer(db, &peer_str, &mirror, now).await {
        warn!(peer = %peer_str, "reconcile: persisting the mirror set failed: {e}");
        return Vec::new();
    }
    let _ = db::pin_subscriptions::set_synced(db, &peer_str, version as i64, now).await;
    info!(
        peer = %peer_str,
        wanted = mirror.iter().filter(|m| m.state == db::mirror_pins::STATE_WANTED).count(),
        skipped = mirror.iter().filter(|m| m.state == db::mirror_pins::STATE_SKIPPED).count(),
        "reconcile: applied pin-set"
    );

    // Fetch the wanted entries we don't already hold (present as a share, e.g.
    // from a prior mirror or our own download).
    let mut fetch = Vec::new();
    for (e, m) in entries.iter().zip(mirror.iter()) {
        if m.state != db::mirror_pins::STATE_WANTED {
            continue;
        }
        match db::shares::get_by_hash(db, &e.root_hash).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                // We don't hold it, so this copy will exist only because we
                // mirror it: mark it owned so eviction may later delete it
                // (the user's own content is never marked, never evicted).
                if let Err(err) = db::mirror_owned::mark(db, &e.root_hash, now).await {
                    warn!("reconcile: marking mirror-owned failed: {err}");
                }
                fetch.push(FetchItem {
                    root_hash: e.root_hash,
                    name: e.name.clone(),
                });
            }
            Err(err) => warn!("reconcile: share lookup failed: {err}"),
        }
    }
    fetch
}

/// Evict mirror content nobody wants any more. A hash is evicted only when it is
/// mirror-owned (we fetched it solely to mirror), is neither a manual pin nor
/// wanted by any subscription, and its file lives under `pin_dir`. That triple
/// guard means we never delete the user's own downloads or shares. Returns how
/// many hashes were evicted.
pub async fn evict_unwanted(
    db: &db::Db,
    cmd_tx: &Sender<NodeCmd>,
    pin_dir: &std::path::Path,
) -> usize {
    let owned = match db::mirror_owned::list(db).await {
        Ok(o) => o,
        Err(e) => {
            warn!("eviction: cannot list mirror-owned hashes: {e}");
            return 0;
        }
    };
    let mut evicted = 0usize;
    for hash in owned {
        // Still wanted (manual pin or some subscription): keep it.
        let pinned = db::pins::exists(db, &hash).await.unwrap_or(false);
        let wanted = db::mirror_pins::is_wanted(db, &hash).await.unwrap_or(false);
        if pinned || wanted {
            continue;
        }

        // No longer wanted. Find where its copy lives.
        match db::shares::get_by_hash(db, &hash).await {
            Ok(Some(share)) => {
                let path = std::path::Path::new(&share.path);
                if path.starts_with(pin_dir) {
                    // Mirror copy under pin_dir — safe to delete.
                    if let Err(e) = tokio::fs::remove_file(path).await
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        warn!(path = %share.path, "eviction: could not delete file: {e}");
                    }
                    let _ = db::shares::delete_by_hash(db, &hash).await;
                    let _ = cmd_tx.send(NodeCmd::StopProviding(hash.to_vec())).await;
                    info!(hash = %hex::encode(hash), "Evicted mirror content (no longer wanted)");
                    evicted += 1;
                } else {
                    // Outside pin_dir: the user keeps it elsewhere (e.g. a
                    // manual download of the same hash). Don't touch the file
                    // or the share — just drop our ownership claim.
                    info!(hash = %hex::encode(hash), "Mirror no longer wanted but file is outside pin_dir — leaving it, dropping ownership");
                }
            }
            // No local copy (fetch never completed / already gone): nothing to
            // delete; just drop the ownership row below.
            Ok(None) => {}
            Err(e) => {
                warn!("eviction: share lookup failed: {e}");
                continue; // keep the owned row; retry next sweep
            }
        }
        let _ = db::mirror_owned::unmark(db, &hash).await;
    }
    if evicted > 0 {
        info!(count = evicted, "eviction sweep complete");
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::shares::NewSharedFile;

    async fn test_db() -> (db::Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        crate::db::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    #[tokio::test]
    async fn pinset_only_includes_present_pins() {
        let (db, _dir) = test_db().await;
        let have = [1u8; 32];
        let absent = [2u8; 32];

        // A shared file we'll pin.
        db::shares::insert(
            &db,
            NewSharedFile {
                root_hash: &have,
                name: "kept.bin",
                size: 4096,
                mime_type: None,
                path: "/tmp/kept.bin",
                chunk_size: 4096,
                added_at: 1,
                mtime: 0,
                chunks: &[(0, [9u8; 32], 4096)],
            },
        )
        .await
        .unwrap();

        // Pin both the present share and an absent hash.
        db::pins::add(&db, &have, 10).await.unwrap();
        db::pins::add(&db, &absent, 11).await.unwrap();

        let resp = build_pinset(&db).await;
        let PinsetResponse::Ok { version, entries } = resp else {
            panic!("expected Ok");
        };
        // Only the present pin is offered.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].root_hash, have);
        assert_eq!(entries[0].size, 4096);
        assert_eq!(entries[0].name, "kept.bin");
        // Version is the fingerprint of those entries (stable, recomputable).
        assert_eq!(version, PinsetResponse::fingerprint(&entries));
    }

    fn entry(hash: [u8; 32], size: u64, name: &str) -> PinsetEntry {
        PinsetEntry {
            root_hash: hash,
            size,
            name: name.into(),
        }
    }

    #[tokio::test]
    async fn reconcile_selects_within_quota_and_fetches_missing() {
        let (db, _dir) = test_db().await;
        let peer = PeerId::random();
        let peer_str = peer.to_string();
        // Quota fits the two smallest (100 + 200) but not the 5000 one.
        db::pin_subscriptions::upsert(&db, &peer_str, 400, 1)
            .await
            .unwrap();

        let small = [10u8; 32];
        let mid = [20u8; 32];
        let big = [30u8; 32];
        let resp = PinsetResponse::Ok {
            version: 42,
            entries: vec![
                entry(big, 5000, "big.bin"),
                entry(small, 100, "small.bin"),
                entry(mid, 200, "mid.bin"),
            ],
        };

        let fetch = on_pinset_received(&db, peer, resp, 100).await;

        // Smallest-first within quota: small + mid wanted, big skipped.
        assert!(db::mirror_pins::is_wanted(&db, &small).await.unwrap());
        assert!(db::mirror_pins::is_wanted(&db, &mid).await.unwrap());
        assert!(!db::mirror_pins::is_wanted(&db, &big).await.unwrap());
        assert_eq!(
            db::mirror_pins::wanted_bytes_for_peer(&db, &peer_str)
                .await
                .unwrap(),
            300
        );
        // Both wanted entries are missing locally, so both are fetched.
        let mut got: Vec<[u8; 32]> = fetch.iter().map(|f| f.root_hash).collect();
        got.sort();
        assert_eq!(got, vec![small, mid]);

        // The synced version was recorded.
        let sub = db::pin_subscriptions::get(&db, &peer_str)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sub.last_version, 42);

        // Same version again: nothing changed, no re-fetch.
        let resp_same = PinsetResponse::Ok {
            version: 42,
            entries: vec![entry(small, 100, "small.bin")],
        };
        assert!(
            on_pinset_received(&db, peer, resp_same, 200)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn reconcile_skips_already_held_shares() {
        let (db, _dir) = test_db().await;
        let peer = PeerId::random();
        let peer_str = peer.to_string();
        db::pin_subscriptions::upsert(&db, &peer_str, 10_000, 1)
            .await
            .unwrap();

        let have = [11u8; 32];
        let need = [22u8; 32];
        // We already hold `have` as a share.
        db::shares::insert(
            &db,
            NewSharedFile {
                root_hash: &have,
                name: "have.bin",
                size: 100,
                mime_type: None,
                path: "/tmp/have.bin",
                chunk_size: 100,
                added_at: 1,
                mtime: 0,
                chunks: &[(0, [7u8; 32], 100)],
            },
        )
        .await
        .unwrap();

        let resp = PinsetResponse::Ok {
            version: 7,
            entries: vec![entry(have, 100, "have.bin"), entry(need, 200, "need.bin")],
        };
        let fetch = on_pinset_received(&db, peer, resp, 100).await;

        // Both are wanted (within quota) but only the missing one is fetched.
        assert!(db::mirror_pins::is_wanted(&db, &have).await.unwrap());
        assert!(db::mirror_pins::is_wanted(&db, &need).await.unwrap());
        assert_eq!(fetch.len(), 1);
        assert_eq!(fetch[0].root_hash, need);
    }

    /// Insert a share whose `path` is a real file in `dir`, return its path.
    async fn share_file(db: &db::Db, dir: &std::path::Path, hash: &[u8; 32], name: &str) -> String {
        let path = dir.join(name);
        tokio::fs::write(&path, b"data").await.unwrap();
        let path_str = path.to_str().unwrap().to_string();
        db::shares::insert(
            db,
            NewSharedFile {
                root_hash: hash,
                name,
                size: 4,
                mime_type: None,
                path: &path_str,
                chunk_size: 4,
                added_at: 1,
                mtime: 0,
                chunks: &[(0, [3u8; 32], 4)],
            },
        )
        .await
        .unwrap();
        path_str
    }

    #[tokio::test]
    async fn eviction_respects_ownership_pins_and_location() {
        let (db, dir) = test_db().await;
        let pin_dir = dir.path().join("pins");
        let other_dir = dir.path().join("downloads");
        tokio::fs::create_dir_all(&pin_dir).await.unwrap();
        tokio::fs::create_dir_all(&other_dir).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel::<NodeCmd>(16);

        // 1. Owned mirror under pin_dir, nobody wants it -> evicted.
        let gone = [1u8; 32];
        let gone_path = share_file(&db, &pin_dir, &gone, "gone.bin").await;
        db::mirror_owned::mark(&db, &gone, 1).await.unwrap();

        // 2. Owned but still manually pinned -> kept.
        let pinned = [2u8; 32];
        let pinned_path = share_file(&db, &pin_dir, &pinned, "pinned.bin").await;
        db::mirror_owned::mark(&db, &pinned, 1).await.unwrap();
        db::pins::add(&db, &pinned, 1).await.unwrap();

        // 3. Owned but still wanted by a subscription -> kept.
        let peer = PeerId::random();
        db::pin_subscriptions::upsert(&db, &peer.to_string(), 10_000, 1)
            .await
            .unwrap();
        let wanted = [3u8; 32];
        let wanted_path = share_file(&db, &pin_dir, &wanted, "wanted.bin").await;
        db::mirror_owned::mark(&db, &wanted, 1).await.unwrap();
        db::mirror_pins::set_for_peer(
            &db,
            &peer.to_string(),
            &[db::mirror_pins::MirrorEntry {
                root_hash: wanted,
                name: Some("wanted.bin".into()),
                size: 4,
                state: db::mirror_pins::STATE_WANTED.into(),
            }],
            1,
        )
        .await
        .unwrap();

        // 4. NOT owned (the user's own share), unwanted -> never touched.
        let user = [4u8; 32];
        let user_path = share_file(&db, &pin_dir, &user, "user.bin").await;

        // 5. Owned + unwanted but the file lives outside pin_dir -> file kept,
        //    ownership dropped.
        let elsewhere = [5u8; 32];
        let elsewhere_path = share_file(&db, &other_dir, &elsewhere, "elsewhere.bin").await;
        db::mirror_owned::mark(&db, &elsewhere, 1).await.unwrap();

        let n = evict_unwanted(&db, &tx, &pin_dir).await;
        assert_eq!(n, 1, "only the one unwanted owned file under pin_dir");

        // 1. Evicted: file, share and ownership all gone.
        assert!(!std::path::Path::new(&gone_path).exists());
        assert!(db::shares::get_by_hash(&db, &gone).await.unwrap().is_none());
        assert!(!db::mirror_owned::is_owned(&db, &gone).await.unwrap());

        // 2/3. Kept entirely.
        assert!(std::path::Path::new(&pinned_path).exists());
        assert!(db::mirror_owned::is_owned(&db, &pinned).await.unwrap());
        assert!(std::path::Path::new(&wanted_path).exists());
        assert!(db::mirror_owned::is_owned(&db, &wanted).await.unwrap());

        // 4. User content untouched.
        assert!(std::path::Path::new(&user_path).exists());
        assert!(db::shares::get_by_hash(&db, &user).await.unwrap().is_some());

        // 5. File kept, but no longer claimed as ours.
        assert!(std::path::Path::new(&elsewhere_path).exists());
        assert!(
            db::shares::get_by_hash(&db, &elsewhere)
                .await
                .unwrap()
                .is_some()
        );
        assert!(!db::mirror_owned::is_owned(&db, &elsewhere).await.unwrap());
    }
}
