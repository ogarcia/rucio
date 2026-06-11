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

use crate::api::DownloadRequest;
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
                // Empty label normalises to "uncollected" on the wire.
                collection: pin.collection.filter(|c| !c.is_empty()),
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
        request_one_pinset(cmd_tx, peer).await;
    }
}

/// Ask a single peer for its pin-set now (used on a fresh subscription so the
/// first sync doesn't wait for the next reconcile tick).
pub async fn request_one_pinset(cmd_tx: &Sender<NodeCmd>, peer: PeerId) {
    // Resolve the peer's current addresses by PeerId via its signed DHT record,
    // then warm the routing table. Both add addresses so `send_request` can dial
    // a subscription peer we've never connected to — without baking volatile IPs
    // into the subscription, which is just the stable PeerId.
    let _ = cmd_tx.send(NodeCmd::ResolvePeer { peer }).await;
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

    // Record the collections the peer advertises from the FULL set, before any
    // follow-scope filtering, so the UI can offer them even when nothing is
    // being mirrored yet (follow_all = 0 with an empty/narrow followed set).
    let seen: Vec<String> = {
        let mut s: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for e in &entries {
            s.insert(e.collection.clone().unwrap_or_default());
        }
        s.into_iter().collect()
    };
    if let Err(e) = db::pin_subscriptions::set_seen_collections(db, &peer_str, &seen).await {
        warn!(peer = %peer_str, "reconcile: recording seen collections failed: {e}");
    }

    // Scope the pin-set to the collections this subscription follows. When
    // `follow_all` is set the whole set is in scope; otherwise keep only entries
    // whose collection is in the followed set (the empty label "" matches the
    // peer's uncollected pins). Out-of-scope entries are dropped entirely — they
    // are not this subscription's concern, so they aren't even recorded as
    // `skipped` (which is reserved for in-scope entries that don't fit quota).
    if !sub.follow_all {
        let followed: std::collections::HashSet<String> =
            match db::pin_subscriptions::list_collections(db, &peer_str).await {
                Ok(c) => c.into_iter().collect(),
                Err(e) => {
                    warn!(peer = %peer_str, "reconcile: listing followed collections failed: {e}");
                    return Vec::new();
                }
            };
        entries.retain(|e| {
            let label = e.collection.as_deref().unwrap_or("");
            followed.contains(label)
        });
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
            collection: e.collection.clone(),
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

/// Keep mirrored content that's going out of scope (unsubscribe, or dropping a
/// followed collection): drop the `mirror_owned` mark for each hash no *other*
/// subscription still wants, turning it into a permanent share the eviction
/// sweep won't reclaim. Hashes another subscription wants are left owned — that
/// subscription keeps managing them.
///
/// `exclude_peer`: when the subscription losing the content still exists (a
/// collection narrowing), pass its peer id so its own (about-to-be-dropped)
/// `wanted` rows don't count as a keeper. Pass `None` once the subscription has
/// already been removed (unsubscribe), where any remaining `wanted` row is by
/// definition another subscription's.
pub async fn retain_mirror_content(db: &db::Db, hashes: &[[u8; 32]], exclude_peer: Option<&str>) {
    for h in hashes {
        if !db::mirror_owned::is_owned(db, h).await.unwrap_or(false) {
            continue; // the user already had it — not mirror-managed, nothing to do
        }
        let wanted_elsewhere = match exclude_peer {
            Some(p) => db::mirror_pins::wanted_by_other(db, h, p).await,
            None => db::mirror_pins::is_wanted(db, h).await,
        }
        .unwrap_or(false);
        if wanted_elsewhere {
            continue; // another subscription still wants it; leave it managed
        }
        if let Err(e) = db::mirror_owned::unmark(db, h).await {
            warn!("retain: dropping mirror ownership failed: {e}");
        }
    }
}

/// How much of `hashes` going out of scope for `peer_id` freeing would actually
/// reclaim: mirror-owned, not a manual pin, not wanted by another subscription,
/// and either a completed copy under `pin_dir` (deleted) or an in-flight fetch
/// (cancelled). Returns `(count, bytes)`. Lets the UI skip the keep/free prompt
/// only when nothing is truly at stake — e.g. content auto-tagged into a
/// category dir outside `pin_dir`, where "free" would be a no-op.
pub async fn evictable_count(
    db: &db::Db,
    hashes: &[[u8; 32]],
    peer_id: &str,
    pin_dir: &std::path::Path,
) -> (usize, u64) {
    let mut count = 0usize;
    let mut bytes = 0u64;
    for h in hashes {
        if !db::mirror_owned::is_owned(db, h).await.unwrap_or(false) {
            continue;
        }
        if db::pins::exists(db, h).await.unwrap_or(false) {
            continue;
        }
        if db::mirror_pins::wanted_by_other(db, h, peer_id)
            .await
            .unwrap_or(false)
        {
            continue;
        }
        // A completed copy under pin_dir would be deleted.
        if let Ok(Some(share)) = db::shares::get_by_hash(db, h).await {
            if std::path::Path::new(&share.path).starts_with(pin_dir) {
                count += 1;
                bytes += share.size.max(0) as u64;
            }
            continue;
        }
        // No share yet: an in-flight fetch would be cancelled (and its partial
        // discarded). That still counts as something to free, so the user is
        // asked rather than silently keeping a download that's still running.
        if let Ok(Some(row)) = db::downloads::get_by_root_hash(db, h).await
            && matches!(
                row.status.as_str(),
                "finding_providers" | "queued" | "downloading" | "stalled"
            )
        {
            count += 1;
            bytes += row.total_size.max(0) as u64;
        }
    }
    (count, bytes)
}

/// Evict mirror content nobody wants any more. A hash is evicted only when it is
/// mirror-owned (we fetched it solely to mirror), is neither a manual pin nor
/// wanted by any subscription, and its file lives under `pin_dir`. That triple
/// guard means we never delete the user's own downloads or shares. Returns how
/// many hashes were evicted.
pub async fn evict_unwanted(
    db: &db::Db,
    cmd_tx: &Sender<NodeCmd>,
    download_tx: &Sender<DownloadRequest>,
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

        // Still fetching (no share yet): cancel it so it doesn't complete into a
        // permanent share after the user chose to free the space. `cancel`
        // deletes the .part and stops providing; we drop ownership below.
        if let Ok(Some(row)) = db::downloads::get_by_root_hash(db, &hash).await
            && matches!(
                row.status.as_str(),
                "finding_providers" | "queued" | "downloading" | "stalled"
            )
        {
            let _ = db::downloads::set_status(db, row.id, "cancelled", None).await;
            let _ = download_tx
                .send(DownloadRequest::Cancel {
                    download_id: row.id,
                    root_hash: hash.to_vec(),
                })
                .await;
            info!(hash = %hex::encode(hash), "Cancelled in-flight mirror download (no longer wanted)");
            evicted += 1;
        }

        // No longer wanted. Find where its (completed) copy lives.
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
                    // Drop the now-stale `completed` download record too: it was
                    // this mirror's (the hash is mirror-owned), and leaving it
                    // would make a later re-subscribe dedupe against it
                    // (`already completed`) and never re-fetch the deleted file.
                    if let Ok(Some(dl)) = db::downloads::get_by_root_hash(db, &hash).await {
                        let _ = db::downloads::delete(db, dl.id).await;
                    }
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
        db::pins::add(&db, &have, None, 10).await.unwrap();
        db::pins::add(&db, &absent, None, 11).await.unwrap();

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
            collection: None,
        }
    }

    fn entry_in(hash: [u8; 32], size: u64, name: &str, collection: &str) -> PinsetEntry {
        PinsetEntry {
            collection: Some(collection.into()),
            ..entry(hash, size, name)
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
    async fn reconcile_mirrors_only_followed_collections() {
        let (db, _dir) = test_db().await;
        let peer = PeerId::random();
        let peer_str = peer.to_string();
        db::pin_subscriptions::upsert(&db, &peer_str, 1_000_000, 1)
            .await
            .unwrap();
        // Follow only "Manuals" (not "Series", not uncollected).
        db::pin_subscriptions::set_collections(&db, &peer_str, false, &["Manuals".to_string()])
            .await
            .unwrap();

        let manual = [10u8; 32];
        let serie = [20u8; 32];
        let loose = [30u8; 32];
        let resp = PinsetResponse::Ok {
            version: 7,
            entries: vec![
                entry_in(manual, 100, "guide.pdf", "Manuals"),
                entry_in(serie, 200, "ep1.mkv", "Series"),
                entry(loose, 300, "loose.bin"), // uncollected
            ],
        };

        let fetch = on_pinset_received(&db, peer, resp, 100).await;

        // Only the followed collection is mirrored; the rest aren't even recorded.
        assert!(db::mirror_pins::is_wanted(&db, &manual).await.unwrap());
        assert!(!db::mirror_pins::is_wanted(&db, &serie).await.unwrap());
        assert!(!db::mirror_pins::is_wanted(&db, &loose).await.unwrap());
        let got: Vec<[u8; 32]> = fetch.iter().map(|f| f.root_hash).collect();
        assert_eq!(got, vec![manual]);
        assert_eq!(
            db::mirror_pins::list_for_peer(&db, &peer_str)
                .await
                .unwrap()
                .len(),
            1
        );
        // ...but the UI still discovers every collection the peer advertises,
        // including the unfollowed and uncollected ones.
        assert_eq!(
            db::pin_subscriptions::list_seen_collections(&db, &peer_str)
                .await
                .unwrap(),
            vec!["".to_string(), "Manuals".to_string(), "Series".to_string()]
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
        let (dtx, _drx) = tokio::sync::mpsc::channel::<DownloadRequest>(16);

        // 1. Owned mirror under pin_dir, nobody wants it -> evicted.
        let gone = [1u8; 32];
        let gone_path = share_file(&db, &pin_dir, &gone, "gone.bin").await;
        db::mirror_owned::mark(&db, &gone, 1).await.unwrap();

        // 2. Owned but still manually pinned -> kept.
        let pinned = [2u8; 32];
        let pinned_path = share_file(&db, &pin_dir, &pinned, "pinned.bin").await;
        db::mirror_owned::mark(&db, &pinned, 1).await.unwrap();
        db::pins::add(&db, &pinned, None, 1).await.unwrap();

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
                collection: None,
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

        let n = evict_unwanted(&db, &tx, &dtx, &pin_dir).await;
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

    #[tokio::test]
    async fn retain_keeps_owned_unless_wanted_elsewhere() {
        let (db, _dir) = test_db().await;
        let alice = PeerId::random();
        let bob = PeerId::random();
        db::pin_subscriptions::upsert(&db, &alice.to_string(), 10_000, 1)
            .await
            .unwrap();
        db::pin_subscriptions::upsert(&db, &bob.to_string(), 10_000, 1)
            .await
            .unwrap();

        // `solo` is mirrored only from Alice; `shared` from both.
        let solo = [1u8; 32];
        let shared = [2u8; 32];
        for h in [solo, shared] {
            db::mirror_owned::mark(&db, &h, 1).await.unwrap();
        }
        let entry = |h: [u8; 32], name: &str| db::mirror_pins::MirrorEntry {
            root_hash: h,
            name: Some(name.into()),
            size: 4,
            state: db::mirror_pins::STATE_WANTED.into(),
            collection: None,
        };
        db::mirror_pins::set_for_peer(
            &db,
            &alice.to_string(),
            &[entry(solo, "solo"), entry(shared, "shared")],
            1,
        )
        .await
        .unwrap();
        db::mirror_pins::set_for_peer(&db, &bob.to_string(), &[entry(shared, "shared")], 1)
            .await
            .unwrap();

        // Unsubscribe from Alice with keep: capture her hashes, remove, retain.
        let hashes: Vec<[u8; 32]> = db::mirror_pins::list_for_peer(&db, &alice.to_string())
            .await
            .unwrap()
            .iter()
            .filter_map(|r| <[u8; 32]>::try_from(r.root_hash.as_slice()).ok())
            .collect();
        db::pin_subscriptions::remove(&db, &alice.to_string())
            .await
            .unwrap();
        retain_mirror_content(&db, &hashes, None).await;

        // `solo` nobody else wants -> ownership dropped (now a permanent share).
        assert!(!db::mirror_owned::is_owned(&db, &solo).await.unwrap());
        // `shared` is still wanted by Bob -> stays owned (Bob keeps managing it).
        assert!(db::mirror_owned::is_owned(&db, &shared).await.unwrap());
    }

    #[tokio::test]
    async fn evictable_only_counts_deletable_content() {
        let (db, dir) = test_db().await;
        let pin_dir = dir.path().join("pins");
        let cat_dir = dir.path().join("category");
        tokio::fs::create_dir_all(&pin_dir).await.unwrap();
        tokio::fs::create_dir_all(&cat_dir).await.unwrap();
        let peer = PeerId::random();

        // a: owned, under pin_dir, unpinned, not wanted elsewhere -> evictable.
        let a = [1u8; 32];
        share_file(&db, &pin_dir, &a, "a.bin").await;
        db::mirror_owned::mark(&db, &a, 1).await.unwrap();
        // b: owned but auto-tagged OUTSIDE pin_dir -> not evictable (the case
        //    the user raised: "free" would do nothing).
        let b = [2u8; 32];
        share_file(&db, &cat_dir, &b, "b.bin").await;
        db::mirror_owned::mark(&db, &b, 1).await.unwrap();
        // c: under pin_dir but the user pinned it -> not evictable.
        let c = [3u8; 32];
        share_file(&db, &pin_dir, &c, "c.bin").await;
        db::mirror_owned::mark(&db, &c, 1).await.unwrap();
        db::pins::add(&db, &c, None, 1).await.unwrap();

        let hashes = [a, b, c];
        let (count, bytes) = evictable_count(&db, &hashes, &peer.to_string(), &pin_dir).await;
        assert_eq!(count, 1, "only `a` is actually deletable");
        assert_eq!(bytes, 4); // share_file writes 4 bytes

        // All outside pin_dir / pinned -> nothing to free, prompt can be skipped.
        let (n2, _) = evictable_count(&db, &[b, c], &peer.to_string(), &pin_dir).await;
        assert_eq!(n2, 0);

        // An in-flight fetch (no share yet) still counts — freeing cancels it,
        // so the user must be asked rather than silently keeping it.
        let d = [4u8; 32];
        db::downloads::create_pending(&db, &d, Some("d.bin"), 1, true, None)
            .await
            .unwrap();
        db::mirror_owned::mark(&db, &d, 1).await.unwrap();
        let (n3, _) = evictable_count(&db, &[d], &peer.to_string(), &pin_dir).await;
        assert_eq!(n3, 1, "in-flight mirror download counts as freeable");
    }

    #[tokio::test]
    async fn evict_cancels_in_flight_mirror_download() {
        let (db, dir) = test_db().await;
        let pin_dir = dir.path().join("pins");
        tokio::fs::create_dir_all(&pin_dir).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel::<NodeCmd>(16);
        let (dtx, mut drx) = tokio::sync::mpsc::channel::<DownloadRequest>(16);

        // A mirror-owned hash that's an in-flight download (no share yet) and
        // wanted by nobody -> eviction must cancel it, not let it finish.
        let h = [1u8; 32];
        let res = db::downloads::create_pending(&db, &h, Some("x.bin"), 1, true, None)
            .await
            .unwrap();
        let id = res.id();
        db::mirror_owned::mark(&db, &h, 1).await.unwrap();

        let n = evict_unwanted(&db, &tx, &dtx, &pin_dir).await;
        assert_eq!(n, 1);
        // A Cancel was emitted for this download.
        match drx.try_recv() {
            Ok(DownloadRequest::Cancel {
                download_id,
                root_hash,
            }) => {
                assert_eq!(download_id, id);
                assert_eq!(root_hash, h.to_vec());
            }
            _ => panic!("expected a Cancel for the in-flight mirror download"),
        }
        // Marked cancelled and ownership dropped.
        assert_eq!(
            db::downloads::get_status(&db, id).await.unwrap().as_deref(),
            Some("cancelled")
        );
        assert!(!db::mirror_owned::is_owned(&db, &h).await.unwrap());
    }

    #[tokio::test]
    async fn eviction_clears_completed_record_so_resubscribe_refetches() {
        let (db, dir) = test_db().await;
        let pin_dir = dir.path().join("pins");
        tokio::fs::create_dir_all(&pin_dir).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel::<NodeCmd>(16);
        let (dtx, _drx) = tokio::sync::mpsc::channel::<DownloadRequest>(16);

        // A completed mirror download: its share lives under pin_dir, it's owned
        // and wanted by nobody.
        let h = [7u8; 32];
        let res = db::downloads::create_pending(&db, &h, Some("ep.nfo"), 1, true, None)
            .await
            .unwrap();
        let id = res.id();
        db::downloads::set_status(&db, id, "completed", None)
            .await
            .unwrap();
        share_file(&db, &pin_dir, &h, "ep.nfo").await;
        db::mirror_owned::mark(&db, &h, 1).await.unwrap();

        evict_unwanted(&db, &tx, &dtx, &pin_dir).await;

        // The share is gone AND the completed record is cleared, so a later
        // re-subscribe starts a fresh download instead of "already completed".
        assert!(db::shares::get_by_hash(&db, &h).await.unwrap().is_none());
        assert!(db::downloads::get(&db, id).await.unwrap().is_none());
    }
}
