//! Cooperative pinning: serving our pin-set over `/rucio/pinset/1.0.0`.
//!
//! Our published pin-set is the set of manually pinned hashes we currently
//! *have* (i.e. that are present as shares). We don't advertise a pin whose
//! content we couldn't actually serve. The exchange is authenticated by the
//! libp2p connection, so the response carries no signature.

use rucio_core::protocol::pinset::{PinsetEntry, PinsetResponse};
use tokio::sync::mpsc::Sender;

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
}
