//! Our persistent eMule user hash (credit identity).
//!
//! eMule's credit system keys a peer's standing by the 16-byte user hash it
//! advertises in HELLO. We generate one random hash per node, mark it as an
//! eMule client (byte 5 = 14, byte 14 = 111, the convention real clients check),
//! and persist it so the credit we earn by seeding accrues to a single, stable
//! identity across restarts.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// Return our persistent eMule user hash, generating and storing one on first use.
pub async fn get_or_create(db: &Db) -> Result<[u8; 16]> {
    if let Some(row) = sqlx::query("SELECT user_hash FROM emule_identity WHERE id = 1")
        .fetch_optional(db)
        .await?
    {
        let stored: Vec<u8> = row.get("user_hash");
        if let Ok(hash) = <[u8; 16]>::try_from(stored.as_slice()) {
            return Ok(hash);
        }
        // Corrupt/wrong length — fall through and regenerate.
    }
    let hash = random_user_hash();
    sqlx::query("INSERT OR REPLACE INTO emule_identity (id, user_hash) VALUES (1, ?1)")
        .bind(hash.as_slice())
        .execute(db)
        .await?;
    Ok(hash)
}

/// A random 16-byte eMule user hash carrying the markers (`[5] = 14`,
/// `[14] = 111`) that real clients use to recognise an eMule-compatible peer.
fn random_user_hash() -> [u8; 16] {
    let mut hash = *uuid::Uuid::new_v4().as_bytes();
    hash[5] = 14;
    hash[14] = 111;
    hash
}
