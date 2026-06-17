//! Our persistent eMule user hash (credit identity), stored on disk.
//!
//! eMule's credit system keys a peer's standing by the 16-byte user hash it
//! advertises in HELLO. We generate one random hash per node, mark it as an
//! eMule client (byte 5 = 14, byte 14 = 111, the convention real clients check)
//! and persist it so the credit we earn by seeding accrues to a single, stable
//! identity across restarts.
//!
//! It lives at `emule.identity_path` (defaulting next to the libp2p
//! `identity.key`), *not* in the database: both are long-lived identities that
//! must survive a database rebuild (the DB holds only reconstructible state).
//! See [`rucio_net::identity`] for the rucio side — this is the eMule mirror of
//! it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

/// Where the eMule user hash lives: `emule.identity_path`. Defaults next to the
/// libp2p `identity.key` (so both node identities sit together out of the box),
/// but is independently configurable.
pub fn path(config: &crate::config::Config) -> PathBuf {
    config.emule.identity_path.clone()
}

/// Load the 16-byte user hash from `path`, creating it on first run.
pub fn load_or_create(path: &Path) -> Result<[u8; 16]> {
    if path.exists() {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        if let Ok(hash) = <[u8; 16]>::try_from(bytes.as_slice()) {
            info!("Loaded eMule user hash from disk");
            return Ok(hash);
        }
        // Wrong length — the file is corrupt; regenerate rather than fail.
        warn!(path = %path.display(), "eMule identity file malformed — regenerating");
    } else {
        warn!(path = %path.display(), "eMule identity file not found — generating new user hash");
    }

    let hash = random_user_hash();
    persist(&hash, path)?;
    info!("Generated new eMule user hash");
    Ok(hash)
}

/// Write the user hash to `path`, creating parent directories as needed.
fn persist(hash: &[u8; 16], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    std::fs::write(path, hash).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// A random 16-byte eMule user hash carrying the markers (`[5] = 14`,
/// `[14] = 111`) that real clients use to recognise an eMule-compatible peer.
fn random_user_hash() -> [u8; 16] {
    let mut hash = *uuid::Uuid::new_v4().as_bytes();
    hash[5] = 14;
    hash[14] = 111;
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_load_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("emule_identity.key");

        let first = load_or_create(&p).unwrap();
        // Markers identifying an eMule-compatible client.
        assert_eq!(first[5], 14);
        assert_eq!(first[14], 111);

        // A second load returns the same hash (persisted, not regenerated).
        let second = load_or_create(&p).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn malformed_file_is_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("emule_identity.key");
        std::fs::write(&p, b"too short").unwrap();

        let hash = load_or_create(&p).unwrap();
        assert_eq!(hash[5], 14);
        assert_eq!(hash[14], 111);
        // The file now holds a valid 16-byte hash.
        assert_eq!(std::fs::read(&p).unwrap().len(), 16);
    }
}
