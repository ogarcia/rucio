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

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tracing::{info, warn};

/// Where the eMule user hash lives: `emule.identity_path`. Defaults next to the
/// libp2p `identity.key` (so both node identities sit together out of the box),
/// but is independently configurable.
pub fn path(config: &crate::config::Config) -> PathBuf {
    config.emule.identity_path.clone()
}

/// Load the 16-byte user hash from `path`, creating it on first run.
///
/// Infallible by design: it ALWAYS returns a valid hash carrying the eMule
/// markers, never an all-zero (`[0u8; 16]`) "null" hash. Repeated or concurrent
/// calls converge on the same on-disk identity — the file is created
/// exclusively, so a caller that loses the race adopts the winner's hash instead
/// of persisting its own. This matters because the download, seeding and Kad
/// subsystems each call this: they must advertise ONE stable user hash.
/// Advertising a different hash (or a null hash) from the same endpoint trips
/// eMuleAI's hash-changer / bad-hash protection, which bans us as "Bad user
/// hash". If the file cannot be persisted (e.g. a read-only config dir) we log
/// and fall back to a session-only hash rather than emitting a null one.
pub fn load_or_create(path: &Path) -> [u8; 16] {
    if let Some(hash) = read_hash(path) {
        info!("Loaded eMule user hash from disk");
        return hash;
    }

    if path.exists() {
        // Present but the wrong length (or unreadable) — treat as corrupt and
        // overwrite with a fresh hash so we stop advertising a broken identity.
        warn!(path = %path.display(), "eMule identity file malformed — regenerating");
        let hash = random_user_hash();
        if let Err(e) = write_hash(&hash, path, false) {
            warn!(path = %path.display(), error = %e,
                "could not rewrite eMule identity — using a session-only hash");
        }
        return hash;
    }

    // Missing — create it exclusively so concurrent callers converge on one hash.
    warn!(path = %path.display(), "eMule identity file not found — generating new user hash");
    let hash = random_user_hash();
    match write_hash(&hash, path, true) {
        Ok(()) => {
            info!("Generated new eMule user hash");
            hash
        }
        // Another caller created it first; adopt theirs so every subsystem agrees.
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => read_hash(path).unwrap_or(hash),
        Err(e) => {
            warn!(path = %path.display(), error = %e,
                "could not persist eMule identity — using a session-only hash");
            hash
        }
    }
}

/// Read and validate a 16-byte hash from `path`; `None` if missing/malformed.
fn read_hash(path: &Path) -> Option<[u8; 16]> {
    let bytes = std::fs::read(path).ok()?;
    <[u8; 16]>::try_from(bytes.as_slice()).ok()
}

/// Write the user hash to `path`, creating parent directories as needed.
/// `exclusive` uses `create_new` so a concurrent creator is detected
/// (`AlreadyExists`) instead of silently clobbered.
fn write_hash(hash: &[u8; 16], path: &Path, exclusive: bool) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut opts = OpenOptions::new();
    opts.write(true);
    if exclusive {
        opts.create_new(true);
    } else {
        opts.create(true).truncate(true);
    }
    opts.open(path)?.write_all(hash)
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

        let first = load_or_create(&p);
        // Markers identifying an eMule-compatible client.
        assert_eq!(first[5], 14);
        assert_eq!(first[14], 111);

        // A second load returns the same hash (persisted, not regenerated).
        let second = load_or_create(&p);
        assert_eq!(first, second);
    }

    #[test]
    fn repeated_loads_converge_and_are_never_null() {
        // The download, seeding and Kad subsystems each load independently; every
        // call must yield the SAME non-null hash so we never advertise two
        // identities (or a null one) from the same endpoint.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("emule_identity.key");

        let a = load_or_create(&p);
        let b = load_or_create(&p);
        let c = load_or_create(&p);
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_ne!(a, [0u8; 16], "must never be the null hash");
    }

    #[test]
    fn malformed_file_is_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("emule_identity.key");
        std::fs::write(&p, b"too short").unwrap();

        let hash = load_or_create(&p);
        assert_eq!(hash[5], 14);
        assert_eq!(hash[14], 111);
        // The file now holds a valid 16-byte hash, and a reload is stable.
        assert_eq!(std::fs::read(&p).unwrap().len(), 16);
        assert_eq!(load_or_create(&p), hash);
    }
}
