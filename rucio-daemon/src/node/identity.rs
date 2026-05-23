//! Persistent Ed25519 identity for the local node.
//!
//! The keypair is stored on disk as raw 64-byte secret-key material
//! (libp2p `Keypair::to_protobuf_encoding`).  On first run the file is
//! created automatically; on subsequent runs the same PeerId is restored.

use anyhow::{Context, Result};
use libp2p::identity::{ed25519, Keypair};
use std::path::Path;
use tracing::{info, warn};

/// Load the keypair from `path`, creating it if the file does not exist.
pub fn load_or_create(path: &Path) -> Result<Keypair> {
    if path.exists() {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading identity file {}", path.display()))?;
        let keypair =
            Keypair::from_protobuf_encoding(&bytes).context("decoding identity keypair")?;
        info!(peer_id = %keypair.public().to_peer_id(), "Loaded identity from disk");
        Ok(keypair)
    } else {
        warn!(path = %path.display(), "Identity file not found — generating new keypair");
        let secret = ed25519::SecretKey::generate();
        let keypair = Keypair::from(ed25519::Keypair::from(secret));
        persist(&keypair, path)?;
        info!(peer_id = %keypair.public().to_peer_id(), "Generated new identity");
        Ok(keypair)
    }
}

/// Write the keypair to `path`, creating parent directories as needed.
fn persist(keypair: &Keypair, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    let bytes = keypair
        .to_protobuf_encoding()
        .context("encoding identity keypair")?;
    std::fs::write(path, &bytes)
        .with_context(|| format!("writing identity file {}", path.display()))?;
    Ok(())
}
