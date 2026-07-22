//! Wire types for the `/rucio/outboard/1.0.0` request-response protocol.
//!
//! Lets a node fetch the full bao outboard (the BLAKE3 inner-hash tree) of a
//! file by its root hash, to rebuild a lost `.obao` sidecar and re-validate a
//! partially-downloaded `.part` locally without re-downloading the data.

/// Request the outboard for a file identified by its root hash.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutboardRequest {
    pub root_hash: [u8; 32],
}

/// The response to an [`OutboardRequest`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum OutboardResponse {
    /// The full outboard for the requested file. Only a holder of the COMPLETE
    /// file can answer this (a partial holder lacks the full tree).
    Ok {
        root_hash: [u8; 32],
        outboard: Vec<u8>,
    },
    /// The provider does not have the complete file.
    NotFound,
    /// Internal error on the provider side.
    Error(String),
}
