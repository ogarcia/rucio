//! Wire types for the Gossipsub search protocol.
//!
//! Both `SearchQuery` and `SearchResult` are serialised as JSON and published
//! on their respective Gossipsub topics.  Using JSON keeps things debuggable;
//! we can switch to a binary codec later without changing the protocol version.

use crate::protocol::chunk::Hash;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// QueryId
// ---------------------------------------------------------------------------

/// Unique identifier for a search query (UUID v4).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct QueryId(pub String);

impl QueryId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// SearchQuery  (published on /rucio/search/1.0.0)
// ---------------------------------------------------------------------------

/// A search query propagated through the gossip network.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchQuery {
    /// Unique query identifier — used to correlate results.
    pub id: QueryId,
    /// Keywords to match against file names (case-insensitive substring).
    pub keywords: Vec<String>,
    /// Remaining hops before the message is dropped.  Starts at a small
    /// value (e.g. 7) and is decremented by each forwarding peer.
    pub ttl: u8,
    /// libp2p PeerId (base58) of the originating node.
    pub requester: String,
}

impl SearchQuery {
    pub const DEFAULT_TTL: u8 = 7;

    pub fn new(keywords: Vec<String>, requester: String) -> Self {
        Self {
            id: QueryId::new(),
            keywords,
            ttl: Self::DEFAULT_TTL,
            requester,
        }
    }

    /// Returns true if `name` matches any keyword (case-insensitive).
    pub fn matches(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        self.keywords
            .iter()
            .any(|kw| lower.contains(&kw.to_lowercase()))
    }
}

// ---------------------------------------------------------------------------
// SearchResult  (published on /rucio/search/result/1.0.0)
// ---------------------------------------------------------------------------

/// A search result published by a peer that holds a matching file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    /// Correlates back to the originating query.
    pub query_id: QueryId,
    /// BLAKE3 root hash of the file (hex-encoded).
    pub root_hash: String,
    /// Human-readable file name.
    pub name: String,
    /// Total file size in bytes.
    pub size: u64,
    /// Number of chunks.
    pub chunk_count: usize,
    /// Optional MIME type.
    pub mime_type: Option<String>,
    /// Magnet link for this file.
    pub magnet: String,
    /// PeerId of the peer that holds the file.
    pub provider: String,
}

impl SearchResult {
    /// Build a magnet link from hash hex string, name and size.
    pub fn magnet_from_parts(hash_hex: &str, name: &str, size: u64) -> String {
        format!("rucio:{hash_hex}?name={name}&size={size}")
    }

    /// Build a magnet link from a [`Hash`] value.
    pub fn magnet_from(hash: &Hash, name: &str, size: u64) -> String {
        Self::magnet_from_parts(&hash.to_hex(), name, size)
    }
}
