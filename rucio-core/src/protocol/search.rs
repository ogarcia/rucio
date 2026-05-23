use crate::protocol::file::FileDescriptor;
use uuid::Uuid;

/// Unique identifier for a search query.
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

/// A search query propagated through the gossip network.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchQuery {
    pub id: QueryId,
    pub keywords: Vec<String>,
    /// Remaining hops. Decremented at each peer before forwarding.
    pub ttl: u8,
    /// PeerId of the originating peer (as string).
    pub requester: String,
}

/// A search result returned by a peer that has a matching file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    pub query_id: QueryId,
    pub descriptor: FileDescriptor,
}
