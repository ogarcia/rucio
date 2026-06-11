//! DTOs for the subscriptions API (`/api/v1/subscriptions`).
//!
//! A subscription mirrors another node's published pin-set: the daemon
//! periodically fetches that peer's pin-set and keeps a copy of it within a
//! disk quota, becoming an extra provider for that content (cooperative
//! pinning). The peer is identified by its libp2p PeerId; share it as a
//! `rucio-peer:<peer_id>` link.

/// Canonical scheme for a shareable subscription link.
pub const PEER_LINK_SCHEME: &str = "rucio-peer:";

/// Format a peer id as a shareable subscription link.
pub fn peer_link(peer_id: &str) -> String {
    format!("{PEER_LINK_SCHEME}{peer_id}")
}

/// Extract the peer id from a subscription input, accepting either a bare
/// PeerId or a `rucio-peer:`-prefixed link (with surrounding whitespace).
pub fn parse_peer_input(input: &str) -> &str {
    input
        .trim()
        .strip_prefix(PEER_LINK_SCHEME)
        .unwrap_or_else(|| input.trim())
        .trim()
}

/// Request body for `POST /api/v1/subscriptions`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionRequest {
    /// The peer to mirror: a libp2p PeerId, optionally `rucio-peer:`-prefixed.
    pub peer: String,
    /// Hard ceiling of disk (bytes) to devote to mirroring this peer. Must be
    /// greater than zero.
    pub quota_bytes: u64,
}

/// A subscription as returned by the API, enriched with mirror progress.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionResponse {
    /// The mirrored peer's libp2p PeerId.
    pub peer_id: String,
    /// Disk quota in bytes.
    pub quota_bytes: u64,
    /// Bytes selected within quota for this peer (committed total: present +
    /// still-fetching).
    pub used_bytes: u64,
    /// Bytes actually on disk so far (the present subset of `used_bytes`).
    pub present_bytes: u64,
    /// Number of mirror entries selected within quota (wanted).
    pub wanted_count: usize,
    /// Number of wanted entries already present on disk (genuinely mirrored).
    /// `wanted_count - present_count` are still being fetched.
    pub present_count: usize,
    /// Number of entries known but skipped because they don't fit the quota.
    pub skipped_count: usize,
    /// Version of the peer's pin-set we last applied (0 = never synced).
    pub last_version: i64,
    /// Unix timestamp of the last successful sync (0 = never).
    pub last_synced_at: i64,
    /// Unix timestamp when the subscription was added.
    pub added_at: i64,
    /// true = mirror the whole peer; false = only `followed_collections`.
    pub follow_all: bool,
    /// Collections of this peer we follow (meaningful only when `follow_all` is
    /// false). The empty string "" denotes the peer's uncollected pins.
    #[serde(default)]
    pub followed_collections: Vec<String>,
    /// Distinct collections seen in this peer's synced pin-set, for the UI's
    /// selector. "" denotes uncollected pins. Populated after the first sync.
    #[serde(default)]
    pub available_collections: Vec<String>,
}

/// Request body for `PUT /api/v1/subscriptions/{peer_id}/collections`: choose
/// which of a peer's collections to mirror.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionCollectionsRequest {
    /// true = mirror the whole peer (the `collections` list is then ignored but
    /// remembered, so flipping back restores the selection).
    pub follow_all: bool,
    /// The collections to follow when `follow_all` is false. "" = uncollected.
    #[serde(default)]
    pub collections: Vec<String>,
    /// When the new scope drops collections, `true` keeps the content already
    /// mirrored from them (it becomes a permanent share you own) and `false`
    /// (default) lets the re-sync evict it. No effect when the scope only grows.
    #[serde(default)]
    pub keep: bool,
}

/// GET /api/v1/subscriptions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionResponse>,
}

/// Resolved state of a single mirrored file.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MirrorFileState {
    /// Present on disk and shared (genuinely mirrored).
    Present,
    /// Currently being fetched.
    Fetching,
    /// Wanted but neither present nor in flight yet (no provider found, queued).
    Missing,
    /// Known but skipped because it doesn't fit the quota.
    Skipped,
}

/// One file in a subscription's mirror set, with its resolved state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct MirrorFile {
    /// Root hash (hex).
    pub root_hash: String,
    /// File name, if known.
    #[serde(default)]
    pub name: Option<String>,
    /// File size in bytes.
    pub size: u64,
    /// Resolved state.
    pub state: MirrorFileState,
}

/// GET /api/v1/subscriptions/{peer_id}/files
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionFilesResponse {
    pub files: Vec<MirrorFile>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_bare_and_prefixed() {
        assert_eq!(parse_peer_input("12D3KooW"), "12D3KooW");
        assert_eq!(parse_peer_input("rucio-peer:12D3KooW"), "12D3KooW");
        assert_eq!(parse_peer_input("  rucio-peer:12D3KooW  "), "12D3KooW");
        assert_eq!(peer_link("12D3KooW"), "rucio-peer:12D3KooW");
    }
}
