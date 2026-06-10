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
    /// Bytes currently wanted (selected within quota) for this peer.
    pub used_bytes: u64,
    /// Number of mirror entries currently selected (wanted).
    pub wanted_count: usize,
    /// Number of entries known but skipped because they don't fit the quota.
    pub skipped_count: usize,
    /// Version of the peer's pin-set we last applied (0 = never synced).
    pub last_version: i64,
    /// Unix timestamp of the last successful sync (0 = never).
    pub last_synced_at: i64,
    /// Unix timestamp when the subscription was added.
    pub added_at: i64,
}

/// GET /api/v1/subscriptions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionResponse>,
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
