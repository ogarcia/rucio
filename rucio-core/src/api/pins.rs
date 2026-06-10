//! DTOs for the pins API (`/api/v1/pins`).
//!
//! A pin is content the node keeps available on purpose: the user supplies a
//! magnet, and the daemon fetches it if missing (into the pin directory) and
//! keeps it shared and re-provided. The pin row is the intent, distinct from an
//! incidental share.

/// Request body for `POST /api/v1/pins`: pin a magnet (fetch if not present).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PinRequest {
    /// A `rucio:` magnet link identifying the content to pin.
    pub magnet: String,
    /// Optional provider PeerId hints to seed the fetch (as in download add).
    #[serde(default)]
    pub providers: Vec<String>,
}

/// A pin as returned by the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PinResponse {
    /// Root hash (hex).
    pub root_hash: String,
    /// File name, if known (from the share or the in-flight download).
    #[serde(default)]
    pub name: Option<String>,
    /// File size in bytes, if known.
    #[serde(default)]
    pub size: Option<u64>,
    /// Current state of the pinned content:
    /// `available` (present and shared), `fetching` (being downloaded),
    /// or `missing` (pinned but neither present nor downloading).
    pub state: PinState,
    /// Unix timestamp when the pin was added.
    pub added_at: i64,
}

/// State of a pin's underlying content.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PinState {
    /// Present on disk and shared/re-provided.
    Available,
    /// Currently being fetched.
    Fetching,
    /// Pinned but neither present nor in flight (e.g. the fetch was cancelled).
    Missing,
}

/// GET /api/v1/pins
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PinsResponse {
    pub pins: Vec<PinResponse>,
}
