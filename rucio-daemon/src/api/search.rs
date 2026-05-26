//! Legacy search handler stubs.
//!
//! The old `/api/v1/search` and `/api/v1/search/{query_id}` endpoints have
//! been replaced by the unified `/api/v1/searches` family in the `searches`
//! module.
//!
//! This file is kept because [`rucio_core::api::search::SearchResultResponse`]
//! is still referenced by the WebSocket event type
//! [`rucio_core::api::ws::WsEvent::SearchResult`].  The route handlers
//! (`start_search` and `get_results`) have been removed; re-add them here if
//! you need to support old clients.
