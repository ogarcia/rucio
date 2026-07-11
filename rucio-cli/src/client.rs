//! Thin wrapper around reqwest for talking to the Rucio daemon API.
//!
//! All methods return `anyhow::Result`; HTTP errors (4xx/5xx) are surfaced
//! as `anyhow::Error` with the status code and body included.

use anyhow::{Context, Result, bail};
use reqwest::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Convenience alias for the WebSocket stream type returned by [`ApiClient::ws_stream`].
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

use rucio_core::api::{
    categories::{CategoriesResponse, CategoryRequest, CategoryResponse, SetCategoryRequest},
    config::ConfigResponse,
    downloads::{
        DownloadDetailResponse, DownloadResponse, DownloadsResponse, StartDownloadRequest,
    },
    emule::{EmuleBootstrapRequest, EmuleBootstrapResponse, EmuleStatusResponse},
    metrics::{HealthResponse, MetricsResponse},
    pins::{PinRequest, PinResponse, PinsResponse},
    searches::{
        SearchDetailResponse, SearchListResponse, SearchNetwork, SearchStartedResponse,
        StartSearchRequest,
    },
    shares::{
        AddShareRequest, AddShareResponse, ShareFilter, ShareResponse, SharedDirsResponse,
        SharesResponse, UpdateSharedDirRequest,
    },
    status::{PeersResponse, StatusResponse},
    subscriptions::{SubscriptionRequest, SubscriptionResponse, SubscriptionsResponse},
    uploads::UploadsResponse,
};

/// HTTP client bound to a specific daemon base URL.
#[derive(Clone)]
pub struct ApiClient {
    base: String,
    inner: reqwest::Client,
}

impl ApiClient {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            inner: reqwest::Client::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Generic helpers
    // -----------------------------------------------------------------------

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request::<(), T>(Method::GET, path, None).await
    }

    async fn post<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        self.request::<B, T>(Method::POST, path, Some(body)).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self
            .inner
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("DELETE {url} → {status}: {body}");
        }
    }

    /// POST with no request or response body (for action endpoints returning 204).
    async fn post_empty(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self
            .inner
            .post(&url)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("POST {url} → {status}: {body}");
        }
    }

    async fn put<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self
            .inner
            .put(&url)
            .json(body)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("PUT {url} → {status}: {body}");
        }
    }

    async fn request<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.inner.request(method.clone(), &url);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("{method} {url}"))?;

        if resp.status().is_success() {
            resp.json::<T>()
                .await
                .with_context(|| format!("decoding response from {url}"))
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("{method} {url} → {status}: {body}");
        }
    }

    // -----------------------------------------------------------------------
    // Status
    // -----------------------------------------------------------------------

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get("/api/v1/status").await
    }

    pub async fn peers(&self) -> Result<PeersResponse> {
        self.get("/api/v1/peers").await
    }

    // -----------------------------------------------------------------------
    // Shares
    // -----------------------------------------------------------------------

    /// The directories being shared (watched), with file count and total size.
    pub async fn list_shared_dirs(&self) -> Result<SharedDirsResponse> {
        self.get("/api/v1/shares").await
    }

    /// One page of shared files. `GET /api/v1/shares/files` is paginated
    /// (`GET /api/v1/shares` lists the watched directories instead).
    pub async fn list_shares_page(
        &self,
        q: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<SharesResponse> {
        let mut path = format!("/api/v1/shares/files?limit={limit}&offset={offset}");
        if let Some(q) = q.filter(|s| !s.is_empty()) {
            path.push_str(&format!("&q={}", urlencoding::encode(q)));
        }
        self.get(&path).await
    }

    /// Every shared file matching `q` (server-side, case-insensitive substring),
    /// paging through `list_shares_page` so the result is complete regardless of
    /// library size — the listing endpoint caps a single page at 1000 rows.
    pub async fn list_all_shares(&self, q: Option<&str>) -> Result<Vec<ShareResponse>> {
        const PAGE: i64 = 1000;
        let mut out = Vec::new();
        let mut offset = 0i64;
        loop {
            let resp = self.list_shares_page(q, PAGE, offset).await?;
            let got = resp.shares.len();
            out.extend(resp.shares);
            if (got as i64) < PAGE || out.len() as u64 >= resp.total {
                break;
            }
            offset += PAGE;
        }
        Ok(out)
    }

    pub async fn add_share(&self, path: &str, filter: ShareFilter) -> Result<AddShareResponse> {
        self.post(
            "/api/v1/shares",
            &AddShareRequest {
                path: path.to_string(),
                filter,
            },
        )
        .await
    }

    /// Update a shared directory's file filter.
    pub async fn update_shared_dir(&self, path: &str, filter: ShareFilter) -> Result<()> {
        self.put(
            "/api/v1/shares",
            &UpdateSharedDirRequest {
                path: path.to_string(),
                filter,
            },
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Categories
    // -----------------------------------------------------------------------

    pub async fn list_categories(&self) -> Result<CategoriesResponse> {
        self.get("/api/v1/categories").await
    }

    pub async fn create_category(
        &self,
        name: &str,
        download_dir: Option<&str>,
        color: Option<&str>,
        match_keywords: Option<&str>,
    ) -> Result<CategoryResponse> {
        self.post(
            "/api/v1/categories",
            &CategoryRequest {
                name: name.to_string(),
                download_dir: download_dir.map(str::to_string),
                color: color.map(str::to_string),
                match_keywords: match_keywords.map(str::to_string),
            },
        )
        .await
    }

    pub async fn update_category(
        &self,
        id: i64,
        name: &str,
        download_dir: Option<&str>,
        color: Option<&str>,
        match_keywords: Option<&str>,
    ) -> Result<()> {
        self.put(
            &format!("/api/v1/categories/{id}"),
            &CategoryRequest {
                name: name.to_string(),
                download_dir: download_dir.map(str::to_string),
                color: color.map(str::to_string),
                match_keywords: match_keywords.map(str::to_string),
            },
        )
        .await
    }

    pub async fn delete_category(&self, id: i64) -> Result<()> {
        self.delete(&format!("/api/v1/categories/{id}")).await
    }

    // --- Pins --------------------------------------------------------------

    pub async fn list_pins(&self) -> Result<PinsResponse> {
        self.get("/api/v1/pins").await
    }

    pub async fn create_pin(
        &self,
        magnet: &str,
        providers: Vec<String>,
        collection: Option<String>,
    ) -> Result<PinResponse> {
        self.post(
            "/api/v1/pins",
            &PinRequest {
                magnet: magnet.to_string(),
                providers,
                collection,
            },
        )
        .await
    }

    pub async fn delete_pin(&self, hash: &str) -> Result<()> {
        self.delete(&format!("/api/v1/pins/{hash}")).await
    }

    // --- Subscriptions -----------------------------------------------------

    pub async fn list_subscriptions(&self) -> Result<SubscriptionsResponse> {
        self.get("/api/v1/subscriptions").await
    }

    pub async fn create_subscription(
        &self,
        peer: &str,
        quota_bytes: u64,
    ) -> Result<SubscriptionResponse> {
        self.post(
            "/api/v1/subscriptions",
            &SubscriptionRequest {
                peer: peer.to_string(),
                quota_bytes,
            },
        )
        .await
    }

    pub async fn delete_subscription(&self, peer_id: &str, keep: bool) -> Result<()> {
        self.delete(&format!("/api/v1/subscriptions/{peer_id}?keep={keep}"))
            .await
    }

    /// Assign (or clear, with `None`) a download's category. `id` may be negative
    /// for an eMule download.
    pub async fn set_download_category(&self, id: i64, category_id: Option<i64>) -> Result<()> {
        self.put(
            &format!("/api/v1/downloads/{id}/category"),
            &SetCategoryRequest { category_id },
        )
        .await
    }

    /// Set a download's user priority. `id` may be negative for an eMule download.
    pub async fn set_download_priority(
        &self,
        id: i64,
        priority: rucio_core::api::downloads::DownloadPriority,
    ) -> Result<()> {
        self.put(
            &format!("/api/v1/downloads/{id}/priority"),
            &rucio_core::api::downloads::SetDownloadPriorityRequest { priority },
        )
        .await
    }

    /// Retrieve the magnet link for a locally shared file by hash (full or prefix).
    pub async fn get_share_magnet(&self, hash: &str) -> Result<String> {
        self.get(&format!("/api/v1/shares/{hash}/magnet")).await
    }

    /// Return the number of files currently being indexed by the daemon.
    /// Files pending indexing: `(rucio, emule)` — the Rucio (BLAKE3) backlog and
    /// the separate eMule (MD4) hashing backlog (`0` when eMule is disabled).
    pub async fn indexing_pending(&self) -> Result<(usize, usize)> {
        let url = format!("{}/api/v1/shares/indexing", self.base);
        let resp = self
            .inner
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let rucio = body["pending"].as_u64().unwrap_or(0) as usize;
        let emule = body["ed2k_pending"].as_u64().unwrap_or(0) as usize;
        Ok((rucio, emule))
    }

    pub async fn remove_shares_by_path(&self, path: &str) -> Result<u64> {
        let url = format!(
            "{}/api/v1/shares?path={}",
            self.base,
            urlencoding::encode(path)
        );
        let resp = self
            .inner
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;

        if resp.status().is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            Ok(body["removed"].as_u64().unwrap_or(0))
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("DELETE {url} → {status}: {body}");
        }
    }

    // -----------------------------------------------------------------------
    // Downloads
    // -----------------------------------------------------------------------

    pub async fn list_downloads(&self) -> Result<DownloadsResponse> {
        self.get("/api/v1/downloads").await
    }

    pub async fn list_uploads(&self) -> Result<UploadsResponse> {
        self.get("/api/v1/uploads").await
    }

    pub async fn get_download(&self, id: i64) -> Result<DownloadDetailResponse> {
        self.get(&format!("/api/v1/downloads/{id}")).await
    }

    pub async fn start_download(
        &self,
        magnet: &str,
        providers: Vec<String>,
        category_id: Option<i64>,
    ) -> Result<()> {
        let url = format!("{}/api/v1/downloads", self.base);
        let resp = self
            .inner
            .post(&url)
            .json(&StartDownloadRequest {
                magnet: magnet.to_string(),
                providers,
                category_id,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("POST {url} → {status}: {body}");
        }
    }

    pub async fn start_ed2k_download(&self, link: &str, category_id: Option<i64>) -> Result<()> {
        use rucio_core::api::downloads::StartEd2kDownloadRequest;
        let url = format!("{}/api/v1/downloads/ed2k", self.base);
        let resp = self
            .inner
            .post(&url)
            .json(&StartEd2kDownloadRequest {
                link: link.to_string(),
                category_id,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.as_u16() == 501 {
                bail!(
                    "The daemon does not support eMule downloads (emule-compat feature not compiled in)."
                );
            }
            bail!("POST {url} → {status}: {body}");
        }
    }

    pub async fn emule_status(&self) -> Result<EmuleStatusResponse> {
        self.get("/api/v1/emule/status").await
    }

    pub async fn emule_bootstrap(&self, url: Option<String>) -> Result<EmuleBootstrapResponse> {
        let api_url = format!("{}/api/v1/emule/bootstrap", self.base);
        let resp = self
            .inner
            .post(&api_url)
            .json(&EmuleBootstrapRequest { url })
            .send()
            .await
            .with_context(|| format!("POST {api_url}"))?;

        if resp.status().is_success() {
            Ok(resp
                .json()
                .await
                .with_context(|| "parsing bootstrap response")?)
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.as_u16() == 501 {
                bail!(
                    "The daemon does not support eMule commands (emule-compat feature not compiled in)."
                );
            }
            bail!("POST {api_url} → {status}: {body}");
        }
    }

    pub async fn cancel_download(&self, id: i64) -> Result<()> {
        self.post_empty(&format!("/api/v1/downloads/{id}/cancel"))
            .await
    }

    pub async fn pause_download(&self, id: i64) -> Result<()> {
        self.post_empty(&format!("/api/v1/downloads/{id}/pause"))
            .await
    }

    pub async fn resume_download(&self, id: i64) -> Result<()> {
        self.post_empty(&format!("/api/v1/downloads/{id}/resume"))
            .await
    }

    /// Permanently remove a finished download from the history.
    pub async fn delete_download(&self, id: i64) -> Result<()> {
        self.delete(&format!("/api/v1/downloads/{id}")).await
    }

    // -----------------------------------------------------------------------
    // Unified searches
    // -----------------------------------------------------------------------

    pub async fn start_search(
        &self,
        keywords: Vec<String>,
        network: SearchNetwork,
    ) -> Result<SearchStartedResponse> {
        self.post(
            "/api/v1/searches",
            &StartSearchRequest { keywords, network },
        )
        .await
    }

    pub async fn get_search(&self, id: u64) -> Result<SearchDetailResponse> {
        self.get(&format!("/api/v1/searches/{id}")).await
    }

    pub async fn list_searches(&self) -> Result<SearchListResponse> {
        self.get("/api/v1/searches").await
    }

    pub async fn delete_search(&self, id: u64) -> Result<()> {
        self.delete(&format!("/api/v1/searches/{id}")).await
    }

    pub async fn relaunch_search(&self, id: u64) -> Result<SearchStartedResponse> {
        self.post(&format!("/api/v1/searches/{id}/relaunch"), &())
            .await
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    pub async fn get_config(&self) -> Result<ConfigResponse> {
        self.get("/api/v1/config").await
    }

    pub async fn put_config(&self, cfg: &ConfigResponse) -> Result<()> {
        self.put("/api/v1/config", cfg).await
    }

    // -----------------------------------------------------------------------
    // Metrics & health
    // -----------------------------------------------------------------------

    pub async fn metrics(&self) -> Result<MetricsResponse> {
        self.get("/api/v1/metrics").await
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get("/health").await
    }

    // -----------------------------------------------------------------------
    // Downloads by hash (for cancel-by-hash convenience)
    // -----------------------------------------------------------------------

    pub async fn find_download_by_hash(&self, hash: &str) -> Result<Option<DownloadResponse>> {
        let resp = self.list_downloads().await?;
        // Accept full hash or unambiguous prefix.
        let matches: Vec<_> = resp
            .downloads
            .into_iter()
            .filter(|d| d.root_hash.starts_with(hash))
            .collect();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches.into_iter().next().unwrap())),
            n => anyhow::bail!("Ambiguous hash prefix '{hash}' matches {n} downloads"),
        }
    }

    /// Resolve a download by 1-based row index (as shown in `rucio download list`)
    /// or by hash prefix.  Returns `None` if nothing matches.
    pub async fn find_download_by_idx_or_hash(
        &self,
        arg: &str,
    ) -> Result<Option<DownloadResponse>> {
        if let Ok(idx) = arg.trim().parse::<usize>() {
            let resp = self.list_downloads().await?;
            return Ok(resp.downloads.into_iter().nth(idx.saturating_sub(1)));
        }
        self.find_download_by_hash(arg).await
    }

    // -----------------------------------------------------------------------
    // WebSocket event stream
    // -----------------------------------------------------------------------

    /// Connect to the daemon WebSocket bus and return the stream.
    ///
    /// The base URL (`http://...` or `https://...`) is automatically converted
    /// to the appropriate WebSocket scheme (`ws://` / `wss://`).
    pub async fn ws_stream(&self) -> Result<WsStream> {
        // Convert http(s):// → ws(s)://
        let ws_url = self
            .base
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        let url = format!("{ws_url}/api/ws");

        let (stream, _response) = tokio_tungstenite::connect_async(&url)
            .await
            .with_context(|| format!("WebSocket connect to {url}"))?;

        Ok(stream)
    }
}
