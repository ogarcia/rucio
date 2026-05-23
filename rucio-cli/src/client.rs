//! Thin wrapper around reqwest for talking to the Rucio daemon API.
//!
//! All methods return `anyhow::Result`; HTTP errors (4xx/5xx) are surfaced
//! as `anyhow::Error` with the status code and body included.

use anyhow::{Context, Result, bail};
use reqwest::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;

use rucio_core::api::{
    config::ConfigResponse,
    downloads::{DownloadResponse, DownloadsResponse, StartDownloadRequest},
    search::{SearchRequest, SearchResultsResponse, SearchStartedResponse},
    shares::{AddShareRequest, AddShareResponse, SharesResponse},
    status::{PeersResponse, StatusResponse},
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

    pub async fn list_shares(&self) -> Result<SharesResponse> {
        self.get("/api/v1/shares").await
    }

    pub async fn add_share(&self, path: &str) -> Result<AddShareResponse> {
        self.post(
            "/api/v1/shares",
            &AddShareRequest {
                path: path.to_string(),
            },
        )
        .await
    }

    pub async fn remove_share(&self, hash: &str) -> Result<()> {
        self.delete(&format!("/api/v1/shares/{hash}")).await
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

    pub async fn start_download(&self, magnet: &str, provider: Option<&str>) -> Result<()> {
        let url = format!("{}/api/v1/downloads", self.base);
        let resp = self
            .inner
            .post(&url)
            .json(&StartDownloadRequest {
                magnet: magnet.to_string(),
                provider: provider.map(|s| s.to_string()),
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

    pub async fn cancel_download(&self, id: i64) -> Result<()> {
        self.delete(&format!("/api/v1/downloads/{id}")).await
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    pub async fn start_search(&self, keywords: Vec<String>) -> Result<SearchStartedResponse> {
        self.post("/api/v1/search", &SearchRequest { keywords })
            .await
    }

    pub async fn poll_search(&self, query_id: &str) -> Result<SearchResultsResponse> {
        self.get(&format!("/api/v1/search/{query_id}")).await
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
    // Downloads by hash (for cancel-by-hash convenience)
    // -----------------------------------------------------------------------

    pub async fn find_download_by_hash(&self, hash: &str) -> Result<Option<DownloadResponse>> {
        let resp = self.list_downloads().await?;
        Ok(resp.downloads.into_iter().find(|d| d.root_hash == hash))
    }
}
