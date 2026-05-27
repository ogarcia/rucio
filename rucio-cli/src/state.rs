//! Persistent CLI state: last search results saved to disk so that
//! `rucio download add <N>` can reference results from a previous `rucio search start`.
//!
//! Location: `$XDG_DATA_HOME/rucio/last_search.json`
//! (falls back to `~/.local/share/rucio/last_search.json`)

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A cached search result entry — only the fields needed to start a download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResult {
    pub name: String,
    pub size: u64,
    /// Download link: a `rucio:` magnet for Rucio results, or an `ed2k://` link
    /// for eMule results.
    pub download_link: String,
    /// All known providers for this file (non-empty only for Rucio results).
    pub providers: Vec<String>,
    /// Which network provided this result: `"rucio"` or `"emule"`.
    pub source: String,
}

/// The full last-search state written to disk.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LastSearch {
    pub results: Vec<CachedResult>,
}

impl LastSearch {
    /// Load from disk. Returns an empty state if the file does not exist.
    pub fn load() -> Self {
        match std::fs::read_to_string(state_path()) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Persist to disk, creating parent directories as needed.
    pub fn save(&self) -> Result<()> {
        let path = state_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self).context("serialising last search")?;
        std::fs::write(&path, json)
            .with_context(|| format!("writing state file {}", path.display()))
    }

    /// Look up a 1-based index. Returns `None` if out of range.
    pub fn get(&self, idx: usize) -> Option<&CachedResult> {
        self.results.get(idx.saturating_sub(1))
    }
}

fn state_path() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME").unwrap_or_else(|| "/tmp".into());
            PathBuf::from(home).join(".local").join("share")
        });
    base.join("rucio").join("last_search.json")
}
