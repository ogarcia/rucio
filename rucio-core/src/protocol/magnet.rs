use crate::protocol::chunk::Hash;
use thiserror::Error;

/// A magnet link identifying a file on the Rucio network.
///
/// Format: `rucio:<root_hash_hex>?name=<name>&size=<size>`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MagnetLink {
    pub root_hash: Hash,
    pub name: Option<String>,
    pub size: Option<u64>,
}

#[derive(Debug, Error)]
pub enum MagnetError {
    #[error("invalid scheme, expected 'rucio:'")]
    InvalidScheme,
    #[error("missing or invalid hash")]
    InvalidHash,
}

impl std::fmt::Display for MagnetLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rucio:{}", self.root_hash.to_hex())?;
        let mut params: Vec<String> = Vec::new();
        if let Some(ref name) = self.name {
            params.push(format!("name={}", name));
        }
        if let Some(size) = self.size {
            params.push(format!("size={}", size));
        }
        if !params.is_empty() {
            write!(f, "?{}", params.join("&"))?;
        }
        Ok(())
    }
}

impl MagnetLink {
    pub fn parse(s: &str) -> Result<Self, MagnetError> {
        let s = s.strip_prefix("rucio:").ok_or(MagnetError::InvalidScheme)?;
        let (hash_hex, query) = match s.split_once('?') {
            Some((h, q)) => (h, Some(q)),
            None => (s, None),
        };

        let hash_bytes = hex::decode(hash_hex)
            .ok()
            .and_then(|b| b.try_into().ok())
            .map(Hash)
            .ok_or(MagnetError::InvalidHash)?;

        let mut name = None;
        let mut size = None;

        if let Some(q) = query {
            for param in q.split('&') {
                if let Some(v) = param.strip_prefix("name=") {
                    name = Some(v.to_string());
                } else if let Some(v) = param.strip_prefix("size=") {
                    size = v.parse().ok();
                }
            }
        }

        Ok(Self {
            root_hash: hash_bytes,
            name,
            size,
        })
    }
}
