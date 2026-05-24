use crate::protocol::chunk::Hash;
use thiserror::Error;

/// A magnet link identifying a file on the Rucio network.
///
/// Primary format (minimal): `rucio:<root_hash_hex>`
///
/// Extended format: `rucio:<root_hash_hex>?name=<name>&size=<bytes>&provider=<peer_id>&provider=<peer_id>`
///
/// Only `root_hash` is mandatory.  All other fields are optional hints that
/// allow the download engine to display metadata or connect faster, but the
/// network can resolve everything from the hash alone via the DHT.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MagnetLink {
    pub root_hash: Hash,
    pub name: Option<String>,
    pub size: Option<u64>,
    /// Known providers — zero or more PeerIds encoded as strings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<String>,
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
            params.push(format!("name={}", urlencoding::encode(name)));
        }
        if let Some(size) = self.size {
            params.push(format!("size={}", size));
        }
        for p in &self.providers {
            params.push(format!("provider={}", p));
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

        let mut providers: Vec<String> = Vec::new();

        if let Some(q) = query {
            for param in q.split('&') {
                if let Some(v) = param.strip_prefix("name=") {
                    name = Some(
                        urlencoding::decode(v)
                            .unwrap_or_else(|_| v.into())
                            .into_owned(),
                    );
                } else if let Some(v) = param.strip_prefix("size=") {
                    size = v.parse().ok();
                } else if let Some(v) = param.strip_prefix("provider=")
                    && !v.is_empty()
                {
                    providers.push(v.to_string());
                }
            }
        }

        Ok(Self {
            root_hash: hash_bytes,
            name,
            size,
            providers,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_hash() -> Hash {
        Hash([0u8; 32])
    }

    fn make_link(name: Option<&str>, size: Option<u64>, providers: Vec<&str>) -> MagnetLink {
        MagnetLink {
            root_hash: zero_hash(),
            name: name.map(|s| s.to_string()),
            size,
            providers: providers.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn minimal_roundtrip() {
        let link = make_link(None, None, vec![]);
        let s = link.to_string();
        assert!(s.starts_with("rucio:"));
        assert!(!s.contains('?'));
        let parsed = MagnetLink::parse(&s).unwrap();
        assert_eq!(parsed.root_hash.0, [0u8; 32]);
        assert!(parsed.name.is_none());
        assert!(parsed.size.is_none());
        assert!(parsed.providers.is_empty());
    }

    #[test]
    fn full_roundtrip() {
        let link = make_link(Some("my file.mkv"), Some(12345), vec!["PeerXyz"]);
        let s = link.to_string();
        assert!(s.contains("name=my%20file.mkv") || s.contains("name=my+file.mkv"));
        let parsed = MagnetLink::parse(&s).unwrap();
        assert_eq!(parsed.name.as_deref(), Some("my file.mkv"));
        assert_eq!(parsed.size, Some(12345));
        assert_eq!(parsed.providers, vec!["PeerXyz"]);
    }

    #[test]
    fn special_chars_in_name() {
        let link = make_link(Some("héllo & wörld"), None, vec![]);
        let s = link.to_string();
        let parsed = MagnetLink::parse(&s).unwrap();
        assert_eq!(parsed.name.as_deref(), Some("héllo & wörld"));
    }

    #[test]
    fn multiple_providers() {
        let link = make_link(None, None, vec!["Peer1", "Peer2", "Peer3"]);
        let s = link.to_string();
        let parsed = MagnetLink::parse(&s).unwrap();
        assert_eq!(parsed.providers, vec!["Peer1", "Peer2", "Peer3"]);
    }

    #[test]
    fn invalid_scheme() {
        assert!(matches!(
            MagnetLink::parse("magnet:?xt=foo"),
            Err(MagnetError::InvalidScheme)
        ));
    }

    #[test]
    fn invalid_hash() {
        assert!(matches!(
            MagnetLink::parse("rucio:notahex"),
            Err(MagnetError::InvalidHash)
        ));
        // Wrong length (not 32 bytes)
        assert!(matches!(
            MagnetLink::parse("rucio:deadbeef"),
            Err(MagnetError::InvalidHash)
        ));
    }
}
