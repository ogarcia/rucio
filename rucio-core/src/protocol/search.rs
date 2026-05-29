//! Wire types for the Gossipsub search protocol.
//!
//! Both `SearchQuery` and `SearchResult` are serialised as JSON and published
//! on their respective Gossipsub topics.  Using JSON keeps things debuggable;
//! we can switch to a binary codec later without changing the protocol version.

use crate::protocol::chunk::Hash;
use urlencoding;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// QueryId
// ---------------------------------------------------------------------------

/// Unique identifier for a search query (UUID v4).
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

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// SearchQuery  (published on /rucio/search/1.0.0)
// ---------------------------------------------------------------------------

/// A search query propagated through the gossip network.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchQuery {
    /// Unique query identifier — used to correlate results.
    pub id: QueryId,
    /// Keywords to match against file names (case-insensitive substring).
    pub keywords: Vec<String>,
    /// Remaining hops before the message is dropped.  Starts at a small
    /// value (e.g. 7) and is decremented by each forwarding peer.
    pub ttl: u8,
    /// libp2p PeerId (base58) of the originating node.
    pub requester: String,
}

impl SearchQuery {
    pub const DEFAULT_TTL: u8 = 7;

    pub fn new(keywords: Vec<String>, requester: String) -> Self {
        Self {
            id: QueryId::new(),
            keywords,
            ttl: Self::DEFAULT_TTL,
            requester,
        }
    }

    /// Returns true if `name` contains **all** keywords.
    ///
    /// Comparison is case-insensitive and accent-insensitive so that
    /// "ultimo" matches "Último" and vice versa.
    pub fn matches(&self, name: &str) -> bool {
        if self.keywords.is_empty() {
            return false;
        }
        let norm_name = normalize_search_term(name);
        self.keywords
            .iter()
            .all(|kw| norm_name.contains(&normalize_search_term(kw)))
    }
}

// ---------------------------------------------------------------------------
// SearchResult  (published on /rucio/search/result/1.0.0)
// ---------------------------------------------------------------------------

/// A search result published by a peer that holds a matching file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    /// Correlates back to the originating query.
    pub query_id: QueryId,
    /// BLAKE3 root hash of the file (hex-encoded).
    pub root_hash: String,
    /// Human-readable file name.
    pub name: String,
    /// Total file size in bytes.
    pub size: u64,
    /// Number of chunks.
    pub chunk_count: usize,
    /// Optional MIME type.
    pub mime_type: Option<String>,
    /// Magnet link for this file.
    pub magnet: String,
    /// PeerId of the peer that holds the file.
    pub provider: String,
}

impl SearchResult {
    /// Build a magnet link from hash hex string, name, size, and an optional
    /// provider PeerId string.  The name is URL-encoded so that spaces and
    /// special characters survive the round-trip through `parse_magnet`.
    pub fn magnet_from_parts(
        hash_hex: &str,
        name: &str,
        size: u64,
        provider: Option<&str>,
    ) -> String {
        let encoded_name = urlencoding::encode(name);
        let provider_param = provider
            .filter(|p| !p.is_empty())
            .map(|p| format!("&provider={p}"))
            .unwrap_or_default();
        format!("rucio:{hash_hex}?name={encoded_name}&size={size}{provider_param}")
    }

    /// Build a magnet link from a [`Hash`] value.
    pub fn magnet_from(hash: &Hash, name: &str, size: u64, provider: Option<&str>) -> String {
        Self::magnet_from_parts(&hash.to_hex(), name, size, provider)
    }
}

// ---------------------------------------------------------------------------
// Keyword normalization
// ---------------------------------------------------------------------------

/// Normalize a search term for case- and accent-insensitive matching.
///
/// Lowercases the input and folds Latin diacritics to their ASCII base
/// characters, mirroring the normalization eMule clients apply before
/// hashing keywords for the Kad2 DHT.  Used both in Gossipsub result
/// matching and in Kad2 keyword generation so both paths operate in the
/// same character space.
pub fn normalize_search_term(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let lc = c.to_lowercase().next().unwrap_or(c);
        match lc {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => out.push('a'),
            'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => out.push('e'),
            'ì' | 'í' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'ĩ' => out.push('i'),
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => out.push('o'),
            'ù' | 'ú' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => out.push('u'),
            'ç' | 'ć' | 'ĉ' | 'č' => out.push('c'),
            // NOTE: 'ñ' is intentionally NOT folded. In Spanish it is a distinct
            // letter, not an accented 'n' (año ≠ ano, caña ≠ cana), and eMule
            // indexes Kad keywords with the 'ñ' intact — folding it to 'n' made
            // our keyword hash miss those files. Other languages' n-diacritics
            // (ń/ņ/ň) are genuine accents, so they still fold to 'n'.
            'ń' | 'ņ' | 'ň' => out.push('n'),
            'ý' | 'ÿ' => out.push('y'),
            'ð' | 'ď' => out.push('d'),
            'ß' => {
                out.push('s');
                out.push('s');
            }
            'æ' => {
                out.push('a');
                out.push('e');
            }
            'ł' => out.push('l'),
            'þ' => {
                out.push('t');
                out.push('h');
            }
            'ź' | 'ż' | 'ž' => out.push('z'),
            'š' | 'ś' | 'ş' | 'ŝ' => out.push('s'),
            'ř' | 'ŗ' => out.push('r'),
            'ğ' | 'ĝ' | 'ġ' => out.push('g'),
            'ħ' => out.push('h'),
            'ĵ' => out.push('j'),
            'ķ' => out.push('k'),
            'ľ' | 'ļ' | 'ĺ' => out.push('l'),
            'ţ' | 'ť' => out.push('t'),
            other => out.push(other),
        }
    }
    out
}

/// Normalize a keyword for the eMule **Kad2** network: lowercase only, no
/// diacritic folding.
///
/// Kad hashes keywords for exact match, and real eMule clients only lowercase
/// before hashing — it was the old ed2k *servers* that folded accents, and we
/// don't use those. Folding here (as [`normalize_search_term`] does) would make
/// our keyword hash miss the files clients indexed with the accent intact.
///
/// **Known limitation:** Kad searches are therefore accent-sensitive — `camion`
/// and `camión` are distinct keywords, and there is no symmetric workaround
/// because the DHT index is not under our control. Accent-insensitive search
/// applies only to the rucio network, where we control both sides of the match
/// (see [`normalize_search_term`]).
pub fn lowercase_keyword(s: &str) -> String {
    s.to_lowercase()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn query(keywords: &[&str]) -> SearchQuery {
        SearchQuery::new(
            keywords.iter().map(|s| s.to_string()).collect(),
            "peer123".to_string(),
        )
    }

    #[test]
    fn matches_exact() {
        let q = query(&["hello"]);
        assert!(q.matches("hello.txt"));
    }

    #[test]
    fn matches_substring() {
        let q = query(&["rust"]);
        assert!(q.matches("learn-rust-2024.pdf"));
    }

    #[test]
    fn matches_case_insensitive() {
        let q = query(&["Rust"]);
        assert!(q.matches("learn-rust-2024.pdf"));

        let q2 = query(&["rust"]);
        assert!(q2.matches("Rust_Programming.epub"));
    }

    #[test]
    fn matches_accent_insensitive() {
        // Search without accent finds accented filename.
        let q = query(&["ultimo"]);
        assert!(q.matches("Último año.avi"));

        // Search with accent finds plain filename.
        let q2 = query(&["último"]);
        assert!(q2.matches("ultimo año.avi"));

        // Both directions work for multi-word (accents only).
        let q3 = query(&["ultimo", "año"]);
        assert!(q3.matches("Último Año.avi"));

        // 'ñ' is a distinct letter: 'ano' must NOT match 'Año'.
        let q4 = query(&["ano"]);
        assert!(!q4.matches("Último Año.avi"));
        // …but 'año' (with ñ) does, ignoring case.
        let q5 = query(&["año"]);
        assert!(q5.matches("Último AÑO.avi"));
    }

    #[test]
    fn normalize_search_term_basic() {
        use super::normalize_search_term;
        assert_eq!(normalize_search_term("Último"), "ultimo");
        assert_eq!(normalize_search_term("ÜBER"), "uber");
        assert_eq!(normalize_search_term("straße"), "strasse");
        // 'ñ' is a distinct letter and must NOT fold to 'n' (only lowercased).
        assert_eq!(normalize_search_term("Ñoño"), "ñoño");
        assert_eq!(normalize_search_term("Fariña"), "fariña");
        // Other-language n-diacritics still fold.
        assert_eq!(normalize_search_term("Gdańsk"), "gdansk");
    }

    #[test]
    fn requires_all_keywords() {
        let q = query(&["foo", "bar"]);
        assert!(q.matches("foo_bar_file.zip")); // both present
        assert!(!q.matches("foofile.zip")); // only first
        assert!(!q.matches("barfile.zip")); // only second
    }

    #[test]
    fn no_match() {
        let q = query(&["xyz"]);
        assert!(!q.matches("hello_world.mp4"));
    }

    #[test]
    fn empty_keywords_never_match() {
        let q = query(&[]);
        assert!(!q.matches("anything.txt"));
    }

    #[test]
    fn default_ttl() {
        let q = query(&["test"]);
        assert_eq!(q.ttl, SearchQuery::DEFAULT_TTL);
    }

    #[test]
    fn query_id_is_unique() {
        let q1 = query(&["a"]);
        let q2 = query(&["a"]);
        assert_ne!(q1.id.0, q2.id.0);
    }
}
