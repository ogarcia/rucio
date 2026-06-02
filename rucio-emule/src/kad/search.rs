//! Kad2 search state machine — pure data, no I/O.
//!
//! [`ActiveSearch`] encapsulates all state for an in-flight Kad2 lookup
//! (source search or keyword search).  Callers feed events in via
//! [`ActiveSearch::on_res`] and [`ActiveSearch::on_search_res`] and receive
//! back the list of UDP packets to send.  All socket I/O stays in
//! [`super::task`].

use super::packet::{
    self, Contact, KadId, ResPayload, encode_publish_source_req, encode_req, encode_search_key_req,
    encode_search_source_req,
};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, trace};

// ── Public result types ────────────────────────────────────────────────────────

/// A source found by a Kad2 source search.
#[derive(Debug, Clone)]
pub struct KadSource {
    pub ip: Ipv4Addr,
    pub tcp_port: u16,
    pub udp_port: u16,
    /// The peer's KadID / UserHash (16 bytes).  Used to derive the RC4 key
    /// for TCP obfuscated connections.
    pub user_hash: [u8; 16],
}

/// One file result from a Kad keyword search.
#[derive(Debug, Clone)]
pub struct KeywordHit {
    /// ed2k/MD4 hash of the file (16 bytes, wire byte order).
    pub hash: [u8; 16],
    pub name: String,
    pub size: u64,
}

// ── Outgoing packet descriptor ─────────────────────────────────────────────────

/// How the task should obfuscate an outgoing packet produced by the search.
pub enum ObfuscMode {
    Plain,
    /// Encrypt with the peer's announced UDPKey (RecvKey scheme).
    RecvKey(u32),
    /// Encrypt using the peer's KadID (KadID scheme, eMule v6+).
    KadId([u8; 16]),
}

/// A UDP packet that the search wants the task to send.
pub struct OutPacket {
    pub addr: SocketAddr,
    pub bytes: Vec<u8>,
    pub obfusc: ObfuscMode,
}

// ── Internal mode ─────────────────────────────────────────────────────────────

enum SearchMode {
    Sources {
        file_size: u64,
        reply: oneshot::Sender<Vec<KadSource>>,
        sources: Vec<KadSource>,
    },
    Keyword {
        reply: oneshot::Sender<Vec<KeywordHit>>,
        hits: Vec<KeywordHit>,
    },
    /// Announce ourselves as a source of `target` to the closest nodes. The
    /// two-phase lookup is identical to a source search; only the phase-2
    /// packet differs (PUBLISH_SOURCE_REQ instead of SEARCH_SOURCE_REQ).
    Publish {
        our_id: KadId,
        tcp_port: u16,
        udp_port: u16,
        file_size: u64,
        connect_options: u8,
        /// Number of nodes that acknowledged the store (PUBLISH_RES received).
        published: usize,
        reply: oneshot::Sender<usize>,
    },
}

// ── ActiveSearch ──────────────────────────────────────────────────────────────

/// State machine for an in-flight Kad2 search (source or keyword).
pub struct ActiveSearch {
    pub target: KadId,
    pub deadline: Instant,
    max_results: usize,
    queried: HashSet<SocketAddrV4>,
    queried_ids: HashMap<SocketAddrV4, KadId>,
    searched: HashSet<SocketAddrV4>,
    mode: SearchMode,
}

impl ActiveSearch {
    /// Begin a source search.  Returns the state and the initial REQ packets.
    pub fn new_source_search(
        target: KadId,
        file_size: u64,
        deadline: Instant,
        max_results: usize,
        initial_candidates: &[Contact],
        alpha: usize,
        reply: oneshot::Sender<Vec<KadSource>>,
    ) -> (Self, Vec<OutPacket>) {
        let (queried, queried_ids, out) = initial_reqs(&target, initial_candidates, alpha);
        let search = Self {
            target,
            deadline,
            max_results,
            queried,
            queried_ids,
            searched: HashSet::new(),
            mode: SearchMode::Sources {
                file_size,
                reply,
                sources: Vec::new(),
            },
        };
        (search, out)
    }

    /// Begin a keyword search.  Returns the state and the initial REQ packets.
    pub fn new_keyword_search(
        target: KadId,
        deadline: Instant,
        max_results: usize,
        initial_candidates: &[Contact],
        alpha: usize,
        reply: oneshot::Sender<Vec<KeywordHit>>,
    ) -> (Self, Vec<OutPacket>) {
        let (queried, queried_ids, out) = initial_reqs(&target, initial_candidates, alpha);
        let search = Self {
            target,
            deadline,
            max_results,
            queried,
            queried_ids,
            searched: HashSet::new(),
            mode: SearchMode::Keyword {
                reply,
                hits: Vec::new(),
            },
        };
        (search, out)
    }

    /// Begin a source publish.  Returns the state and the initial REQ packets.
    #[allow(clippy::too_many_arguments)]
    pub fn new_publish(
        target: KadId,
        our_id: KadId,
        tcp_port: u16,
        udp_port: u16,
        file_size: u64,
        connect_options: u8,
        deadline: Instant,
        max_stores: usize,
        initial_candidates: &[Contact],
        alpha: usize,
        reply: oneshot::Sender<usize>,
    ) -> (Self, Vec<OutPacket>) {
        let (queried, queried_ids, out) = initial_reqs(&target, initial_candidates, alpha);
        let search = Self {
            target,
            deadline,
            max_results: max_stores,
            queried,
            queried_ids,
            searched: HashSet::new(),
            mode: SearchMode::Publish {
                our_id,
                tcp_port,
                udp_port,
                file_size,
                connect_options,
                published: 0,
                reply,
            },
        };
        (search, out)
    }

    pub fn queried_count(&self) -> usize {
        self.queried.len()
    }

    /// True when enough results have been collected or the deadline has passed.
    pub fn is_done(&self) -> bool {
        let n = match &self.mode {
            SearchMode::Sources { sources, .. } => sources.len(),
            SearchMode::Keyword { hits, .. } => hits.len(),
            SearchMode::Publish { published, .. } => *published,
        };
        n >= self.max_results || Instant::now() >= self.deadline
    }

    /// Deliver results to the reply channel and consume the search.
    pub fn finish(self) {
        let queried = self.queried.len();
        let target = self.target;
        match self.mode {
            SearchMode::Sources { sources, reply, .. } => {
                debug!(sources = sources.len(), queried, %target, "Kad2 source search finished");
                let _ = reply.send(sources);
            }
            SearchMode::Keyword { hits, reply } => {
                debug!(hits = hits.len(), queried, %target, "Kad2 keyword search finished");
                let _ = reply.send(hits);
            }
            SearchMode::Publish {
                published, reply, ..
            } => {
                debug!(published, queried, %target, "Kad2 source publish finished");
                let _ = reply.send(published);
            }
        }
    }

    /// Handle an incoming KADEMLIA2_RES; returns packets to send.
    ///
    /// Two-phase Kademlia:
    /// 1. If `sender` previously received a REQ from us and hasn't been
    ///    searched yet, send the SEARCH packet now (KadID obfuscation when
    ///    available).
    /// 2. For each new contact discovered in the response, enqueue a REQ
    ///    (up to `alpha` new contacts per response).
    pub fn on_res(
        &mut self,
        res: &ResPayload,
        sender: SocketAddrV4,
        alpha: usize,
    ) -> Vec<OutPacket> {
        let mut out = Vec::new();

        if self.queried.contains(&sender) && !self.searched.contains(&sender) {
            self.searched.insert(sender);
            let pkt = self.make_search_packet();
            let obfusc = match self.queried_ids.get(&sender) {
                Some(id) => {
                    trace!(%sender, "Two-phase SEARCH with KadID obfuscation");
                    ObfuscMode::KadId(*id.as_bytes())
                }
                None => ObfuscMode::Plain,
            };
            out.push(OutPacket {
                addr: SocketAddr::V4(sender),
                bytes: pkt,
                obfusc,
            });
        }

        let new_contacts: Vec<_> = res
            .contacts
            .iter()
            .filter(|c| !self.queried.contains(&c.socket_addr_udp()))
            .take(alpha)
            .collect();
        debug!(
            from = %sender,
            total_in_res = res.contacts.len(),
            new_to_query = new_contacts.len(),
            "Kad2 Res during search"
        );
        for c in new_contacts {
            let addr = c.socket_addr_udp();
            self.queried.insert(addr);
            self.queried_ids.insert(addr, c.id);
            let pkt = encode_req(2, &self.target, &c.id);
            let obfusc = match c.udp_key {
                Some(key) => ObfuscMode::RecvKey(key),
                None => ObfuscMode::Plain,
            };
            out.push(OutPacket {
                addr: SocketAddr::V4(addr),
                bytes: pkt,
                obfusc,
            });
        }
        out
    }

    /// Handle an incoming KADEMLIA2_SEARCH_RES raw payload.
    pub fn on_search_res(&mut self, raw: &[u8], src: SocketAddr) {
        match &mut self.mode {
            SearchMode::Sources { sources, .. } => match packet::parse_search_res_sources(raw) {
                Ok(res) if res.target == self.target => {
                    for s in res.sources {
                        if sources.len() < self.max_results
                            && s.tcp_port != 0
                            && !s.ip.is_unspecified()
                        {
                            sources.push(KadSource {
                                ip: s.ip,
                                tcp_port: s.tcp_port,
                                udp_port: s.udp_port,
                                // The source ID is the peer's user hash in
                                // CUInt128 wire (word-swapped) order; recover the
                                // raw form the TCP-obfuscation RC4 key needs.
                                user_hash: packet::user_hash_from_source_id(&s.id),
                            });
                        }
                    }
                    debug!(sources = sources.len(), %src, "Accumulated Kad2 sources");
                }
                Ok(_) => {}
                Err(e) => trace!(%src, error = %e, "Failed to parse SearchRes as sources"),
            },
            SearchMode::Keyword { hits, .. } => match packet::parse_search_res_keywords(raw) {
                Ok(res) if res.target == self.target => {
                    debug!(count = res.results.len(), %src, "Accumulated Kad2 keyword hits");
                    for entry in res.results {
                        if hits.len() < self.max_results {
                            hits.push(KeywordHit {
                                hash: *entry.file_hash.as_bytes(),
                                name: entry.name,
                                size: entry.size,
                            });
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => trace!(%src, error = %e, "Failed to parse SearchRes as keywords"),
            },
            // A publish never issues a SEARCH, so it gets PUBLISH_RES instead of
            // SEARCH_RES; this path is unreachable for it.
            SearchMode::Publish { .. } => {}
        }
    }

    /// Handle an incoming KADEMLIA2_PUBLISH_RES: count one acknowledged store.
    /// `load` is the node's index saturation (0–100); we don't act on it.
    pub fn on_publish_res(&mut self, _load: u8) {
        if let SearchMode::Publish { published, .. } = &mut self.mode {
            *published += 1;
        }
    }

    fn make_search_packet(&self) -> Vec<u8> {
        match &self.mode {
            SearchMode::Sources { file_size, .. } => {
                encode_search_source_req(&self.target, *file_size)
            }
            SearchMode::Keyword { .. } => encode_search_key_req(&self.target),
            SearchMode::Publish {
                our_id,
                tcp_port,
                udp_port,
                file_size,
                connect_options,
                ..
            } => encode_publish_source_req(
                &self.target,
                our_id,
                *tcp_port,
                *udp_port,
                *file_size,
                *connect_options,
            ),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn initial_reqs(
    target: &KadId,
    candidates: &[Contact],
    alpha: usize,
) -> (
    HashSet<SocketAddrV4>,
    HashMap<SocketAddrV4, KadId>,
    Vec<OutPacket>,
) {
    let mut queried = HashSet::new();
    let mut queried_ids = HashMap::new();
    let mut out = Vec::new();
    for c in candidates.iter().take(alpha) {
        let addr = c.socket_addr_udp();
        queried.insert(addr);
        queried_ids.insert(addr, c.id);
        let pkt = encode_req(2, target, &c.id);
        let obfusc = match c.udp_key {
            Some(key) => ObfuscMode::RecvKey(key),
            None => ObfuscMode::Plain,
        };
        out.push(OutPacket {
            addr: SocketAddr::V4(addr),
            bytes: pkt,
            obfusc,
        });
    }
    (queried, queried_ids, out)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ed2k::Ed2kHash;

    fn make_contact(id_byte: u8, ip: [u8; 4], udp: u16) -> Contact {
        use crate::kad::packet::KadId;
        let mut id = [0u8; 16];
        id[0] = id_byte;
        Contact {
            id: KadId::from_bytes(id),
            ip: std::net::Ipv4Addr::from(ip),
            udp_port: udp,
            tcp_port: udp + 10,
            version: 9,
            udp_key: None,
        }
    }

    #[test]
    fn test_initial_reqs_count() {
        let target = KadId::from_bytes([0u8; 16]);
        let candidates: Vec<_> = (1u8..=10)
            .map(|i| make_contact(i, [i, 0, 0, 1], 4672))
            .collect();
        let (search, pkts) = {
            let (tx, _rx) = oneshot::channel();
            ActiveSearch::new_source_search(
                target,
                0,
                Instant::now() + std::time::Duration::from_secs(60),
                50,
                &candidates,
                5,
                tx,
            )
        };
        assert_eq!(pkts.len(), 5, "alpha=5 → 5 initial REQ packets");
        assert_eq!(search.queried_count(), 5);
    }

    #[test]
    fn test_target_from_hash() {
        let hash = Ed2kHash::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let kid = KadId::from_bytes(*hash.as_bytes());
        assert_eq!(kid.as_bytes()[0], 0xd4);
    }

    #[test]
    fn test_search_config_is_gone() {
        // SearchConfig was removed; verify the old defaults live in KadTaskConfig.
        let cfg = crate::kad::task::KadTaskConfig::default();
        assert_eq!(cfg.alpha, 20);
        assert!(cfg.max_sources > 0);
    }
}
