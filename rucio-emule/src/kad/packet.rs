//! Kad2 UDP packet serialization / deserialization.
//!
//! All multi-byte integers on the wire are **little-endian** (eMule convention).
//!
//! ## Packet framing
//!
//! Every Kad2 UDP datagram starts with two bytes:
//!   - `[0]` = protocol header: `0xe4` for unencrypted Kad2.
//!   - `[1]` = opcode (see [`Opcode`]).
//!
//! Followed by opcode-specific payload.
//!
//! ## Kad2 node ID
//!
//! A 128-bit unsigned integer stored as 16 bytes **little-endian** (the low
//! bytes come first on the wire, matching CUInt128::WriteToFile).

use std::io::{self, Cursor, Read, Write};
use thiserror::Error;

// ── Protocol byte ─────────────────────────────────────────────────────────────

/// Protocol header byte for unencrypted Kad2 UDP packets.
pub const KAD2_PROTO: u8 = 0xe4;

/// Current Kad version we advertise (KADEMLIA_VERSION = 0x09 per eMule opcodes.h).
pub const KAD_VERSION: u8 = 9;

// ── Opcodes ───────────────────────────────────────────────────────────────────

/// Kad2 UDP opcodes (byte `[1]` of every packet).
///
/// Values taken from eMule/aMule source:
/// `src/protocol/kad2/Client2Client/UDP.h` (aMule) and eMule `opcodes.h`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    /// Bootstrap request — payload: empty.
    BootstrapReq = 0x01,
    /// Bootstrap response — payload: KadID(16) + tcp_port(2) + version(1) + count(2) + contacts.
    BootstrapRes = 0x09,
    /// Hello request — payload: contact descriptor.
    HelloReq = 0x11,
    /// Hello response — payload: contact descriptor.
    HelloRes = 0x19,
    /// Hello response ACK.
    HelloResAck = 0x22,
    /// Node lookup request.
    Req = 0x21,
    /// Node lookup response.
    Res = 0x29,
    /// Search source request (find sources for a file hash).
    SearchSourceReq = 0x34,
    /// Search response (keyword or source).
    SearchRes = 0x3b,
    /// Keyword search request.
    SearchKeyReq = 0x33,
    /// Ping.
    Ping = 0x60,
    /// Pong.
    Pong = 0x61,
    /// Publish source request.
    PublishSourceReq = 0x44,
    /// Publish response.
    PublishRes = 0x4b,
    /// Firewall check request — payload: our TCP port (u16). Asks the peer to
    /// open a TCP connection back to us (callback) and tell us our external IP.
    FirewalledReq = 0x50,
    /// Firewall check response — payload: our external IPv4 (u32), as the peer
    /// sees us. Sent regardless of whether the TCP callback succeeded.
    FirewalledRes = 0x58,
    /// Firewall check ack — no payload. Sent by the checked node once it has
    /// received the TCP callback, so the checker knows its probe was useful.
    FirewalledAck = 0x59,
}

impl Opcode {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::BootstrapReq),
            0x09 => Some(Self::BootstrapRes),
            0x11 => Some(Self::HelloReq),
            0x19 => Some(Self::HelloRes),
            0x22 => Some(Self::HelloResAck),
            0x21 => Some(Self::Req),
            0x29 => Some(Self::Res),
            0x34 => Some(Self::SearchSourceReq),
            0x3b => Some(Self::SearchRes),
            0x33 => Some(Self::SearchKeyReq),
            0x60 => Some(Self::Ping),
            0x61 => Some(Self::Pong),
            0x44 => Some(Self::PublishSourceReq),
            0x4b => Some(Self::PublishRes),
            0x50 => Some(Self::FirewalledReq),
            0x58 => Some(Self::FirewalledRes),
            0x59 => Some(Self::FirewalledAck),
            _ => None,
        }
    }
}

// ── KadId ─────────────────────────────────────────────────────────────────────

/// 128-bit Kad node / target identifier, stored as 16 bytes little-endian on wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct KadId([u8; 16]);

impl KadId {
    pub fn zero() -> Self {
        Self([0u8; 16])
    }

    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }

    /// Create a random KadId using OS entropy.
    pub fn random() -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};
        // Simple determinism-free random using time + pid.
        let mut h = DefaultHasher::new();
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut h);
        std::process::id().hash(&mut h);
        let a = h.finish();
        h.finish().hash(&mut h);
        let b = h.finish();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&a.to_le_bytes());
        bytes[8..].copy_from_slice(&b.to_le_bytes());
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// XOR distance between two KadIds (standard Kademlia metric).
    pub fn distance(&self, other: &KadId) -> KadId {
        let mut d = [0u8; 16];
        for (i, byte) in d.iter_mut().enumerate() {
            *byte = self.0[i] ^ other.0[i];
        }
        KadId(d)
    }

    /// Compare two KadIds lexicographically (for sorting by XOR distance).
    pub fn cmp_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// True if `self` is closer to `target` than `other` is.
    pub fn is_closer_to(&self, target: &KadId, other: &KadId) -> bool {
        let da = self.distance(target);
        let db = other.distance(target);
        da.0 < db.0
    }

    /// Read 16 bytes little-endian from reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut b = [0u8; 16];
        r.read_exact(&mut b)?;
        Ok(Self(b))
    }

    /// Write 16 bytes to writer.
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.0)
    }
}

impl std::fmt::Display for KadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

// ── Contact ───────────────────────────────────────────────────────────────────

/// A Kad2 contact (entry in the routing table / bootstrap response).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contact {
    pub id: KadId,
    /// IPv4 address in host byte order.
    pub ip: std::net::Ipv4Addr,
    /// UDP port.
    pub udp_port: u16,
    /// TCP port.
    pub tcp_port: u16,
    /// Kad protocol version (2–11).
    pub version: u8,
    /// UDP obfuscation key received from this peer via HELLO_RES.
    /// None = unknown (no handshake yet or peer has obfuscation disabled).
    pub udp_key: Option<u32>,
}

impl Contact {
    pub fn socket_addr_udp(&self) -> std::net::SocketAddrV4 {
        std::net::SocketAddrV4::new(self.ip, self.udp_port)
    }
}

// ── Packet codec ──────────────────────────────────────────────────────────────

/// Read a u16 little-endian from `r`.
fn read_u16<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}

/// Read a u32 little-endian from `r`.
fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

/// Read a u64 little-endian from `r`.
fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn write_u16<W: Write>(w: &mut W, v: u16) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
#[allow(dead_code)]
fn write_u32<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

/// Incoming parsed Kad2 packet variants (those we need to handle).
#[derive(Debug, Clone)]
pub enum KadPacket {
    BootstrapReq,
    BootstrapRes(BootstrapResPayload),
    HelloReq(HelloPayload),
    HelloRes(HelloPayload),
    Req(ReqPayload),
    Res(ResPayload),
    SearchSourceReq(SearchSourceReqPayload),
    /// Raw 0x2b packet — parsed lazily in the task depending on search mode.
    SearchRes {
        raw: Vec<u8>,
    },
    Ping,
    Pong(u16), // external UDP port echoed back
    /// Firewall check request from a peer — it wants us to TCP-connect back to
    /// `src_ip:tcp_port` and tell it its external IP.
    FirewalledReq {
        tcp_port: u16,
    },
    /// Firewall check response — our external IPv4 as the peer sees us.
    FirewalledRes {
        ip: std::net::Ipv4Addr,
    },
    /// Firewall check ack from a node we probed.
    FirewalledAck,
    Unknown {
        opcode: u8,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub struct BootstrapResPayload {
    pub sender_id: KadId,
    pub tcp_port: u16,
    pub version: u8,
    pub contacts: Vec<Contact>,
}

#[derive(Debug, Clone)]
pub struct HelloPayload {
    pub id: KadId,
    pub tcp_port: u16,
    pub version: u8,
    /// Number of tags (we skip them for simplicity).
    pub tag_count: u8,
    /// UDP obfuscation key advertised by the peer (TAG_UDPKEY = 0x08), if present.
    pub udp_key: Option<u32>,
    /// Our external IPv4 as seen by the peer (TAG_SENDER_IP = 0x09), if present.
    pub sender_ip: Option<std::net::Ipv4Addr>,
}

#[derive(Debug, Clone)]
pub struct ReqPayload {
    pub search_type: u8,
    pub target: KadId,
    pub receiver: KadId,
}

#[derive(Debug, Clone)]
pub struct ResPayload {
    pub target: KadId,
    pub contacts: Vec<Contact>,
}

#[derive(Debug, Clone)]
pub struct SearchSourceReqPayload {
    pub target: KadId,
    pub start_position: u16,
    pub file_size: u64,
}

/// A single source returned by a SEARCH_RES packet.
#[derive(Debug, Clone)]
pub struct SourceEntry {
    pub id: KadId,
    /// IPv4 address.
    pub ip: std::net::Ipv4Addr,
    /// TCP port.
    pub tcp_port: u16,
    /// UDP port.
    pub udp_port: u16,
}

#[derive(Debug, Clone)]
pub struct SearchResPayload {
    pub sender_id: KadId,
    pub target: KadId,
    pub sources: Vec<SourceEntry>,
}

/// One result entry from a keyword search (`KADEMLIA2_SEARCH_RES` in response to
/// `KADEMLIA2_SEARCH_KEY_REQ`).  The `answer` KadID is the file's ed2k hash.
#[derive(Debug, Clone)]
pub struct KeywordResultEntry {
    /// The file's ed2k/MD4 hash (16 bytes, same byte order as on wire).
    pub file_hash: KadId,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct KeywordResPayload {
    pub sender_id: KadId,
    pub target: KadId,
    pub results: Vec<KeywordResultEntry>,
}

// ── Decode ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("packet too short")]
    TooShort,
    #[error("wrong protocol byte: 0x{0:02x}")]
    WrongProto(u8),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("decompression error")]
    Decompress,
}

/// Decode a raw UDP datagram into a [`KadPacket`].
///
/// Handles both plain Kad2 (`0xe4`) and packed Kad2 (`0xe5`) frames.
/// Packed frames have a zlib-compressed payload starting at byte 2.
pub fn decode(data: &[u8]) -> Result<KadPacket, PacketError> {
    if data.len() < 2 {
        return Err(PacketError::TooShort);
    }
    // 0xe5 = OP_KADEMLIAPACKEDPROT: zlib-compressed Kad2 packet.
    // Decompress and re-frame as a plain 0xe4 packet.
    if data[0] == 0xe5 {
        use flate2::read::ZlibDecoder;
        use std::io::Read as _;
        let mut dec = ZlibDecoder::new(&data[2..]);
        let mut decompressed = Vec::new();
        dec.read_to_end(&mut decompressed)
            .map_err(|_| PacketError::Decompress)?;
        // Re-frame: prepend KAD2_PROTO so decode_payload can be called recursively.
        let mut reframed = Vec::with_capacity(2 + decompressed.len());
        reframed.push(KAD2_PROTO);
        reframed.push(data[1]); // opcode is outside the compressed region
        reframed.extend_from_slice(&decompressed);
        return decode(&reframed);
    }
    if data[0] != KAD2_PROTO {
        return Err(PacketError::WrongProto(data[0]));
    }
    let opcode = data[1];
    let payload = &data[2..];
    let mut cur = Cursor::new(payload);

    let pkt = match Opcode::from_byte(opcode) {
        Some(Opcode::BootstrapReq) => KadPacket::BootstrapReq,

        Some(Opcode::BootstrapRes) => {
            let sender_id = KadId::read_from(&mut cur)?;
            let tcp_port = read_u16(&mut cur)?;
            let version = {
                let mut b = [0u8];
                cur.read_exact(&mut b)?;
                b[0]
            };
            let count = read_u16(&mut cur)?;
            let mut contacts = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let id = KadId::read_from(&mut cur)?;
                let ip_raw = read_u32(&mut cur)?;
                let udp_port = read_u16(&mut cur)?;
                let tcp_p = read_u16(&mut cur)?;
                let ver = {
                    let mut b = [0u8];
                    cur.read_exact(&mut b)?;
                    b[0]
                };
                if ver >= 2 {
                    // Only Kad2 contacts.
                    contacts.push(Contact {
                        id,
                        ip: std::net::Ipv4Addr::from(ip_raw.to_be_bytes()),
                        udp_port,
                        tcp_port: tcp_p,
                        version: ver,
                        udp_key: None,
                    });
                }
            }
            KadPacket::BootstrapRes(BootstrapResPayload {
                sender_id,
                tcp_port,
                version,
                contacts,
            })
        }

        Some(Opcode::HelloReq) | Some(Opcode::HelloRes) => {
            let id = KadId::read_from(&mut cur)?;
            let tcp_port = read_u16(&mut cur)?;
            let version = {
                let mut b = [0u8];
                cur.read_exact(&mut b)?;
                b[0]
            };
            let tag_count = {
                let mut b = [0u8];
                cur.read_exact(&mut b)?;
                b[0]
            };
            // Parse tags looking for TAG_UDPKEY and TAG_SENDER_IP.
            // Wire format: type(1) + name_len(2 LE) + name(n) + value
            let mut udp_key: Option<u32> = None;
            let mut sender_ip: Option<std::net::Ipv4Addr> = None;
            for _ in 0..tag_count {
                let tag_type = {
                    let mut b = [0u8];
                    if cur.read_exact(&mut b).is_err() {
                        break;
                    }
                    b[0]
                };
                // Read name: uint16 length + bytes (aMule SafeFile.cpp WriteTag)
                let name_len = match read_u16(&mut cur) {
                    Ok(v) => v as usize,
                    Err(_) => break,
                };
                let mut name = vec![0u8; name_len];
                if cur.read_exact(&mut name).is_err() {
                    break;
                }
                match (tag_type, name.as_slice()) {
                    // TAG_UDPKEY: TAGTYPE_UINT32 (0x03), name=[0x08]
                    (0x03, [0x08]) => {
                        udp_key = read_u32(&mut cur).ok();
                    }
                    // TAG_SENDER_IP: TAGTYPE_UINT32 (0x03), name=[0x09]
                    (0x03, [0x09]) => {
                        if let Ok(raw) = read_u32(&mut cur) {
                            sender_ip = Some(std::net::Ipv4Addr::from(raw.to_be_bytes()));
                        }
                    }
                    // Skip any other tag value
                    (t, _) => {
                        if skip_tag_value(&mut cur, t).is_err() {
                            break;
                        }
                    }
                }
            }
            let p = HelloPayload {
                id,
                tcp_port,
                version,
                tag_count,
                udp_key,
                sender_ip,
            };
            if opcode == Opcode::HelloReq as u8 {
                KadPacket::HelloReq(p)
            } else {
                KadPacket::HelloRes(p)
            }
        }

        Some(Opcode::Req) => {
            let mut b = [0u8];
            cur.read_exact(&mut b)?;
            let search_type = b[0] & 0x1f;
            let target = KadId::read_from(&mut cur)?;
            let receiver = KadId::read_from(&mut cur)?;
            KadPacket::Req(ReqPayload {
                search_type,
                target,
                receiver,
            })
        }

        Some(Opcode::Res) => {
            let target = KadId::read_from(&mut cur)?;
            let count = {
                let mut b = [0u8];
                cur.read_exact(&mut b)?;
                b[0]
            };
            let mut contacts = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let id = KadId::read_from(&mut cur)?;
                let ip_raw = read_u32(&mut cur)?;
                let udp_port = read_u16(&mut cur)?;
                let tcp_p = read_u16(&mut cur)?;
                let ver = {
                    let mut b = [0u8];
                    cur.read_exact(&mut b)?;
                    b[0]
                };
                if ver >= 2 {
                    contacts.push(Contact {
                        id,
                        ip: std::net::Ipv4Addr::from(ip_raw.to_be_bytes()),
                        udp_port,
                        tcp_port: tcp_p,
                        version: ver,
                        udp_key: None,
                    });
                }
                // ver < 2 = Kad1-only peer; we still consume the bytes but skip adding.
            }
            KadPacket::Res(ResPayload { target, contacts })
        }

        Some(Opcode::SearchSourceReq) => {
            let target = KadId::read_from(&mut cur)?;
            let raw = read_u16(&mut cur)?;
            let start_position = raw & 0x7fff;
            let file_size = read_u64(&mut cur)?;
            KadPacket::SearchSourceReq(SearchSourceReqPayload {
                target,
                start_position,
                file_size,
            })
        }

        Some(Opcode::SearchRes) => {
            // Store raw payload; let the task parse it based on the active search mode.
            KadPacket::SearchRes {
                raw: payload.to_vec(),
            }
        }

        Some(Opcode::Ping) => KadPacket::Ping,

        Some(Opcode::FirewalledReq) => {
            // Payload: TCP port (u16 LE). Some senders append extra bytes; we
            // only need the port.
            let port = read_u16(&mut cur).unwrap_or(0);
            KadPacket::FirewalledReq { tcp_port: port }
        }
        Some(Opcode::FirewalledRes) => {
            // Payload: our external IPv4 (u32). Encode/decode mirror so a value
            // we write round-trips; real eMule sends it the same way.
            match read_u32(&mut cur) {
                Ok(raw) => KadPacket::FirewalledRes {
                    ip: std::net::Ipv4Addr::from(raw.to_be_bytes()),
                },
                Err(_) => KadPacket::Unknown {
                    opcode,
                    payload: Vec::new(),
                },
            }
        }
        Some(Opcode::FirewalledAck) => KadPacket::FirewalledAck,
        Some(Opcode::Pong) => {
            let port = if payload.len() >= 2 {
                read_u16(&mut cur).unwrap_or(0)
            } else {
                0
            };
            KadPacket::Pong(port)
        }

        _ => KadPacket::Unknown {
            opcode,
            payload: payload.to_vec(),
        },
    };
    Ok(pkt)
}

/// Parse a `KADEMLIA2_SEARCH_RES` payload as keyword search results.
///
/// Same wire format as source-search results but tag list contains file metadata
/// (name, size) instead of IP/port.  Call this instead of the source variant when
/// the active search was a keyword search.
pub fn parse_keyword_res(payload: &[u8]) -> io::Result<KeywordResPayload> {
    let mut cur = Cursor::new(payload);
    let sender_id = KadId::read_from(&mut cur)?;
    let target = KadId::read_from(&mut cur)?;
    let count = read_u16(&mut cur)?;
    let mut results = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let file_hash = KadId::read_from(&mut cur)?;
        let (name, size) = read_keyword_tags(&mut cur).unwrap_or_default();
        results.push(KeywordResultEntry {
            file_hash,
            name,
            size,
        });
    }
    Ok(KeywordResPayload {
        sender_id,
        target,
        results,
    })
}

// ── Tag constants (aMule TagTypes.h) ──────────────────────────────────────────
// TAGTYPE_HASH16 = 0x01  (16 bytes)
// TAGTYPE_STRING = 0x02  (uint16 len + bytes)
// TAGTYPE_UINT32 = 0x03  (4 bytes LE)
// TAGTYPE_FLOAT32= 0x04  (4 bytes)
// TAGTYPE_UINT16 = 0x08  (2 bytes LE)
// TAGTYPE_UINT8  = 0x09  (1 byte)
// TAGTYPE_UINT64 = 0x0B  (8 bytes LE)
// TAGTYPE_STR1..N= 0x11..0x26  (inline string of length N-0x10)
//
// Tag name format (aMule SafeFile.cpp WriteTag):
//   If name is a string:  uint16(len) + bytes
//   If name is a numeric ID:  uint16(1) + uint8(id)
// In practice Kad2 uses both; we always read uint16+bytes.
//
// Source entry tag names (FileTags.h):
//   TAG_SOURCEIP    = "\xFE"  (uint32 LE, host-byte-order)
//   TAG_SOURCEPORT  = "\xFD"  (uint16 LE, TCP port)
//   TAG_SOURCEUPORT = "\xFC"  (uint16 LE, UDP port)
//   TAG_FILESIZE    = "\x02"  (varint / uint64)

/// Skip the value of a tag given its type byte; return Err on unknown type.
fn skip_tag_value<R: Read>(r: &mut R, type_byte: u8) -> io::Result<()> {
    match type_byte {
        0x01 => {
            // TAGTYPE_HASH16
            let mut b = [0u8; 16];
            r.read_exact(&mut b)
        }
        0x02 => {
            // TAGTYPE_STRING: uint16 len + bytes
            let len = read_u16(r)? as usize;
            let mut b = vec![0u8; len];
            r.read_exact(&mut b)
        }
        0x03 | 0x04 => {
            // TAGTYPE_UINT32 / TAGTYPE_FLOAT32
            let mut b = [0u8; 4];
            r.read_exact(&mut b)
        }
        0x05..=0x07 => {
            // BOOL, BOOLARRAY, BLOB — treat as 1 byte to avoid misalign
            let mut b = [0u8; 1];
            r.read_exact(&mut b)
        }
        0x08 => {
            // TAGTYPE_UINT16
            let mut b = [0u8; 2];
            r.read_exact(&mut b)
        }
        0x09 => {
            // TAGTYPE_UINT8
            let mut b = [0u8; 1];
            r.read_exact(&mut b)
        }
        0x0a => {
            // TAGTYPE_BSOB: 1-byte size + bytes
            let mut sb = [0u8; 1];
            r.read_exact(&mut sb)?;
            let mut b = vec![0u8; sb[0] as usize];
            r.read_exact(&mut b)
        }
        0x0b => {
            // TAGTYPE_UINT64
            let mut b = [0u8; 8];
            r.read_exact(&mut b)
        }
        n if (0x11..=0x26).contains(&n) => {
            // TAGTYPE_STR1..STR22 — inline string of fixed length (type - 0x10)
            let len = (n - 0x10) as usize;
            let mut b = vec![0u8; len];
            r.read_exact(&mut b)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown tag type 0x{type_byte:02x}"),
        )),
    }
}

/// Read a Kad tag list and extract source IP / TCP port / UDP port.
///
/// Wire format per aMule (SafeFile.cpp WriteTag):
///   tag_count(1)  — uint8
///   per tag:
///     type(1)
///     name_len(2 LE) + name_bytes  — always uint16 length prefix
///     value  — depends on type
///
/// Tag names for source entries (FileTags.h):
///   `[0xFE]` = TAG_SOURCEIP   (TAGTYPE_UINT32, host-order LE)
///   `[0xFD]` = TAG_SOURCEPORT (TAGTYPE_UINT16, TCP port)
///   `[0xFC]` = TAG_SOURCEUPORT(TAGTYPE_UINT16, UDP port)
fn read_source_tags<R: Read>(r: &mut R) -> io::Result<(std::net::Ipv4Addr, u16, u16)> {
    let count = {
        let mut b = [0u8];
        r.read_exact(&mut b)?;
        b[0]
    };
    let mut ip = std::net::Ipv4Addr::UNSPECIFIED;
    let mut tcp_port: u16 = 0;
    let mut udp_port: u16 = 0;

    for _ in 0..count {
        let type_byte = {
            let mut b = [0u8];
            r.read_exact(&mut b)?;
            b[0]
        };
        // Read name: uint16 length + bytes
        let name_len = read_u16(r)? as usize;
        let mut name = vec![0u8; name_len];
        r.read_exact(&mut name)?;

        match (type_byte, name.as_slice()) {
            // TAG_SOURCEIP: uint32 in network byte order, written as LE.
            // The wire value is the raw network-order IP (as from socket APIs),
            // so u32::from_le_bytes gives the network-order value, which
            // Ipv4Addr::from(u32) correctly interprets as big-endian.
            (0x03, [0xfe]) => {
                let v = read_u32(r)?;
                ip = std::net::Ipv4Addr::from(v);
            }
            // TAG_SOURCEPORT: uint16 LE
            (0x08, [0xfd]) => {
                tcp_port = read_u16(r)?;
            }
            // TAG_SOURCEUPORT: uint16 LE
            (0x08, [0xfc]) => {
                udp_port = read_u16(r)?;
            }
            // Any other tag: skip the value
            (t, _) => {
                skip_tag_value(r, t)?;
            }
        }
    }
    Ok((ip, tcp_port, udp_port))
}

/// Read a keyword-result tag list and extract file name and size.
///
/// Tag names (opcodes.h):
///   `[0x01]` = TAG_FILENAME (TAGTYPE_STRING or TAGTYPE_STR1..N)
///   `[0x02]` = TAG_FILESIZE (TAGTYPE_UINT32 or TAGTYPE_UINT64)
fn read_keyword_tags<R: Read>(r: &mut R) -> io::Result<(String, u64)> {
    let count = {
        let mut b = [0u8];
        r.read_exact(&mut b)?;
        b[0]
    };
    let mut name = String::new();
    let mut size: u64 = 0;

    for _ in 0..count {
        let type_byte = {
            let mut b = [0u8];
            r.read_exact(&mut b)?;
            b[0]
        };
        // Read name: uint16 length + bytes
        let name_len = read_u16(r)? as usize;
        let mut tag_name = vec![0u8; name_len];
        r.read_exact(&mut tag_name)?;

        match (type_byte, tag_name.as_slice()) {
            // TAG_FILENAME: TAGTYPE_STRING (0x02)
            (0x02, [0x01]) => {
                let len = read_u16(r)? as usize;
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
                name = String::from_utf8_lossy(&buf).into_owned();
            }
            // TAG_FILENAME: TAGTYPE_STR1..STR22 (0x11..0x26) — inline length
            (n, [0x01]) if (0x11..=0x26).contains(&n) => {
                let len = (n - 0x10) as usize;
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
                name = String::from_utf8_lossy(&buf).into_owned();
            }
            // TAG_FILESIZE: TAGTYPE_UINT32 (0x03)
            (0x03, [0x02]) => {
                size = read_u32(r)? as u64;
            }
            // TAG_FILESIZE: TAGTYPE_UINT64 (0x0b)
            (0x0b, [0x02]) => {
                let mut b = [0u8; 8];
                r.read_exact(&mut b)?;
                size = u64::from_le_bytes(b);
            }
            // Any other tag: skip the value
            (t, _) => {
                skip_tag_value(r, t)?;
            }
        }
    }
    Ok((name, size))
}

// ── Encode ────────────────────────────────────────────────────────────────────

/// Build a `KADEMLIA2_BOOTSTRAP_REQ` packet (2 bytes total).
pub fn encode_bootstrap_req() -> Vec<u8> {
    vec![KAD2_PROTO, Opcode::BootstrapReq as u8]
}

/// Parse a `KADEMLIA2_SEARCH_RES` (0x3b) payload as a **source** search result.
///
/// Wire format (from aMule `Indexed.cpp SendValidSourceResult`):
///   sender_id(16) + target(16) + count(2) + [count × (answer_id(16) + tag_list)]
pub fn parse_search_res_sources(payload: &[u8]) -> io::Result<SearchResPayload> {
    let mut cur = Cursor::new(payload);
    let sender_id = KadId::read_from(&mut cur)?;
    let target = KadId::read_from(&mut cur)?;
    let count = read_u16(&mut cur)?;
    let mut sources = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let id = KadId::read_from(&mut cur)?;
        let (ip, tcp_port, udp_port) = read_source_tags(&mut cur)?;
        sources.push(SourceEntry {
            id,
            ip,
            tcp_port,
            udp_port,
        });
    }
    Ok(SearchResPayload {
        sender_id,
        target,
        sources,
    })
}

/// Parse a `KADEMLIA2_SEARCH_RES` (0x3b) payload as a **keyword** search result.
///
/// Wire format (from aMule `Indexed.cpp SendValidKeywordResult`):
///   sender_id(16) + target(16) + count(2) + [count × (answer_id(16) + tag_list)]
/// The tag list contains file metadata (name, size, etc.) not IP/port.
pub fn parse_search_res_keywords(payload: &[u8]) -> io::Result<KeywordResPayload> {
    let mut cur = Cursor::new(payload);
    let sender_id = KadId::read_from(&mut cur)?;
    let target = KadId::read_from(&mut cur)?;
    let count = read_u16(&mut cur)?;
    let mut results = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let file_hash = KadId::read_from(&mut cur)?;
        let (name, size) = read_keyword_tags(&mut cur).unwrap_or_default();
        results.push(KeywordResultEntry {
            file_hash,
            name,
            size,
        });
    }
    Ok(KeywordResPayload {
        sender_id,
        target,
        results,
    })
}

/// Build a `KADEMLIA2_SEARCH_KEY_REQ` (opcode 0x33) for a keyword search.
///
/// `target` must be the output of [`keyword_target`] (4-byte-chunk-reversed MD4).
/// `start_pos = 0` — no pagination, no search term filter.
pub fn encode_search_key_req(target: &KadId) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::SearchKeyReq as u8];
    target.write_to(&mut buf).unwrap();
    // start_position = 0x0000 (no search terms filter)
    write_u16(&mut buf, 0u16).unwrap();
    buf
}

/// Convert a raw 16-byte MD4 hash to a [`KadId`] in eMule's CUInt128 wire format.
///
/// eMule/aMule store CUInt128 on the wire as four consecutive LE uint32s in
/// big-endian word order (`SetValueBE` + `WriteUInt32`), so each 4-byte chunk
/// of the MD4 output is byte-reversed on the wire.  All hash-derived KadIds
/// (keyword targets, file source targets) must use this encoding to land in the
/// correct part of the Kad keyspace.
pub fn kad_id_from_hash(hash: &[u8; 16]) -> KadId {
    let mut bytes = [0u8; 16];
    for i in 0..4 {
        bytes[i * 4] = hash[i * 4 + 3];
        bytes[i * 4 + 1] = hash[i * 4 + 2];
        bytes[i * 4 + 2] = hash[i * 4 + 1];
        bytes[i * 4 + 3] = hash[i * 4];
    }
    KadId::from_bytes(bytes)
}

/// Compute the Kad target for a keyword search: `MD4(keyword_utf8)`.
///
/// **The caller is responsible for normalizing `keyword` first** (lowercase
/// only, via `rucio_core::protocol::search::lowercase_keyword`). eMule clients
/// only lowercase before publishing — they do not fold diacritics — so the DHT
/// is keyed by the lowercased word with accents intact; folding the query would
/// miss those entries. Kad keyword search is therefore accent-sensitive.
pub fn keyword_target(keyword: &str) -> KadId {
    use md4::{Digest, Md4};
    let mut h = Md4::new();
    h.update(keyword.as_bytes());
    let hash: [u8; 16] = h.finalize().into();
    kad_id_from_hash(&hash)
}

/// Build a `KADEMLIA2_HELLO_REQ` advertising our node details, including our UDPKey.
///
/// `our_udp_key`: our u32 obfuscation key (advertised so peers can send us obfuscated packets).
pub fn encode_hello_req(our_id: &KadId, tcp_port: u16, our_udp_key: u32) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::HelloReq as u8];
    our_id.write_to(&mut buf).unwrap();
    write_u16(&mut buf, tcp_port).unwrap();
    buf.push(KAD_VERSION);
    // 1 tag: TAG_UDPKEY (type=TAGTYPE_UINT32=0x03, name=uint16(1)+[0x08])
    buf.push(1); // tag count
    buf.push(0x03); // type = TAGTYPE_UINT32
    write_u16(&mut buf, 1).unwrap(); // name length = 1 (as uint16 per aMule wire format)
    buf.push(0x08); // name = TAG_UDPKEY
    write_u32(&mut buf, our_udp_key).unwrap();
    buf
}

/// Build a `KADEMLIA2_REQ` node-lookup packet.
///
/// `search_type` = how many contacts to return (e.g. 2 = closest 2).
pub fn encode_req(search_type: u8, target: &KadId, our_id: &KadId) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::Req as u8];
    buf.push(search_type);
    target.write_to(&mut buf).unwrap();
    our_id.write_to(&mut buf).unwrap();
    buf
}

/// Build a `KADEMLIA2_SEARCH_SOURCE_REQ` to find sources for a file.
///
/// `target` is the ed2k hash interpreted as a KadId (little-endian 16 bytes).
pub fn encode_search_source_req(target: &KadId, file_size: u64) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::SearchSourceReq as u8];
    target.write_to(&mut buf).unwrap();
    write_u16(&mut buf, 0u16).unwrap(); // start_position = 0
    buf.extend_from_slice(&file_size.to_le_bytes());
    buf
}

/// Build a `KADEMLIA_FIREWALLED_REQ` (0x50): asks the peer to TCP-connect back
/// to us on `tcp_port` and report our external IP. Payload is the port (u16 LE).
pub fn encode_firewalled_req(tcp_port: u16) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::FirewalledReq as u8];
    write_u16(&mut buf, tcp_port).unwrap();
    buf
}

/// Build a `KADEMLIA_FIREWALLED_RES` (0x58): tells the peer its external IPv4.
pub fn encode_firewalled_res(ip: std::net::Ipv4Addr) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::FirewalledRes as u8];
    write_u32(&mut buf, u32::from_be_bytes(ip.octets())).unwrap();
    buf
}

/// Build a `KADEMLIA_FIREWALLED_ACK_RES` (0x59): no payload.
pub fn encode_firewalled_ack() -> Vec<u8> {
    vec![KAD2_PROTO, Opcode::FirewalledAck as u8]
}

/// Build a `KADEMLIA2_PING` packet.
pub fn encode_ping() -> Vec<u8> {
    vec![KAD2_PROTO, Opcode::Ping as u8]
}

/// Build a `KADEMLIA2_PONG` packet, echoing back the caller's external UDP port.
pub fn encode_pong(external_port: u16) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::Pong as u8];
    write_u16(&mut buf, external_port).unwrap();
    buf
}

/// Build a `KADEMLIA2_HELLO_RES` advertising our node.
///
/// When `sender_ip` is given, echo it back as `TAG_SENDER_IP` so the peer can
/// learn its own public address without running a firewall check — this is how
/// a node behind no UPnP/configured IP discovers its external IP from us.
pub fn encode_hello_res(
    our_id: &KadId,
    tcp_port: u16,
    sender_ip: Option<std::net::Ipv4Addr>,
) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::HelloRes as u8];
    our_id.write_to(&mut buf).unwrap();
    write_u16(&mut buf, tcp_port).unwrap();
    buf.push(KAD_VERSION);
    match sender_ip {
        Some(ip) if !ip.is_unspecified() => {
            buf.push(1); // tag count = 1
            // TAG_SENDER_IP: TAGTYPE_UINT32(0x03) + name_len(2 LE) + name[0x09]
            // + value(u32). Encoded so our decoder recovers the octets exactly
            // (read_u32 LE → to_be_bytes → Ipv4Addr).
            buf.push(0x03);
            write_u16(&mut buf, 1).unwrap();
            buf.push(0x09);
            write_u32(&mut buf, u32::from_be_bytes(ip.octets())).unwrap();
        }
        _ => buf.push(0), // tag count = 0
    }
    buf
}

/// Build a `KADEMLIA2_HELLO_RES_ACK` packet.
///
/// Wire format per eMule: `[NodeID(16)][tag_count=0(1)]` — minimum 17 bytes;
/// shorter packets are rejected by eMule's `Process_KADEMLIA2_HELLO_RES_ACK`.
pub fn encode_hello_res_ack(our_id: &KadId) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::HelloResAck as u8];
    our_id.write_to(&mut buf).unwrap();
    buf.push(0); // tag count = 0
    buf
}

/// Build a `KADEMLIA2_BOOTSTRAP_RES` with the given contacts.
///
/// Sends up to 20 contacts from the routing table.
pub fn encode_bootstrap_res(our_id: &KadId, tcp_port: u16, contacts: &[Contact]) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::BootstrapRes as u8];
    our_id.write_to(&mut buf).unwrap();
    write_u16(&mut buf, tcp_port).unwrap();
    buf.push(KAD_VERSION);
    let count = contacts.len().min(20) as u16;
    write_u16(&mut buf, count).unwrap();
    for c in contacts.iter().take(20) {
        c.id.write_to(&mut buf).unwrap();
        let ip_u32 = u32::from_be_bytes(c.ip.octets());
        buf.extend_from_slice(&ip_u32.to_le_bytes());
        write_u16(&mut buf, c.udp_port).unwrap();
        write_u16(&mut buf, c.tcp_port).unwrap();
        buf.push(c.version);
    }
    buf
}

/// Build a `KADEMLIA2_RES` node-lookup response.
pub fn encode_res(target: &KadId, contacts: &[Contact]) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::Res as u8];
    target.write_to(&mut buf).unwrap();
    let count = contacts.len().min(20) as u8;
    buf.push(count);
    for c in contacts.iter().take(20) {
        c.id.write_to(&mut buf).unwrap();
        let ip_u32 = u32::from_be_bytes(c.ip.octets());
        buf.extend_from_slice(&ip_u32.to_le_bytes());
        write_u16(&mut buf, c.udp_port).unwrap();
        write_u16(&mut buf, c.tcp_port).unwrap();
        buf.push(c.version);
    }
    buf
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_req_roundtrip() {
        let pkt = encode_bootstrap_req();
        assert_eq!(pkt, [KAD2_PROTO, 0x01]);
        let decoded = decode(&pkt).unwrap();
        assert!(matches!(decoded, KadPacket::BootstrapReq));
    }

    #[test]
    fn test_ping_roundtrip() {
        let pkt = encode_ping();
        let decoded = decode(&pkt).unwrap();
        assert!(matches!(decoded, KadPacket::Ping));
    }

    #[test]
    fn test_firewalled_req_roundtrip() {
        let pkt = encode_firewalled_req(4662);
        assert_eq!(pkt[1], Opcode::FirewalledReq as u8); // 0x50
        let decoded = decode(&pkt).unwrap();
        assert!(matches!(
            decoded,
            KadPacket::FirewalledReq { tcp_port: 4662 }
        ));
    }

    #[test]
    fn test_firewalled_res_roundtrip() {
        let ip = std::net::Ipv4Addr::new(203, 0, 113, 7);
        let pkt = encode_firewalled_res(ip);
        assert_eq!(pkt[1], Opcode::FirewalledRes as u8); // 0x58
        let decoded = decode(&pkt).unwrap();
        assert!(matches!(decoded, KadPacket::FirewalledRes { ip: got } if got == ip));
    }

    #[test]
    fn test_firewalled_ack_roundtrip() {
        let pkt = encode_firewalled_ack();
        assert_eq!(pkt[1], Opcode::FirewalledAck as u8); // 0x59
        assert!(matches!(decode(&pkt).unwrap(), KadPacket::FirewalledAck));
    }

    #[test]
    fn test_wrong_proto() {
        let pkt = [0xe3, 0x01];
        assert!(matches!(decode(&pkt), Err(PacketError::WrongProto(0xe3))));
    }

    #[test]
    fn test_too_short() {
        assert!(matches!(decode(&[]), Err(PacketError::TooShort)));
        assert!(matches!(decode(&[0xe4]), Err(PacketError::TooShort)));
    }

    #[test]
    fn test_kad_id_distance() {
        let a = KadId::from_bytes([0xff; 16]);
        let b = KadId::from_bytes([0x00; 16]);
        let d = a.distance(&b);
        assert_eq!(d.0, [0xff; 16]);

        let same = a.distance(&a);
        assert_eq!(same.0, [0x00; 16]);
    }

    #[test]
    fn test_encode_search_source_req() {
        let target = KadId::from_bytes([1u8; 16]);
        let pkt = encode_search_source_req(&target, 12345678);
        assert_eq!(pkt[0], KAD2_PROTO);
        assert_eq!(pkt[1], Opcode::SearchSourceReq as u8);
        // 16 bytes target + 2 bytes start + 8 bytes size = 26 payload bytes
        assert_eq!(pkt.len(), 2 + 16 + 2 + 8);
    }

    /// Regression test: contact IPs in BootstrapRes and Res must survive an
    /// encode → decode roundtrip with the correct byte order (LE on the wire).
    #[test]
    fn test_bootstrap_res_ip_roundtrip() {
        use std::net::Ipv4Addr;

        let our_id = KadId::from_bytes([0xab; 16]);
        let contact = Contact {
            id: KadId::from_bytes([0x01; 16]),
            ip: Ipv4Addr::new(192, 168, 1, 100),
            udp_port: 4672,
            tcp_port: 4662,
            version: 11,
            udp_key: None,
        };

        let pkt = encode_bootstrap_res(&our_id, 4662, std::slice::from_ref(&contact));
        let decoded = decode(&pkt).unwrap();

        let KadPacket::BootstrapRes(res) = decoded else {
            panic!("expected BootstrapRes");
        };
        assert_eq!(res.contacts.len(), 1);
        assert_eq!(res.contacts[0].ip, Ipv4Addr::new(192, 168, 1, 100));
        assert_eq!(res.contacts[0].udp_port, 4672);
    }

    /// Same check for Res (node-lookup response).
    #[test]
    fn test_res_ip_roundtrip() {
        use std::net::Ipv4Addr;

        let target = KadId::from_bytes([0x77; 16]);
        let contact = Contact {
            id: KadId::from_bytes([0x02; 16]),
            ip: Ipv4Addr::new(10, 0, 0, 1),
            udp_port: 4672,
            tcp_port: 4662,
            version: 11,
            udp_key: None,
        };

        let pkt = encode_res(&target, std::slice::from_ref(&contact));
        let decoded = decode(&pkt).unwrap();

        let KadPacket::Res(res) = decoded else {
            panic!("expected Res");
        };
        assert_eq!(res.contacts.len(), 1);
        assert_eq!(res.contacts[0].ip, Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(res.contacts[0].udp_port, 4672);
    }

    /// Regression: the Kad search target for a file is the raw MD4 bytes
    /// unchanged — eMule's SetValueBE+ToByteArray is an identity operation,
    /// so KadId::from_bytes(*md4) is the correct transformation.
    #[test]
    fn test_file_search_target_is_raw_md4() {
        let md4: [u8; 16] = [
            0x0c, 0x20, 0xe9, 0xeb, 0x26, 0x6c, 0xfd, 0x2e, 0xb5, 0x70, 0x9c, 0xf1, 0x83, 0x4e,
            0x09, 0x86,
        ];
        let kad = KadId::from_bytes(md4);
        // The wire bytes must equal the original MD4 — no transformation.
        assert_eq!(kad.as_bytes(), &md4);
    }
}
