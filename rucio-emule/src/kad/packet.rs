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

/// Current Kad version we advertise (version 11 is the last common Kad2 version).
pub const KAD_VERSION: u8 = 11;

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
    HelloRes = 0x12,
    /// Hello response ACK.
    HelloResAck = 0x22,
    /// Node lookup request.
    Req = 0x21,
    /// Node lookup response.
    Res = 0x29,
    /// Search source request (find sources for a file hash).
    SearchSourceReq = 0x19,
    /// Search response.
    SearchRes = 0x2b,
    /// Ping.
    Ping = 0x60,
    /// Pong.
    Pong = 0x61,
    /// Publish source request.
    PublishSourceReq = 0x35,
    /// Publish response.
    PublishRes = 0x38,
}

impl Opcode {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::BootstrapReq),
            0x09 => Some(Self::BootstrapRes),
            0x11 => Some(Self::HelloReq),
            0x12 => Some(Self::HelloRes),
            0x22 => Some(Self::HelloResAck),
            0x21 => Some(Self::Req),
            0x29 => Some(Self::Res),
            0x19 => Some(Self::SearchSourceReq),
            0x2b => Some(Self::SearchRes),
            0x60 => Some(Self::Ping),
            0x61 => Some(Self::Pong),
            0x35 => Some(Self::PublishSourceReq),
            0x38 => Some(Self::PublishRes),
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

    /// Convert an ed2k MD4 hash to the Kad search target.
    ///
    /// eMule stores `CUInt128` values with its four u32 chunks in big-endian
    /// order internally, and uses `SetValueBE` to load an MD4 hash.
    /// `SetValueBE` maps the first 4 bytes of the MD4 into the *highest*
    /// u32 chunk (index 3) and so on, then `ToByteArray` (the wire serialiser)
    /// reverses that back.  The net effect on the wire is that the four 4-byte
    /// groups of the MD4 hash are written in **reverse order**:
    /// `[bytes 12..15][bytes 8..11][bytes 4..7][bytes 0..3]`.
    ///
    /// Call this instead of `KadId::from_bytes` when building a search target
    /// from an ed2k (MD4) hash.
    pub fn from_ed2k_hash(md4: &[u8; 16]) -> Self {
        let mut out = [0u8; 16];
        out[0..4].copy_from_slice(&md4[12..16]);
        out[4..8].copy_from_slice(&md4[8..12]);
        out[8..12].copy_from_slice(&md4[4..8]);
        out[12..16].copy_from_slice(&md4[0..4]);
        Self(out)
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
    SearchRes(SearchResPayload),
    Ping,
    Pong(u16), // external UDP port echoed back
    Unknown { opcode: u8, payload: Vec<u8> },
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
            let payload = HelloPayload {
                id,
                tcp_port,
                version,
                tag_count,
            };
            if opcode == Opcode::HelloReq as u8 {
                KadPacket::HelloReq(payload)
            } else {
                KadPacket::HelloRes(payload)
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
                    });
                }
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
            // KADEMLIA2_SEARCH_RES: sender_id(16) + target(16) + count(2) + entries
            let sender_id = KadId::read_from(&mut cur)?;
            let target = KadId::read_from(&mut cur)?;
            let count = read_u16(&mut cur)?;
            let mut sources = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let id = KadId::read_from(&mut cur)?;
                // Tags: read tag list (simplified — just grab the source IP/ports from tags)
                // Full tag parsing is complex; for source search we read the answer id and
                // attempt to extract TAG_SOURCEIP (0x01) / TAG_SOURCEPORT (0x02) / TAG_SOURCEUPORT (0x03).
                let (ip, tcp_port, udp_port) = read_source_tags(&mut cur)?;
                sources.push(SourceEntry {
                    id,
                    ip,
                    tcp_port,
                    udp_port,
                });
            }
            KadPacket::SearchRes(SearchResPayload {
                sender_id,
                target,
                sources,
            })
        }

        Some(Opcode::Ping) => KadPacket::Ping,

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

/// Read a Kad tag list and extract source IP / TCP port / UDP port.
///
/// We parse only the tag types we care about; unknown tags are skipped.
/// Kad2 tags: 1-byte name-len + name + type + value.
/// Simplified tag format: type(1) + name_len(1) + name + value.
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
        // name: either 1-byte shortname (bit 7 of type set) or length-prefixed string
        let name_byte = {
            let mut b = [0u8];
            r.read_exact(&mut b)?;
            b[0]
        };
        // If name_byte indicates a string name, read more bytes.
        // Kad tag name encoding: if name_byte <= 0x0f it's the special 1-byte tag id.
        // Otherwise it's a 2-byte length + string.
        if name_byte > 0x0f {
            // It's a 2-byte length (name_byte is low byte of length).
            let name_high = {
                let mut b = [0u8];
                r.read_exact(&mut b)?;
                b[0]
            };
            let name_len = name_byte as usize | ((name_high as usize) << 8);
            let mut name_buf = vec![0u8; name_len];
            r.read_exact(&mut name_buf)?;
        }
        // Read value based on type.
        let value_type = type_byte & 0x7f; // strip "special" flag
        match value_type {
            0x02 => {
                // TAGTYPE_UINT32
                let v = read_u32(r)?;
                if name_byte == 0x01 {
                    // TAG_SOURCEIP
                    ip = std::net::Ipv4Addr::from(v.to_be_bytes());
                }
            }
            0x03 => {
                // TAGTYPE_UINT16
                let mut b = [0u8; 2];
                r.read_exact(&mut b)?;
                let v = u16::from_le_bytes(b);
                match name_byte {
                    0x02 => tcp_port = v, // TAG_SOURCEPORT
                    0x03 => udp_port = v, // TAG_SOURCEUPORT
                    _ => {}
                }
            }
            0x01 | 0x08 | 0x09 => {
                // TAGTYPE_HASH / TAGTYPE_UINT64 / TAGTYPE_UINT8
                let len: usize = match value_type {
                    0x01 => 16,
                    0x08 => 8,
                    0x09 => 1,
                    _ => 0,
                };
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
            }
            0x0b => {
                // TAGTYPE_STR (2-byte len + bytes)
                let len = read_u16(r)? as usize;
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
            }
            _ => {
                // Unknown — we can't skip reliably, stop parsing this entry.
                break;
            }
        }
    }
    Ok((ip, tcp_port, udp_port))
}

// ── Encode ────────────────────────────────────────────────────────────────────

/// Build a `KADEMLIA2_BOOTSTRAP_REQ` packet (2 bytes total).
pub fn encode_bootstrap_req() -> Vec<u8> {
    vec![KAD2_PROTO, Opcode::BootstrapReq as u8]
}

/// Build a `KADEMLIA2_HELLO_REQ` advertising our node details.
pub fn encode_hello_req(our_id: &KadId, tcp_port: u16) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::HelloReq as u8];
    our_id.write_to(&mut buf).unwrap();
    write_u16(&mut buf, tcp_port).unwrap();
    buf.push(KAD_VERSION);
    buf.push(0); // tag count = 0
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
pub fn encode_hello_res(our_id: &KadId, tcp_port: u16) -> Vec<u8> {
    let mut buf = vec![KAD2_PROTO, Opcode::HelloRes as u8];
    our_id.write_to(&mut buf).unwrap();
    write_u16(&mut buf, tcp_port).unwrap();
    buf.push(KAD_VERSION);
    buf.push(0); // tag count = 0
    buf
}

/// Build a `KADEMLIA2_HELLO_RES_ACK` packet.
pub fn encode_hello_res_ack() -> Vec<u8> {
    vec![KAD2_PROTO, Opcode::HelloResAck as u8]
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

    /// Regression: from_ed2k_hash must reverse the four 4-byte chunks of the
    /// MD4 hash to match eMule's CUInt128::SetValueBE → ToByteArray convention.
    #[test]
    fn test_from_ed2k_hash_chunk_reversal() {
        // MD4 with distinct 4-byte groups so we can verify each chunk position.
        let md4: [u8; 16] = [
            0x00, 0x01, 0x02, 0x03, // chunk 0 (bytes 0..3)
            0x10, 0x11, 0x12, 0x13, // chunk 1
            0x20, 0x21, 0x22, 0x23, // chunk 2
            0x30, 0x31, 0x32, 0x33, // chunk 3 (bytes 12..15)
        ];
        let kad = KadId::from_ed2k_hash(&md4);
        let b = kad.as_bytes();
        // On the wire the order must be: chunk3, chunk2, chunk1, chunk0.
        assert_eq!(&b[0..4], &[0x30, 0x31, 0x32, 0x33]);
        assert_eq!(&b[4..8], &[0x20, 0x21, 0x22, 0x23]);
        assert_eq!(&b[8..12], &[0x10, 0x11, 0x12, 0x13]);
        assert_eq!(&b[12..16], &[0x00, 0x01, 0x02, 0x03]);
    }
}
