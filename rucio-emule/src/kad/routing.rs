//! Kad2 routing table and `nodes.dat` file parser.
//!
//! ## `nodes.dat` format (version 3)
//!
//! eMule stores the routing table in `nodes.dat`. The binary format is:
//!
//! ```text
//! u32  num_contacts (if == 0, next u32 is version tag: 2 or 3)
//! u32  version      (if the first u32 was 0; 2 or 3)
//! --- per contact (version 1/2) ---
//! 16b  KadId
//! u32  IPv4 address (big-endian on wire, not network order)
//! u16  udp_port
//! u16  tcp_port
//! u8   version
//! --- version 3 also has ---
//! u32  UDPKey
//! u32  UDPKey_ip  (IP this key was generated for)
//! u8   verified   (0 or 1)
//! ```
//!
//! We parse versions 1, 2, and 3.  On write we produce version 3.

use super::packet::{Contact, KadId};
use std::io::{self, Cursor, Read, Write};
use thiserror::Error;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("io error reading nodes.dat: {0}")]
    Io(#[from] io::Error),
    #[error("unsupported nodes.dat version: {0}")]
    UnsupportedVersion(u32),
}

// ── nodes.dat parser ──────────────────────────────────────────────────────────

fn read_u16_le<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}

fn read_u32_le<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn write_u16_le<W: Write>(w: &mut W, v: u16) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u32_le<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

/// Parse `nodes.dat` bytes into a list of [`Contact`]s.
///
/// Silently skips contacts with version < 2 (Kad1 only).
pub fn parse_nodes_dat(data: &[u8]) -> Result<Vec<Contact>, RoutingError> {
    let mut cur = Cursor::new(data);

    let first_u32 = read_u32_le(&mut cur)?;
    let (file_version, count) = if first_u32 == 0 {
        // New format: first u32 is 0, second is version.
        let ver = read_u32_le(&mut cur)?;
        if ver != 2 && ver != 3 {
            return Err(RoutingError::UnsupportedVersion(ver));
        }
        let cnt = read_u32_le(&mut cur)?;
        (ver, cnt)
    } else {
        // Legacy format (version 1): first u32 is the contact count.
        (1, first_u32)
    };

    let mut contacts = Vec::with_capacity(count.min(2000) as usize);

    for _ in 0..count {
        let id = KadId::read_from(&mut cur)?;
        let ip_raw = read_u32_le(&mut cur)?;
        let udp_port = read_u16_le(&mut cur)?;
        let tcp_port = read_u16_le(&mut cur)?;
        let version = {
            let mut b = [0u8];
            cur.read_exact(&mut b)?;
            b[0]
        };

        if file_version >= 3 {
            // Skip UDPKey (u32), UDPKey_ip (u32), verified (u8) = 9 bytes.
            let mut skip = [0u8; 9];
            cur.read_exact(&mut skip)?;
        }

        if version >= 2 {
            // ip_raw in nodes.dat is stored as little-endian uint32.
            // The actual IPv4 bytes need to be interpreted as network-order (big-endian) for display.
            contacts.push(Contact {
                id,
                ip: std::net::Ipv4Addr::from(ip_raw.to_be_bytes()),
                udp_port,
                tcp_port,
                version,
                udp_key: None,
            });
        }
    }

    Ok(contacts)
}

/// Serialize contacts to `nodes.dat` format version 3.
pub fn write_nodes_dat(contacts: &[Contact]) -> Vec<u8> {
    let mut buf = Vec::new();
    // version marker
    write_u32_le(&mut buf, 0).unwrap();
    write_u32_le(&mut buf, 3).unwrap();
    write_u32_le(&mut buf, contacts.len() as u32).unwrap();

    for c in contacts {
        c.id.write_to(&mut buf).unwrap();
        let ip_raw = u32::from_be_bytes(c.ip.octets());
        write_u32_le(&mut buf, ip_raw).unwrap();
        write_u16_le(&mut buf, c.udp_port).unwrap();
        write_u16_le(&mut buf, c.tcp_port).unwrap();
        buf.push(c.version);
        // UDPKey = 0, UDPKey_ip = 0, verified = 0
        write_u32_le(&mut buf, 0).unwrap();
        write_u32_le(&mut buf, 0).unwrap();
        buf.push(0);
    }
    buf
}

// ── Routing table ─────────────────────────────────────────────────────────────

/// Maximum number of contacts per k-bucket (k = 10).
pub const K: usize = 10;

/// A single k-bucket holding up to K contacts sorted oldest-first.
#[derive(Debug, Default, Clone)]
pub struct KBucket {
    entries: Vec<Contact>,
}

impl KBucket {
    /// Try to add a contact.
    ///
    /// - Duplicate IDs update the existing entry's UDP key and return `false`.
    /// - If the bucket has room, the contact is appended and `true` is returned.
    /// - If the bucket is full, the oldest entry (index 0) is evicted to make
    ///   room for the new contact.  This mirrors eMule's behaviour of preferring
    ///   fresh peers over stale ones when no live-ping infrastructure is present.
    pub fn add(&mut self, contact: Contact) -> bool {
        if let Some(existing) = self.entries.iter_mut().find(|c| c.id == contact.id) {
            if contact.udp_key.is_some() {
                existing.udp_key = contact.udp_key;
            }
            return false;
        }
        if self.entries.len() < K {
            self.entries.push(contact);
        } else {
            // Evict oldest (front) and push the new contact at the back.
            self.entries.remove(0);
            self.entries.push(contact);
        }
        true
    }

    pub fn contacts(&self) -> &[Contact] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A simplified Kad routing table (128 k-buckets, one per XOR bit prefix).
///
/// For our use-case (bootstrapping and source search) we only need:
/// - `insert`: add a contact.
/// - `closest_to`: find the N contacts closest to a target.
/// - `all_contacts`: iterate everything.
#[derive(Debug, Default, Clone)]
pub struct RoutingTable {
    /// Our own node ID.
    pub our_id: KadId,
    /// 128 k-buckets indexed by the index of the first differing bit.
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(our_id: KadId) -> Self {
        Self {
            our_id,
            buckets: (0..128).map(|_| KBucket::default()).collect(),
        }
    }

    /// Insert a contact into the appropriate bucket.
    pub fn insert(&mut self, contact: Contact) -> bool {
        if contact.id == self.our_id {
            return false;
        }
        let idx = bucket_index(&self.our_id, &contact.id);
        self.buckets[idx].add(contact)
    }

    /// Insert a contact, or update its `udp_key` if it already exists.
    pub fn insert_or_update_key(&mut self, contact: Contact) -> bool {
        if contact.id == self.our_id {
            return false;
        }
        let idx = bucket_index(&self.our_id, &contact.id);
        self.buckets[idx].add(contact)
    }

    /// Load all contacts from a parsed `nodes.dat`.
    pub fn load_nodes_dat(&mut self, contacts: Vec<Contact>) {
        for c in contacts {
            self.insert(c);
        }
    }

    /// Return up to `n` contacts closest to `target`.
    pub fn closest_to(&self, target: &KadId, n: usize) -> Vec<Contact> {
        let mut all: Vec<&Contact> = self.buckets.iter().flat_map(|b| b.contacts()).collect();
        all.sort_by(|a, b| {
            let da = a.id.distance(target);
            let db = b.id.distance(target);
            da.cmp_bytes().cmp(db.cmp_bytes())
        });
        all.into_iter().take(n).cloned().collect()
    }

    /// Find a contact by its UDP address.
    pub fn find_by_addr(&self, addr: &std::net::SocketAddrV4) -> Option<&Contact> {
        self.buckets
            .iter()
            .flat_map(|b| b.contacts())
            .find(|c| &c.socket_addr_udp() == addr)
    }

    /// Total number of contacts in the table.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterator over all contacts.
    pub fn all_contacts(&self) -> impl Iterator<Item = &Contact> {
        self.buckets.iter().flat_map(|b| b.contacts())
    }
}

/// Return the index (0–127) of the most-significant bit where `a` and `b` differ.
fn bucket_index(a: &KadId, b: &KadId) -> usize {
    let xor = a.distance(b);
    for (byte_idx, &byte) in xor.as_bytes().iter().enumerate() {
        if byte != 0 {
            let bit = byte.leading_zeros() as usize;
            return byte_idx * 8 + bit;
        }
    }
    127 // identical IDs — shouldn't happen
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_contact(id_byte: u8, ip: [u8; 4], udp: u16, tcp: u16) -> Contact {
        let mut id = [0u8; 16];
        id[0] = id_byte;
        Contact {
            id: KadId::from_bytes(id),
            ip: std::net::Ipv4Addr::from(ip),
            udp_port: udp,
            tcp_port: tcp,
            version: 9,
            udp_key: None,
        }
    }

    #[test]
    fn test_nodes_dat_roundtrip() {
        let contacts = vec![
            make_contact(0x01, [1, 2, 3, 4], 4672, 4662),
            make_contact(0x02, [5, 6, 7, 8], 4672, 4662),
        ];
        let bytes = write_nodes_dat(&contacts);
        let parsed = parse_nodes_dat(&bytes).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, contacts[0].id);
        assert_eq!(parsed[0].ip, contacts[0].ip);
        assert_eq!(parsed[1].tcp_port, contacts[1].tcp_port);
    }

    #[test]
    fn test_routing_table_insert_and_closest() {
        let our_id = KadId::from_bytes([0u8; 16]);
        let mut rt = RoutingTable::new(our_id);
        for i in 1u8..=20 {
            rt.insert(make_contact(i, [i, 0, 0, 1], 4672, 4662));
        }
        assert!(!rt.is_empty());
        let closest = rt.closest_to(&KadId::from_bytes([1u8; 16]), 5);
        assert!(closest.len() <= 5);
    }

    #[test]
    fn test_empty_nodes_dat() {
        let empty = write_nodes_dat(&[]);
        let parsed = parse_nodes_dat(&empty).unwrap();
        assert!(parsed.is_empty());
    }
}
