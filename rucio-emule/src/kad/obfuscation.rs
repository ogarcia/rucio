//! Kad2 UDP packet obfuscation (eMule "obfuscated UDP" protocol).
//!
//! ## Overview
//!
//! eMule peers with version >= 6 can require obfuscated UDP. When a peer
//! has obfuscation enabled, it ignores plain `0xe4` Kad2 packets and only
//! accepts obfuscated ones.
//!
//! ## Wire format (outgoing obfuscated packet)
//!
//! ```text
//! [0..4]   random seed bytes (4 bytes)
//! [4..8]   recv_key XOR 0x395F2EC1  (u32 LE) — tells receiver which key we used
//! [8..]    RC4-encrypted real packet (0xe4 opcode ... )
//! ```
//!
//! RC4 key for encryption = MD5(recv_key_u32_LE ++ sender_ip_u32_LE)
//!
//! ## Wire format (incoming obfuscated packet)
//!
//! To decrypt an incoming obfuscated packet sent to us:
//!   RC4 key = MD5(our_udp_key_u32_LE ++ sender_ip_u32_LE)
//! where `our_udp_key` is the u32 we announced in our HELLO_REQ.
//!
//! The first 4 bytes of the encrypted region are the "magic" validation:
//! after decryption they should equal 0x00_00_00_00 (zero) or contain
//! the eMule "recv_key" identifier — we validate by checking the decrypted
//! protocol byte is 0xe4 or 0xe5.

use md5::{Digest, Md5};
use std::net::Ipv4Addr;

// Magic constant used in the key-id field (bytes 4-7 of the wire).
const KEY_MAGIC: u32 = 0x395F2EC1;

/// A minimal RC4 stream cipher (pure-Rust, no external crate needed).
struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, v) in s.iter_mut().enumerate() {
            *v = i as u8;
        }
        let mut j: u8 = 0;
        for i in 0u8..=255 {
            j = j
                .wrapping_add(s[i as usize])
                .wrapping_add(key[i as usize % key.len()]);
            s.swap(i as usize, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }

    /// XOR-encrypt/decrypt `data` in place.
    fn apply(&mut self, data: &mut [u8]) {
        for byte in data.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte ^= k;
        }
    }
}

/// Derive the RC4 session key: MD5(udp_key_u32_LE || peer_ip_u32_LE).
fn session_key(udp_key: u32, peer_ip: Ipv4Addr) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(udp_key.to_le_bytes());
    h.update(u32::from(peer_ip).to_le_bytes());
    h.finalize().into()
}

/// Wrap a plain Kad2 packet in the eMule obfuscated UDP envelope.
///
/// - `plain`: the unencrypted packet (starting with `0xe4` / `0xe5`).
/// - `recv_key`: the UDPKey we read from the peer's HELLO_RES (their key).
/// - `sender_ip`: our external IPv4 address (as seen by the peer).
/// - `seed`: 4 random bytes used as the datagram seed.
pub fn obfuscate(plain: &[u8], recv_key: u32, sender_ip: Ipv4Addr, seed: [u8; 4]) -> Vec<u8> {
    let key = session_key(recv_key, sender_ip);
    let mut rc4 = Rc4::new(&key);

    // Build the 8-byte header.
    let key_id = (recv_key ^ KEY_MAGIC).to_le_bytes();
    let mut out = Vec::with_capacity(8 + plain.len());
    out.extend_from_slice(&seed);
    out.extend_from_slice(&key_id);

    // Encrypt the real packet.
    let mut encrypted = plain.to_vec();
    rc4.apply(&mut encrypted);
    out.extend_from_slice(&encrypted);
    out
}

/// Attempt to decrypt an incoming obfuscated packet.
///
/// Returns the decrypted payload (starting with `0xe4`) on success.
///
/// - `data`: the raw UDP datagram bytes.
/// - `our_udp_key`: the u32 we announced in HELLO_REQ.
/// - `sender_ip`: the IP the packet came from.
pub fn deobfuscate(data: &[u8], our_udp_key: u32, sender_ip: Ipv4Addr) -> Option<Vec<u8>> {
    if data.len() < 9 {
        return None;
    }
    // Bytes 0-3: seed (ignored for decryption).
    // Bytes 4-7: recv_key XOR KEY_MAGIC — should equal our_udp_key XOR KEY_MAGIC.
    let key_id_wire = u32::from_le_bytes(data[4..8].try_into().ok()?);
    let claimed_recv_key = key_id_wire ^ KEY_MAGIC;
    if claimed_recv_key != our_udp_key {
        return None;
    }

    let key = session_key(our_udp_key, sender_ip);
    let mut rc4 = Rc4::new(&key);
    let mut plain = data[8..].to_vec();
    rc4.apply(&mut plain);

    // Validate: decrypted first byte must be a valid Kad2 protocol byte.
    if plain.first().copied() == Some(0xe4) || plain.first().copied() == Some(0xe5) {
        Some(plain)
    } else {
        None
    }
}

/// Derive the receiver's expected key from their KadID (used when we don't have
/// their UDPKey yet). eMule uses the first 4 bytes of MD5(kad_id_bytes) as the
/// preliminary recv_key, allowing the peer to decrypt our HELLO_REQ using only
/// their own KadID.
pub fn key_from_kad_id(kad_id: &[u8; 16]) -> u32 {
    let mut h = Md5::new();
    h.update(kad_id);
    let digest: [u8; 16] = h.finalize().into();
    u32::from_le_bytes(digest[..4].try_into().unwrap())
}

/// Generate a random u32 UDPKey using OS-level entropy.
pub fn random_udp_key() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    std::process::id().hash(&mut h);
    h.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_rc4_roundtrip() {
        let key = b"testkey";
        let plain = b"hello world";
        let mut data = plain.to_vec();
        Rc4::new(key).apply(&mut data);
        Rc4::new(key).apply(&mut data);
        assert_eq!(&data, plain);
    }

    #[test]
    fn test_obfuscate_deobfuscate_roundtrip() {
        let plain = vec![0xe4u8, 0x21, 0x02, 0xde, 0xad, 0xbe, 0xef];
        let recv_key = 0xDEADBEEFu32;
        let our_ip = Ipv4Addr::new(1, 2, 3, 4);
        let seed = [0x11, 0x22, 0x33, 0x44];

        let obfuscated = obfuscate(&plain, recv_key, our_ip, seed);
        assert!(obfuscated.len() > 8);

        let decrypted = deobfuscate(&obfuscated, recv_key, our_ip).unwrap();
        assert_eq!(decrypted, plain);
    }

    #[test]
    fn test_deobfuscate_wrong_key_returns_none() {
        let plain = vec![0xe4u8, 0x01];
        let recv_key = 0x12345678u32;
        let our_ip = Ipv4Addr::new(10, 0, 0, 1);
        let seed = [0, 0, 0, 0];

        let obfuscated = obfuscate(&plain, recv_key, our_ip, seed);
        // Try to decrypt with wrong key.
        assert!(deobfuscate(&obfuscated, 0xDEADBEEF, our_ip).is_none());
    }
}
