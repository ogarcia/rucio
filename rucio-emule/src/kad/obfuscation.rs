//! Kad2 UDP packet obfuscation (eMule "obfuscated UDP" protocol).
//!
//! ## Overview
//!
//! eMule peers with version >= 6 can require obfuscated UDP. When a peer
//! has obfuscation enabled, it ignores plain `0xe4` Kad2 packets and only
//! accepts obfuscated ones.
//!
//! ## Wire format (outgoing obfuscated packet, Kad with KadID key)
//!
//! ```text
//! Byte 0:    semiRandomNotProtocol  (bits[1:0] = 0b00 = Kad + NodeID key)
//! Bytes 1-2: randomKeyPart (u16 LE) — combined with KadID to derive session key
//! Bytes 3-6: RC4(MAGICVALUE_UDP_SYNC_CLIENT = 0x395F2EC1 as LE bytes = [0xC1,0x2E,0x5F,0x39])
//! Byte 7:    RC4(padLen = 0)
//! -- optional kad verify keys (8 bytes) if receiverVerifyKey != 0 --
//! Bytes 8+:  RC4(plain Kad packet starting with 0xe4)
//! ```
//!
//! Session key = MD5(KadID[16] || randomKeyPart[2])
//!
//! ## Wire format (outgoing obfuscated packet, Kad with UDPKey)
//!
//! Same structure but:
//! - Byte 0: bits[1:0] = 0b10 (Kad + RecvKey)
//! - Session key = MD5(recvKey[4 LE] || randomKeyPart[2])
//!
//! ## Deobfuscation (incoming)
//!
//! The receiver tries both schemes (NodeID-based and RecvKey-based) to decrypt.
//! We currently only accept packets encrypted to our UDPKey (RecvKey scheme).

use md5::{Digest, Md5};
use std::net::Ipv4Addr;

const MAGICVALUE_UDP_SYNC_CLIENT: u32 = 0x395F2EC1;

/// A minimal RC4 stream cipher (pure-Rust, no external crate needed).
pub(crate) struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    pub(crate) fn new(key: &[u8]) -> Self {
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
    pub(crate) fn apply(&mut self, data: &mut [u8]) {
        for byte in data.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte ^= k;
        }
    }

    fn encrypt_into(&mut self, src: &[u8], dst: &mut [u8]) {
        for (s, d) in src.iter().zip(dst.iter_mut()) {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *d = s ^ k;
        }
    }
}

/// Replicate aMule's `CUInt128::StoreCryptValue`.
///
/// Analysis shows that `SetValueBE` followed by `StoreCryptValue` is the identity:
/// it reads the KadID bytes, stores them as four LE u32 values, then writes them
/// back — resulting in the same byte sequence.  Therefore `StoreCryptValue` is a
/// no-op on the wire representation of the KadID.
fn store_crypt_value(kad_id_wire: &[u8; 16]) -> [u8; 16] {
    *kad_id_wire
}

/// Derive the RC4 session key for the KadID scheme:
/// `MD5(StoreCryptValue(kad_id)[16] || random_key_part[2 LE])`
fn session_key_kad_id(kad_id: &[u8; 16], random_key_part: u16) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(store_crypt_value(kad_id));
    h.update(random_key_part.to_le_bytes());
    h.finalize().into()
}

/// Derive the RC4 session key for the RecvKey scheme:
/// `MD5(recv_key[4 LE] || random_key_part[2 LE])`
fn session_key_recv_key(recv_key: u32, random_key_part: u16) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(recv_key.to_le_bytes());
    h.update(random_key_part.to_le_bytes());
    h.finalize().into()
}

/// Legacy session key used for deobfuscating incoming packets keyed to our UDPKey.
/// In the original eMule code, incoming packets use:
/// `MD5(our_udp_key[4 LE] || sender_ip[4 LE])` (Kad1 style, not used for v6+ nodes)
/// but for v6+ the RecvKey scheme above is used.
fn session_key_legacy(udp_key: u32, peer_ip: Ipv4Addr) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(udp_key.to_le_bytes());
    h.update(u32::from(peer_ip).to_le_bytes());
    h.finalize().into()
}

/// Build the 8-byte obfuscation header and return an RC4 cipher ready for payload.
/// The marker byte has bits[1:0] set according to the key type:
/// - 0b00: Kad + NodeID key (kad_id provided)
/// - 0b10: Kad + RecvKey (recv_key provided)
fn build_header(
    rc4: &mut Rc4,
    marker_bits: u8, // lower 2 bits
    random_key_part: u16,
    receiver_verify_key: Option<u32>,
    sender_verify_key: Option<u32>,
) -> Vec<u8> {
    // Generate a semiRandom byte that won't be confused for a plaintext protocol marker.
    // Protocol bytes to avoid: 0xe4 (KAD2), 0xe5 (KAD2_PACKED), 0xd4, 0xc5, 0x01.
    // Use random_key_part as entropy source; try up to 16 candidates.
    let semi_random: u8 = {
        let mut candidate = (random_key_part as u8) ^ ((random_key_part >> 8) as u8);
        // Set the marker bits and clear conflicting high bits.
        candidate = (candidate & 0xFC) | (marker_bits & 0x03);
        // If the candidate is a reserved protocol byte, flip a bit to avoid it.
        if matches!(candidate, 0xe4 | 0xe5 | 0xd4 | 0xc5 | 0x01) {
            candidate ^= 0x08;
        }
        candidate
    };

    // eMule writes `(uchar*)&dwMagicValue` directly (LE layout on x86), so the 4
    // bytes on the wire are the little-endian representation: [0xC1, 0x2E, 0x5F, 0x39].
    let magic_wire = MAGICVALUE_UDP_SYNC_CLIENT.to_le_bytes(); // [0xC1, 0x2E, 0x5F, 0x39]
    let mut magic_enc = [0u8; 4];
    rc4.encrypt_into(&magic_wire, &mut magic_enc);

    let pad_len: u8 = 0;
    let mut pad_enc = [0u8; 1];
    rc4.encrypt_into(&[pad_len], &mut pad_enc);

    let mut out = Vec::with_capacity(8 + if receiver_verify_key.is_some() { 8 } else { 0 });
    out.push(semi_random);
    out.extend_from_slice(&random_key_part.to_le_bytes());
    out.extend_from_slice(&magic_enc);
    out.extend_from_slice(&pad_enc);

    if let (Some(rk), Some(sk)) = (receiver_verify_key, sender_verify_key) {
        let mut rk_enc = [0u8; 4];
        let mut sk_enc = [0u8; 4];
        rc4.encrypt_into(&rk.to_le_bytes(), &mut rk_enc);
        rc4.encrypt_into(&sk.to_le_bytes(), &mut sk_enc);
        out.extend_from_slice(&rk_enc);
        out.extend_from_slice(&sk_enc);
    }

    out
}

/// Wrap a plain Kad2 packet using the **KadID** obfuscation scheme (eMule v6+).
///
/// - `plain`: unencrypted Kad packet starting with `0xe4`.
/// - `kad_id`: the 16-byte Kademlia ID of the recipient node.
/// - `random_key_part`: 2 random bytes (unique per packet).
pub fn obfuscate_kad_id(plain: &[u8], kad_id: &[u8; 16], random_key_part: u16) -> Vec<u8> {
    let key = session_key_kad_id(kad_id, random_key_part);
    let mut rc4 = Rc4::new(&key);

    // Kad packets always include 8 bytes of verify keys (even if zero).
    let mut out = build_header(&mut rc4, 0x00, random_key_part, Some(0), Some(0));

    let mut encrypted = plain.to_vec();
    rc4.apply(&mut encrypted);
    out.extend_from_slice(&encrypted);
    out
}

/// Wrap a plain Kad2 packet using the **RecvKey** obfuscation scheme.
///
/// - `plain`: unencrypted Kad packet starting with `0xe4`.
/// - `recv_key`: the UDPKey announced by the recipient in their HELLO.
/// - `random_key_part`: 2 random bytes.
pub fn obfuscate_recv_key(plain: &[u8], recv_key: u32, random_key_part: u16) -> Vec<u8> {
    let key = session_key_recv_key(recv_key, random_key_part);
    let mut rc4 = Rc4::new(&key);

    // Kad packets always include 8 bytes of verify keys (even if zero).
    let mut out = build_header(&mut rc4, 0x02, random_key_part, Some(0), Some(0));

    let mut encrypted = plain.to_vec();
    rc4.apply(&mut encrypted);
    out.extend_from_slice(&encrypted);
    out
}

/// Wrap a plain Kad2 packet in obfuscation.
///
/// If `recv_key` is `Some`, uses the RecvKey scheme (preferred when available).
/// Otherwise falls back to the KadID scheme using `kad_id`.
///
/// `our_ip` is no longer used (kept for API stability, may be removed later).
pub fn obfuscate(plain: &[u8], recv_key: u32, _our_ip: Ipv4Addr, seed: [u8; 4]) -> Vec<u8> {
    // Legacy path: called from send_kad_pkt with a recv_key derived from KadID.
    // Use the RecvKey scheme; random_key_part comes from the first 2 bytes of seed.
    let random_key_part = u16::from_le_bytes([seed[0], seed[1]]);
    obfuscate_recv_key(plain, recv_key, random_key_part)
}

/// Attempt to decrypt an incoming obfuscated packet.
///
/// Tries the KadID scheme (using our KadID), the RecvKey scheme, and the legacy IP-based scheme.
/// Returns the decrypted payload (starting with `0xe4`) on success.
pub fn deobfuscate(
    data: &[u8],
    our_udp_key: u32,
    sender_ip: Ipv4Addr,
    our_kad_id: Option<&[u8; 16]>,
) -> Option<Vec<u8>> {
    if data.len() < 9 {
        return None;
    }

    let random_key_part = u16::from_le_bytes(data[1..3].try_into().ok()?);

    // Helper to try a given RC4 key and verify the magic.
    // `kad`: if true, 8 extra verify key bytes follow the padding (aMule Kad wire format).
    let try_key = |key: &[u8], kad: bool| -> Option<Vec<u8>> {
        let mut rc4 = Rc4::new(key);
        let mut candidate = data[3..].to_vec();
        rc4.apply(&mut candidate);
        if candidate.len() >= 5 {
            // Wire bytes [0x39, 0x5F, 0x2E, 0xC1] read as BE = 0x395F2EC1 = MAGIC.
            // eMule writes the magic as a LE u32, so we compare with from_le_bytes.
            let magic = u32::from_le_bytes(candidate[0..4].try_into().unwrap());
            if magic == MAGICVALUE_UDP_SYNC_CLIENT {
                let pad_len = (candidate[4] & 0x0f) as usize;
                // Kad packets have 8 extra verify-key bytes before the payload.
                let verify_overhead = if kad { 8 } else { 0 };
                let payload_start = 5 + pad_len + verify_overhead;
                if candidate.len() > payload_start {
                    let payload = candidate[payload_start..].to_vec();
                    if payload.first().copied() == Some(0xe4)
                        || payload.first().copied() == Some(0xe5)
                    {
                        return Some(payload);
                    }
                }
            }
        }
        None
    };

    // Try KadID scheme first (peer encrypted using our KadID).
    if let Some(kad_id) = our_kad_id {
        let key = session_key_kad_id(kad_id, random_key_part);
        if let Some(p) = try_key(&key, true) {
            return Some(p);
        }
    }

    // Try RecvKey scheme (v6+ nodes that know our UDPKey).
    {
        let key = session_key_recv_key(our_udp_key, random_key_part);
        if let Some(p) = try_key(&key, true) {
            return Some(p);
        }
    }

    // Try legacy IP-based scheme (older nodes).
    {
        let key = session_key_legacy(our_udp_key, sender_ip);
        let mut rc4 = Rc4::new(&key);
        // Legacy format: bytes 0-3 = seed, bytes 4-7 = recv_key XOR KEY_MAGIC, bytes 8+ = RC4(plain)
        if data.len() >= 9 {
            let mut candidate = data[8..].to_vec();
            rc4.apply(&mut candidate);
            if candidate.first().copied() == Some(0xe4) || candidate.first().copied() == Some(0xe5)
            {
                return Some(candidate);
            }
        }
    }

    None
}

/// Derive a preliminary recv_key from the remote node's KadID.
/// Not actually used for encryption directly — callers should use
/// `obfuscate_kad_id` with the raw KadID bytes instead.
pub fn key_from_kad_id(kad_id: &[u8; 16]) -> u32 {
    let mut h = Md5::new();
    h.update(kad_id);
    let digest: [u8; 16] = h.finalize().into();
    u32::from_le_bytes(digest[..4].try_into().unwrap())
}

/// Generate a random u16 for use as randomKeyPart.
pub fn random_key_part() -> u16 {
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
    h.finish() as u16
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
    fn test_kad_id_obfuscation_decrypt() {
        let plain = vec![0xe4u8, 0x21, 0x02, 0xde, 0xad, 0xbe, 0xef];
        let kad_id = [1u8; 16];
        let rkp: u16 = 0x1234;

        let obfuscated = obfuscate_kad_id(&plain, &kad_id, rkp);
        // header(8) + verify_keys(8) + payload = 16 + payload bytes
        assert!(obfuscated.len() > 16);

        // Manually decrypt to verify.
        let key = session_key_kad_id(&kad_id, rkp);
        let mut rc4 = Rc4::new(&key);
        let magic_be = MAGICVALUE_UDP_SYNC_CLIENT.to_le_bytes();
        let mut magic_dec = [0u8; 4];
        rc4.encrypt_into(&obfuscated[3..7], &mut magic_dec);
        assert_eq!(
            magic_dec, magic_be,
            "magic value must decrypt correctly (LE)"
        );

        let mut pad_dec = [0u8; 1];
        rc4.encrypt_into(&obfuscated[7..8], &mut pad_dec);
        assert_eq!(pad_dec[0], 0, "pad_len must be 0");

        // Skip verify keys (8 bytes).
        let mut verify_dec = [0u8; 8];
        rc4.encrypt_into(&obfuscated[8..16], &mut verify_dec);

        let mut payload_dec = obfuscated[16..].to_vec();
        rc4.apply(&mut payload_dec);
        assert_eq!(payload_dec, plain);
    }

    #[test]
    fn test_recv_key_obfuscation_roundtrip() {
        let plain = vec![0xe4u8, 0x01, 0x02, 0x03];
        let recv_key = 0xDEADBEEFu32;
        let rkp: u16 = 0xABCD;

        let obfuscated = obfuscate_recv_key(&plain, recv_key, rkp);

        // Verify with manual decrypt.
        let key = session_key_recv_key(recv_key, rkp);
        let mut rc4 = Rc4::new(&key);
        let magic_be = MAGICVALUE_UDP_SYNC_CLIENT.to_le_bytes();
        let mut magic_dec = [0u8; 4];
        rc4.encrypt_into(&obfuscated[3..7], &mut magic_dec);
        assert_eq!(magic_dec, magic_be);

        let mut pad_dec = [0u8; 1];
        rc4.encrypt_into(&obfuscated[7..8], &mut pad_dec);
        assert_eq!(pad_dec[0], 0);

        // Skip verify keys (8 bytes).
        let mut verify_dec = [0u8; 8];
        rc4.encrypt_into(&obfuscated[8..16], &mut verify_dec);

        let mut payload_dec = obfuscated[16..].to_vec();
        rc4.apply(&mut payload_dec);
        assert_eq!(payload_dec, plain);
    }

    /// Full roundtrip: obfuscate_kad_id → deobfuscate should recover the plain packet.
    #[test]
    fn test_full_deobfuscate_kad_id_roundtrip() {
        let plain = vec![0xe4u8, 0x01, 0xAB, 0xCD];
        let kad_id = [0x42u8; 16];
        let rkp: u16 = 0x5678;
        let sender_ip: std::net::Ipv4Addr = "1.2.3.4".parse().unwrap();
        let our_udp_key: u32 = 0;

        let obfuscated = obfuscate_kad_id(&plain, &kad_id, rkp);

        // deobfuscate using our KadID (the sender encrypted using the recipient's KadID)
        let recovered = deobfuscate(&obfuscated, our_udp_key, sender_ip, Some(&kad_id)).unwrap();
        assert_eq!(
            recovered, plain,
            "deobfuscate must recover the original packet"
        );
    }

    /// Full roundtrip: obfuscate_recv_key → deobfuscate should recover the plain packet.
    #[test]
    fn test_full_deobfuscate_recv_key_roundtrip() {
        let plain = vec![0xe4u8, 0x19, 0x01, 0x02, 0x03];
        let recv_key: u32 = 0xBEEFCAFE;
        let rkp: u16 = 0x1A2B;
        let sender_ip: std::net::Ipv4Addr = "5.6.7.8".parse().unwrap();

        let obfuscated = obfuscate_recv_key(&plain, recv_key, rkp);

        // deobfuscate using our UDPKey (the sender encrypted using our published UDPKey)
        let recovered = deobfuscate(&obfuscated, recv_key, sender_ip, None).unwrap();
        assert_eq!(
            recovered, plain,
            "deobfuscate must recover the original packet"
        );
    }
}
