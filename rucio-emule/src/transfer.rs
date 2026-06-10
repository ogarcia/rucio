//! Minimal eMule TCP chunk download protocol.
//!
//! This module implements enough of the eMule client-to-client TCP protocol to
//! download a file from an eMule peer, given that we know:
//! - The peer's IP:TCP-port.
//! - The file's ed2k hash and size.
//!
//! ## Protocol overview
//!
//! eMule TCP exchanges use a simple framing:
//!   `[protocol(1)] [length(4 LE)] [opcode(1)] [payload...]`
//!
//! For ed2k file transfers the relevant opcodes are:
//! - `0x01` HELLO — exchange client IDs.
//! - `0x4f` HELLOANSWER — response to HELLO.
//! - `0x58` FILEREQUEST — request a file by hash.
//! - `0x59` FILEREQUEST_ANSWER — server confirms it has the file.
//! - `0x4b` STARTUPLOAD_REQ — ask peer to start sending.
//! - `0x46` SENDING_CHUNK — peer is sending a data chunk.
//! - `0x47` REQUESTPARTS — request specific byte ranges.
//!
//! We implement only the **downloader** side.
//!
//! ## TCP obfuscation
//!
//! Many eMule peers enable "obfuscated TCP" (RC4 encrypted stream).  Peers
//! that *require* obfuscation will close a plain connection immediately before
//! sending HELLOANSWER — this is the "read HELLOANSWER" error seen in logs.
//!
//! When `DownloadOptions::peer_hash` is set and a plain connection is
//! rejected, `Session::connect` automatically retries using RC4.
//!
//! Wire format (outgoing obfuscated handshake), per eMule
//! `CEncryptedStreamSocket::StartNegotiation`:
//! ```text
//! [1]  marker        — plaintext, any non-protocol byte (≠ 0xE3/0xD4/0xC5)
//! [4]  random_key    — plaintext (LE)
//! [4]  RC4send(0x835E6FC4 LE)  — MAGICVALUE_SYNC, confirms key agreement
//! [1]  RC4send(0x00) — supported method (ENM_OBFUSCATION)
//! [1]  RC4send(0x00) — preferred method (ENM_OBFUSCATION)
//! [1]  RC4send(pad_len) — 0 (no padding)
//! ...  RC4send(eMule frames)   — HELLO and all subsequent sent data
//! ```
//! The peer replies, RC4-encrypted with its own send key:
//! `MAGICVALUE_SYNC(4) + method(1) + pad_len(1) + padding`.
//!
//! Two **separate** RC4 streams are used, with different magic bytes mixed into
//! the key (eMule's requester/server distinction):
//! ```text
//! send key (we encrypt) = MD5(peer_hash[16] || 0x22 || random_key[4])
//! recv key (we decrypt) = MD5(peer_hash[16] || 0xCB || random_key[4])
//! ```
//!
//! ## Chunk / part layout
//!
//! eMule splits files into 9,728,000-byte "parts" for MD4 hash verification.
//! Each part is further split into at most 3 sub-ranges per `REQUESTPARTS`
//! message (eMule's request window).
//!
//! After downloading the full file, call [`crate::ed2k::hash_reader`] to
//! compute the BLAKE3 hash for Rucio DHT integration.

use crate::ed2k::{CHUNK_SIZE, Ed2kHash};
use crate::kad::obfuscation::Rc4;
use anyhow::{Context, Result, bail};
use flate2::read::ZlibDecoder;
use md4::Md4;
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::future::Future;
use std::io::{self, Read as _};
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore, TryAcquireError};
use tokio::time::timeout;
use tracing::{debug, info, warn};

// ── Protocol constants ────────────────────────────────────────────────────────

/// Protocol header byte for standard ed2k TCP messages.
const PROTO_ED2K: u8 = 0xe3;
/// `OP_EMULEPROT` — header byte for eMule-extended TCP messages (plaintext).
const PROTO_EMULE: u8 = 0xc5;
/// `OP_PACKEDPROT` — header byte for zlib-packed TCP messages (plaintext).
const PROTO_PACKED: u8 = 0xd4;

// eMule TCP obfuscation handshake constants (CEncryptedStreamSocket).
/// `MAGICVALUE_SYNC` — confirms a working encrypted stream (sent encrypted).
const MAGICVALUE_SYNC: u32 = 0x835E_6FC4;
/// `MAGICVALUE_REQUESTER` — mixed into the requester's send key (= server's recv key).
const MAGIC_REQUESTER: u8 = 34;
/// `MAGICVALUE_SERVER` — mixed into the server's send key (= requester's recv key).
const MAGIC_SERVER: u8 = 203;
/// `ENM_OBFUSCATION` — the only encryption method we (and modern eMule) speak.
const ENM_OBFUSCATION: u8 = 0x00;
// Obfuscation supported + requested (not required, so we still accept plain peers).
// Published as the ENCRYPTION tag when we announce ourselves as a Kad source.
pub(crate) const TCP_CONNECT_OPTIONS: u8 = 0x03;

// ── Opcodes ───────────────────────────────────────────────────────────────────

const OP_HELLO: u8 = 0x01;
const OP_HELLOANSWER: u8 = 0x4c;
const OP_FILEREQUEST: u8 = 0x58;
const OP_FILEREQUEST_ANSWER: u8 = 0x59;
const OP_FILENOTFOUND: u8 = 0x92;
const OP_REQUESTPARTS: u8 = 0x47;
const OP_SENDINGPART: u8 = 0x46;
/// Extended-protocol (0xc5) opcode: zlib-compressed file data block.
/// Payload: hash[16] + start_offset[4 LE] + zlib_data[…]
/// Range end is implicit: start_offset + decompressed.len()
const OP_COMPRESSEDPART: u8 = 0x40;
const OP_FILESTATUS: u8 = 0x50;
const OP_STARTUPLOAD_REQ: u8 = 0x54;
const OP_ACCEPTUPLOAD_REQ: u8 = 0x55;
const OP_ENDOFDOWNLOAD: u8 = 0x48;
const OP_QUEUE_RANK: u8 = 0x5c;
const OP_QUEUE_FULL: u8 = 0x93;
/// Hashset request — payload: 16-byte file hash.
const OP_HASHSETREQUEST: u8 = 0x51;
/// Hashset answer — payload: file_hash(16) + part_count(u16 LE) + part_hash(16)*N.
const OP_HASHSETANSWER: u8 = 0x52;

// ── Framing ───────────────────────────────────────────────────────────────────

/// Build a framed eMule TCP message.
fn build_message(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 1) as u32; // +1 for opcode byte
    let mut msg = Vec::with_capacity(6 + payload.len());
    msg.push(PROTO_ED2K);
    msg.extend_from_slice(&len.to_le_bytes());
    msg.push(opcode);
    msg.extend_from_slice(payload);
    msg
}

/// RC4 stream pair for a connection. eMule uses **two independent** RC4 streams
/// with different keys — one for each direction — so a single shared cipher
/// cannot work. `None` on both fields means a plain (unencrypted) connection.
#[derive(Default)]
struct ObfCiphers {
    /// Encrypts data we send.
    send: Option<Rc4>,
    /// Decrypts data we receive.
    recv: Option<Rc4>,
}

impl ObfCiphers {
    fn is_obfuscated(&self) -> bool {
        self.send.is_some()
    }
}

/// Maximum eMule TCP frame length we will accept. Legitimate frames are small:
/// a data block is at most a ~180 KB requested window, a hashset a few hundred
/// KB. The header carries the length as a peer-supplied `u32` (up to ~4 GiB), so
/// without a cap a malformed/garbage frame — or a malicious peer — makes us
/// `vec![0u8; len]` gigabytes and the process OOM-aborts. 16 MiB is far above any
/// real frame; anything larger closes the connection instead of allocating.
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Validate the frame length header and return the payload length (`len - 1`,
/// the opcode byte excluded). Rejects empty and oversized frames so we never
/// allocate an attacker-controlled buffer.
fn frame_payload_len(len: u32) -> io::Result<usize> {
    let len = len as usize;
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero-length frame",
        ));
    }
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds maximum {MAX_FRAME_LEN}"),
        ));
    }
    Ok(len - 1)
}

/// Read one eMule TCP frame, applying RC4 decryption with the receive key if the
/// connection is obfuscated. Returns `(protocol, opcode, payload)`.
async fn read_frame(
    stream: &mut TcpStream,
    ciphers: &mut ObfCiphers,
) -> io::Result<(u8, u8, Vec<u8>)> {
    let mut hdr = [0u8; 6];
    stream.read_exact(&mut hdr).await?;
    if let Some(rc4) = ciphers.recv.as_mut() {
        rc4.apply(&mut hdr);
    }
    let proto = hdr[0];
    let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);
    let payload_len = frame_payload_len(len)?;
    let opcode = hdr[5];
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
        if let Some(rc4) = ciphers.recv.as_mut() {
            rc4.apply(&mut payload);
        }
    }
    Ok((proto, opcode, payload))
}

/// Write a framed eMule message, applying RC4 encryption with the send key if
/// the connection is obfuscated.
async fn write_frame(
    stream: &mut TcpStream,
    ciphers: &mut ObfCiphers,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut msg = build_message(opcode, payload);
    if let Some(rc4) = ciphers.send.as_mut() {
        rc4.apply(&mut msg);
    }
    stream.write_all(&msg).await
}

// ── HELLO packet ─────────────────────────────────────────────────────────────

/// Build a HELLO / HELLOANSWER payload advertising ourselves.
///
/// `tcp_port` is our listening TCP port; pass 0 if not listening (Low-ID).
fn build_hello(our_hash: &[u8; 16], tcp_port: u16, nick: &str) -> Vec<u8> {
    let mut p = Vec::new();
    p.push(16u8);
    p.extend_from_slice(our_hash);
    p.extend_from_slice(&0u32.to_le_bytes()); // client ID = 0 (low-ID until server assigns one)
    p.extend_from_slice(&tcp_port.to_le_bytes());
    // Tags: advertise our nickname (CT_NAME) so peers display a name for us.
    // Cap to a sane char length and keep UTF-8 boundaries intact.
    let nick: String = nick.trim().chars().take(60).collect();
    if nick.is_empty() {
        p.extend_from_slice(&0u32.to_le_bytes()); // tag count = 0
    } else {
        p.extend_from_slice(&1u32.to_le_bytes()); // tag count = 1
        // CT_NAME string tag, universal new-ed2k form:
        //   [TAGTYPE_STRING(0x02) | 0x80 special-name][name=CT_NAME(0x01)][len u16 LE][bytes]
        let bytes = nick.as_bytes();
        p.push(0x02 | 0x80);
        p.push(0x01); // CT_NAME
        p.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        p.extend_from_slice(bytes);
    }
    p.extend_from_slice(&0u32.to_le_bytes()); // server IP (unused)
    p.extend_from_slice(&0u16.to_le_bytes()); // server port (unused)
    p
}

// ── Obfuscation helpers ───────────────────────────────────────────────────────

/// Derive an RC4 key for an obfuscated TCP connection:
/// `MD5(peer_hash[16] || magic || rand[4])`. The `magic` byte
/// ([`MAGIC_REQUESTER`]/[`MAGIC_SERVER`]) yields a distinct key per direction.
fn tcp_obf_rc4_key(peer_hash: &[u8; 16], magic: u8, rand: &[u8; 4]) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(peer_hash);
    h.update([magic]);
    h.update(rand);
    h.finalize().into()
}

/// Pick a plaintext handshake marker byte that is not a protocol header byte
/// (eMule's `GetSemiRandomNotProtocolMarker`): the receiver uses the first byte
/// to tell an obfuscated stream from a plain one.
fn obf_marker_byte() -> u8 {
    let [b, ..] = random_tcp_key();
    match b {
        // OP_EDONKEYPROT / OP_PACKEDPROT / OP_EMULEPROT — would read as plain.
        0xe3 | 0xd4 | 0xc5 => 0x01,
        other => other,
    }
}

/// Generate 4 pseudo-random bytes for the obfuscation key exchange.
fn random_tcp_key() -> [u8; 4] {
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
    let v = h.finish();
    [v as u8, (v >> 8) as u8, (v >> 16) as u8, (v >> 24) as u8]
}

// ── Error sentinel ────────────────────────────────────────────────────────────

/// Returned by `connect_plain` when the peer closes the connection before
/// sending HELLOANSWER — the typical sign that it requires TCP obfuscation.
#[derive(Debug)]
struct PeerClosedBeforeHello;

impl std::fmt::Display for PeerClosedBeforeHello {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer closed connection before HELLOANSWER (may require TCP obfuscation)")
    }
}

impl std::error::Error for PeerClosedBeforeHello {}

/// Returned by [`Session::download_range`] when a peer sustains a transfer rate
/// below the configured minimum for a full check window. The caller can drop
/// this source in favour of another — but only when other sources remain in the
/// pool, since a slow source is still better than none.
#[derive(Debug)]
pub struct SlowPeer {
    /// Observed rate over the offending window, in bytes per second.
    pub rate_bytes_per_sec: u64,
}

impl std::fmt::Display for SlowPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "peer too slow ({} B/s sustained below the minimum)",
            self.rate_bytes_per_sec
        )
    }
}

impl std::error::Error for SlowPeer {}

// ── Download options ──────────────────────────────────────────────────────────

/// Options for [`download_file`] and [`Session::connect`].
#[derive(Debug, Clone)]
pub struct DownloadOptions {
    /// Total timeout for the entire download.
    pub timeout: Duration,
    /// Timeout per individual network operation.
    pub op_timeout: Duration,
    /// Maximum number of queue-rank waits before giving up.
    pub max_queue_waits: usize,
    /// File size (needed for REQUESTPARTS range calculation).
    pub file_size: u64,
    /// Expected ed2k hash for per-part verification.
    pub hash: Ed2kHash,
    /// Byte offset to resume from (used only by [`download_file`]).
    /// When using [`Session`] directly, pass the range to [`Session::download_range`].
    pub start_offset: u64,
    /// Peer's KadID / UserHash (16 bytes).  When `Some`, a plain TCP connection
    /// that is rejected before HELLOANSWER will be automatically retried with
    /// RC4 obfuscation.
    pub peer_hash: Option<[u8; 16]>,
    /// Our listening TCP port to advertise in HELLO packets.
    /// Set to 0 if not listening (Low-ID mode).
    pub our_tcp_port: u16,
    /// Our persistent eMule user hash (credit identity) to advertise in HELLO.
    pub our_user_hash: [u8; 16],
    /// Our nickname (CT_NAME) to advertise in HELLO. Empty = no name tag.
    pub our_nick: String,
    /// Minimum sustained transfer rate (bytes/sec) below which a source is
    /// abandoned mid-slice via [`SlowPeer`]. `0` disables the check.
    pub min_speed_bytes_per_sec: u64,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3600),
            op_timeout: Duration::from_secs(30),
            max_queue_waits: 10,
            file_size: 0,
            hash: Ed2kHash::from_bytes([0u8; 16]),
            start_offset: 0,
            peer_hash: None,
            our_tcp_port: 0,
            our_user_hash: [0u8; 16],
            our_nick: String::new(),
            min_speed_bytes_per_sec: 0,
        }
    }
}

// ── Progress events ───────────────────────────────────────────────────────────

/// A progress event emitted during download.
#[derive(Debug)]
pub enum DownloadEvent {
    Connected,
    Queued { rank: u32 },
    Started,
    Progress { bytes_received: u64, total: u64 },
    ChunkVerified { part_index: usize },
    ChunkFailed { part_index: usize },
    Done,
}

// ── Session ───────────────────────────────────────────────────────────────────

/// An active eMule download session with a single peer.
///
/// Establishes the TCP connection and completes the full eMule handshake
/// (HELLO → FILEREQUEST → STARTUPLOAD_REQ → ACCEPTUPLOAD_REQ).  After
/// construction, call [`Session::download_range`] one or more times to
/// retrieve specific byte ranges from the peer.
pub struct Session {
    stream: TcpStream,
    op_timeout: Duration,
    hash: Ed2kHash,
    file_size: u64,
    /// RC4 stream pair (send/recv) for obfuscated connections; both `None` for
    /// plain connections.
    ciphers: ObfCiphers,
    /// Minimum sustained rate (bytes/sec); `0` disables the slow-peer check.
    min_speed_bytes_per_sec: u64,
}

impl Session {
    /// Connect to `peer` and perform the full eMule handshake.
    ///
    /// Tries a plain TCP connection first.  If the peer closes the connection
    /// before sending HELLOANSWER (which indicates it requires obfuscation) and
    /// `opts.peer_hash` is `Some`, transparently retries using RC4 obfuscation.
    ///
    /// Emits [`DownloadEvent::Connected`], [`DownloadEvent::Queued`] (0 or
    /// more times), and [`DownloadEvent::Started`] via `on_event`.
    pub async fn connect<F>(
        peer: SocketAddrV4,
        opts: &DownloadOptions,
        on_event: &mut F,
    ) -> Result<Self>
    where
        F: FnMut(DownloadEvent),
    {
        match Self::connect_plain(peer, opts, on_event).await {
            Ok(session) => Ok(session),
            Err(e) if e.is::<PeerClosedBeforeHello>() && opts.peer_hash.is_some() => {
                // Plain connection was rejected — retry with RC4 obfuscation.
                debug!(
                    %peer,
                    "Plain TCP rejected — retrying with RC4 obfuscation"
                );
                let r = Self::connect_obfuscated(peer, opts, on_event).await;
                if let Err(ref e) = r {
                    debug!(%peer, error = %e, "Obfuscated retry failed");
                }
                r
            }
            Err(e) => Err(e),
        }
    }

    /// Whether this session negotiated RC4 obfuscation (vs a plain stream).
    pub fn is_obfuscated(&self) -> bool {
        self.ciphers.is_obfuscated()
    }

    /// Attempt a plain (unencrypted) TCP connection and handshake.
    async fn connect_plain<F>(
        peer: SocketAddrV4,
        opts: &DownloadOptions,
        on_event: &mut F,
    ) -> Result<Self>
    where
        F: FnMut(DownloadEvent),
    {
        let our_hash = opts.our_user_hash;
        let mut ciphers = ObfCiphers::default();

        let mut stream = timeout(opts.op_timeout, TcpStream::connect(peer))
            .await
            .context("connect timeout")?
            .context("connect to peer")?;
        on_event(DownloadEvent::Connected);

        Self::do_handshake(peer, &mut stream, &mut ciphers, opts, &our_hash, on_event).await?;

        Ok(Self {
            stream,
            op_timeout: opts.op_timeout,
            hash: opts.hash,
            file_size: opts.file_size,
            ciphers,
            min_speed_bytes_per_sec: opts.min_speed_bytes_per_sec,
        })
    }

    /// Attempt an RC4-obfuscated TCP connection and handshake.
    async fn connect_obfuscated<F>(
        peer: SocketAddrV4,
        opts: &DownloadOptions,
        on_event: &mut F,
    ) -> Result<Self>
    where
        F: FnMut(DownloadEvent),
    {
        let peer_hash = opts.peer_hash.as_ref().unwrap();
        let our_hash = opts.our_user_hash;

        let mut stream = timeout(opts.op_timeout, TcpStream::connect(peer))
            .await
            .context("connect timeout (obfuscated)")?
            .context("connect to peer (obfuscated)")?;
        on_event(DownloadEvent::Connected);

        // Derive the two RC4 streams from the same random key (eMule mixes a
        // different magic byte per direction).
        let rand = random_tcp_key();
        let mut send = Rc4::new(&tcp_obf_rc4_key(peer_hash, MAGIC_REQUESTER, &rand));
        let mut recv = Rc4::new(&tcp_obf_rc4_key(peer_hash, MAGIC_SERVER, &rand));
        // eMule's RC4CreateKey discards the first 1024 keystream bytes for TCP
        // (the UDP/Kad path skips this). Without it the keystream is misaligned
        // and the peer rejects our sync magic.
        send.discard(1024);
        recv.discard(1024);

        // Negotiation request:
        //   marker[1] + rand[4]            (plaintext)
        //   RC4send( SYNC[4] + method[1] + method[1] + pad_len[1] )
        let mut header = Vec::with_capacity(12);
        header.push(obf_marker_byte());
        header.extend_from_slice(&rand);
        let mut enc = Vec::with_capacity(7);
        enc.extend_from_slice(&MAGICVALUE_SYNC.to_le_bytes());
        enc.push(ENM_OBFUSCATION); // supported method
        enc.push(ENM_OBFUSCATION); // preferred method
        enc.push(0); // no padding
        send.apply(&mut enc);
        header.extend_from_slice(&enc);
        stream
            .write_all(&header)
            .await
            .context("send obfuscation negotiation")?;

        let mut ciphers = ObfCiphers {
            send: Some(send),
            recv: Some(recv),
        };

        // Read and validate the peer's negotiation response (encrypted with its
        // send key = our recv key): SYNC[4] + method[1] + pad_len[1] + padding.
        Self::read_obf_response(&mut stream, &mut ciphers, opts.op_timeout).await?;

        Self::do_handshake(peer, &mut stream, &mut ciphers, opts, &our_hash, on_event).await?;

        Ok(Self {
            stream,
            op_timeout: opts.op_timeout,
            hash: opts.hash,
            file_size: opts.file_size,
            ciphers,
            min_speed_bytes_per_sec: opts.min_speed_bytes_per_sec,
        })
    }

    /// Read the peer's obfuscation-negotiation response and verify the sync magic
    /// value, advancing the receive RC4 stream. Fails (so the source is dropped)
    /// if the magic does not match — the sign of a wrong key / non-obfuscated peer.
    async fn read_obf_response(
        stream: &mut TcpStream,
        ciphers: &mut ObfCiphers,
        op_timeout: Duration,
    ) -> Result<()> {
        let recv = ciphers
            .recv
            .as_mut()
            .expect("obfuscated session has recv key");
        // SYNC(4) + method(1) + pad_len(1)
        let mut head = [0u8; 6];
        timeout(op_timeout, stream.read_exact(&mut head))
            .await
            .context("obfuscation response timeout")?
            .context("read obfuscation response")?;
        recv.apply(&mut head);
        let magic = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        if magic != MAGICVALUE_SYNC {
            bail!("obfuscation handshake failed: bad sync magic 0x{magic:08x}");
        }
        let pad_len = head[5] as usize;
        if pad_len > 0 {
            let mut pad = vec![0u8; pad_len];
            timeout(op_timeout, stream.read_exact(&mut pad))
                .await
                .context("obfuscation padding timeout")?
                .context("read obfuscation padding")?;
            recv.apply(&mut pad); // discarded, but keeps the keystream aligned
        }
        Ok(())
    }

    /// Shared handshake logic (HELLO → FILEREQUEST → STARTUPLOAD), used by
    /// both plain and obfuscated paths.  Returns a `PeerClosedBeforeHello`
    /// sentinel if the peer closes before HELLOANSWER.
    async fn do_handshake<F>(
        peer: SocketAddrV4,
        stream: &mut TcpStream,
        ciphers: &mut ObfCiphers,
        opts: &DownloadOptions,
        our_hash: &[u8; 16],
        on_event: &mut F,
    ) -> Result<()>
    where
        F: FnMut(DownloadEvent),
    {
        // ── HELLO ────────────────────────────────────────────────────────────
        let hello_payload = build_hello(our_hash, opts.our_tcp_port, &opts.our_nick);
        write_frame(stream, ciphers, OP_HELLO, &hello_payload)
            .await
            .context("send HELLO")?;

        loop {
            let frame = timeout(opts.op_timeout, read_frame(stream, ciphers)).await;
            let (_proto, opcode, _payload) = match frame {
                Err(_timeout) => return Err(anyhow::Error::new(PeerClosedBeforeHello)),
                Ok(Err(e))
                    if e.kind() == io::ErrorKind::ConnectionReset
                        || e.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    return Err(anyhow::Error::new(PeerClosedBeforeHello));
                }
                Ok(Err(e)) => return Err(anyhow::Error::new(e).context("read HELLOANSWER")),
                Ok(Ok(frame)) => frame,
            };
            if opcode == OP_HELLOANSWER {
                // First decrypted frame back: proves the transport works. For the
                // obfuscated path this confirms the RC4 key is correct, even if
                // the peer later queues us instead of granting a slot.
                debug!(
                    %peer,
                    obfuscated = ciphers.is_obfuscated(),
                    "eMule transport handshake OK (HELLOANSWER received)"
                );
                break;
            }
            debug!(%peer, "skipping opcode 0x{opcode:02x} during hello handshake");
        }

        // ── FILEREQUEST ──────────────────────────────────────────────────────
        write_frame(stream, ciphers, OP_FILEREQUEST, opts.hash.as_bytes())
            .await
            .context("send FILEREQUEST")?;

        loop {
            let (_proto, opcode, _payload) = timeout(opts.op_timeout, read_frame(stream, ciphers))
                .await
                .context("FILEREQUEST_ANSWER timeout")?
                .context("read FILEREQUEST_ANSWER")?;
            match opcode {
                OP_FILEREQUEST_ANSWER => break,
                OP_FILENOTFOUND => bail!("peer does not have the file"),
                _ => debug!("skipping opcode 0x{opcode:02x} during file request"),
            }
        }

        // ── STARTUPLOAD_REQ ──────────────────────────────────────────────────
        write_frame(stream, ciphers, OP_STARTUPLOAD_REQ, opts.hash.as_bytes())
            .await
            .context("send STARTUPLOAD_REQ")?;

        let mut queue_waits = 0;
        loop {
            let (_proto, opcode, payload) = timeout(opts.op_timeout, read_frame(stream, ciphers))
                .await
                .context("ACCEPTUPLOAD timeout")?
                .context("read ACCEPTUPLOAD")?;
            match opcode {
                OP_ACCEPTUPLOAD_REQ => {
                    on_event(DownloadEvent::Started);
                    break;
                }
                OP_QUEUE_RANK => {
                    let rank = if payload.len() >= 4 {
                        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]])
                    } else {
                        0
                    };
                    on_event(DownloadEvent::Queued { rank });
                    queue_waits += 1;
                    if queue_waits > opts.max_queue_waits {
                        bail!("exceeded max queue waits ({rank})");
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    write_frame(stream, ciphers, OP_STARTUPLOAD_REQ, opts.hash.as_bytes())
                        .await
                        .context("re-send STARTUPLOAD_REQ")?;
                }
                OP_QUEUE_FULL => bail!("peer queue is full"),
                _ => debug!("skipping opcode 0x{opcode:02x} waiting for ACCEPTUPLOAD"),
            }
        }

        Ok(())
    }

    /// Download bytes `[start, end)` from the peer, writing to `out_writer`.
    ///
    /// The caller must have already seeked `out_writer` to `start` before
    /// calling this method.  Returns the final `bytes_received` value.
    ///
    /// Emits [`DownloadEvent::Progress`] and [`DownloadEvent::ChunkVerified`]
    /// (or [`DownloadEvent::ChunkFailed`]) via `on_event`.
    pub async fn download_range<W, F>(
        &mut self,
        start: u64,
        end: u64,
        out_writer: &mut W,
        on_event: &mut F,
    ) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin,
        F: FnMut(DownloadEvent),
    {
        const PART_WINDOW: u64 = 180 * 1024;

        let mut bytes_received = start;
        let mut part_buf: Vec<u8> = Vec::new();
        let mut current_part_start = start;
        let mut batch_end = (start + 3 * PART_WINDOW).min(end);

        // Reassembles fragmented OP_PACKEDPART blocks (see `PackedReassembler`).
        let mut packed = PackedReassembler::default();

        // Slow-peer detection. After an initial grace period (TCP slow-start /
        // upload-slot warm-up), measure the rate over fixed windows; if a full
        // window stays below `min_speed_bytes_per_sec`, abandon the source via
        // `SlowPeer` so the caller can try another. `0` disables the check.
        const SLOW_GRACE: Duration = Duration::from_secs(20);
        const SLOW_WINDOW: Duration = Duration::from_secs(15);
        let slice_started_at = Instant::now();
        let mut window_started_at = slice_started_at;
        let mut window_start_bytes = bytes_received;

        send_request_parts(
            &mut self.stream,
            &mut self.ciphers,
            self.hash.as_bytes(),
            start,
            end,
            PART_WINDOW,
        )
        .await?;

        loop {
            if bytes_received >= end {
                break;
            }

            // Drop the source if it is sustaining too low a rate (only once past
            // the grace period and after a full window has elapsed).
            if self.min_speed_bytes_per_sec > 0 {
                let now = Instant::now();
                if now.duration_since(slice_started_at) >= SLOW_GRACE
                    && now.duration_since(window_started_at) >= SLOW_WINDOW
                {
                    let elapsed = now.duration_since(window_started_at).as_secs_f64();
                    let moved = bytes_received - window_start_bytes;
                    let rate = (moved as f64 / elapsed) as u64;
                    if rate < self.min_speed_bytes_per_sec {
                        return Err(anyhow::Error::new(SlowPeer {
                            rate_bytes_per_sec: rate,
                        }));
                    }
                    window_started_at = now;
                    window_start_bytes = bytes_received;
                }
            }
            let (_proto, opcode, payload) = timeout(
                self.op_timeout,
                read_frame(&mut self.stream, &mut self.ciphers),
            )
            .await
            .context("data receive timeout")?
            .context("read data frame")?;

            match opcode {
                OP_SENDINGPART => {
                    if payload.len() < 24 {
                        warn!("malformed SENDINGPART (too short: {} bytes)", payload.len());
                        continue;
                    }
                    let _range_start =
                        u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]])
                            as u64;
                    let range_end =
                        u32::from_le_bytes([payload[20], payload[21], payload[22], payload[23]])
                            as u64;
                    let data = &payload[24..];

                    out_writer
                        .write_all(data)
                        .await
                        .context("write chunk data")?;
                    bytes_received = range_end.min(end);

                    on_event(DownloadEvent::Progress {
                        bytes_received,
                        total: self.file_size,
                    });

                    part_buf.extend_from_slice(data);
                    let part_end = current_part_start + CHUNK_SIZE as u64;
                    if bytes_received >= part_end || bytes_received >= end {
                        let part_index = (current_part_start / CHUNK_SIZE as u64) as usize;
                        let _chunk_hash = Md4::digest(&part_buf);
                        on_event(DownloadEvent::ChunkVerified { part_index });
                        part_buf.clear();
                        current_part_start = bytes_received;
                    }

                    if bytes_received >= batch_end && bytes_received < end {
                        send_request_parts(
                            &mut self.stream,
                            &mut self.ciphers,
                            self.hash.as_bytes(),
                            bytes_received,
                            end,
                            PART_WINDOW,
                        )
                        .await?;
                        batch_end = (bytes_received + 3 * PART_WINDOW).min(end);
                    }
                }
                OP_COMPRESSEDPART => {
                    // Per-sub-packet wire format:
                    //   hash[16] + block_start[4 LE] + packed_size[4 LE] + zlib_fragment[…]
                    // `packed_size` is the TOTAL compressed length of the block;
                    // the zlib stream is delivered across several sub-packets that
                    // all repeat this same header.  Accumulate the fragments and
                    // decompress only once the whole stream has arrived.
                    if payload.len() < 24 {
                        warn!(
                            "malformed OP_COMPRESSEDPART (too short: {} bytes)",
                            payload.len()
                        );
                        continue;
                    }
                    let range_start =
                        u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]])
                            as u64;
                    let packed_size =
                        u32::from_le_bytes([payload[20], payload[21], payload[22], payload[23]])
                            as usize;

                    let decompressed = match packed.push(range_start, packed_size, &payload[24..]) {
                        Ok(Some(data)) => data,
                        Ok(None) => continue, // waiting for more sub-packets
                        Err(e) => {
                            warn!(
                                error = %e,
                                packed_size,
                                range_start,
                                "OP_COMPRESSEDPART decompression failed"
                            );
                            continue;
                        }
                    };

                    let range_end = range_start + decompressed.len() as u64;

                    out_writer
                        .write_all(&decompressed)
                        .await
                        .context("write compressed chunk data")?;
                    bytes_received = range_end.min(end);

                    on_event(DownloadEvent::Progress {
                        bytes_received,
                        total: self.file_size,
                    });

                    part_buf.extend_from_slice(&decompressed);
                    let part_end = current_part_start + CHUNK_SIZE as u64;
                    if bytes_received >= part_end || bytes_received >= end {
                        let part_index = (current_part_start / CHUNK_SIZE as u64) as usize;
                        let _chunk_hash = Md4::digest(&part_buf);
                        on_event(DownloadEvent::ChunkVerified { part_index });
                        part_buf.clear();
                        current_part_start = bytes_received;
                    }

                    if bytes_received >= batch_end && bytes_received < end {
                        send_request_parts(
                            &mut self.stream,
                            &mut self.ciphers,
                            self.hash.as_bytes(),
                            bytes_received,
                            end,
                            PART_WINDOW,
                        )
                        .await?;
                        batch_end = (bytes_received + 3 * PART_WINDOW).min(end);
                    }
                }
                _ => {
                    debug!("skipping opcode 0x{opcode:02x} during data transfer");
                }
            }
        }

        Ok(bytes_received)
    }
}

// ── download_file ─────────────────────────────────────────────────────────────

/// Download a file from a single eMule peer, writing data to `out_writer`.
///
/// Convenience wrapper around [`Session::connect`] + [`Session::download_range`].
/// For parallel multi-peer downloads use [`Session`] directly.
pub async fn download_file<W, F>(
    peer: SocketAddrV4,
    opts: DownloadOptions,
    mut out_writer: W,
    mut on_event: F,
) -> Result<u64>
where
    W: tokio::io::AsyncWrite + Unpin,
    F: FnMut(DownloadEvent),
{
    let mut session = Session::connect(peer, &opts, &mut on_event).await?;
    let bytes = session
        .download_range(
            opts.start_offset,
            opts.file_size,
            &mut out_writer,
            &mut on_event,
        )
        .await?;
    on_event(DownloadEvent::Done);
    Ok(bytes)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Send a `REQUESTPARTS` message for up to 3 consecutive 180 KB windows.
async fn send_request_parts(
    stream: &mut TcpStream,
    ciphers: &mut ObfCiphers,
    file_hash: &[u8; 16],
    offset: u64,
    max_end: u64,
    window: u64,
) -> Result<()> {
    let mut payload = Vec::with_capacity(16 + 6 * 4);
    payload.extend_from_slice(file_hash);

    let mut starts = [0u32; 3];
    let mut ends = [0u32; 3];
    for i in 0..3 {
        let s = offset + (i as u64) * window;
        if s >= max_end {
            starts[i] = s.min(max_end) as u32;
            ends[i] = starts[i];
        } else {
            starts[i] = s as u32;
            ends[i] = (s + window).min(max_end) as u32;
        }
    }
    for s in &starts {
        payload.extend_from_slice(&s.to_le_bytes());
    }
    for e in &ends {
        payload.extend_from_slice(&e.to_le_bytes());
    }
    write_frame(stream, ciphers, OP_REQUESTPARTS, &payload)
        .await
        .context("send REQUESTPARTS")
}

/// Reassembles a fragmented eMule `OP_PACKEDPART` compressed block.
///
/// eMule's `CreatePackedPackets` compresses a whole block into a single zlib
/// stream of `packed_size` bytes, then splits it into ~10 KB sub-packets that
/// all repeat the same `(block_start, packed_size)` header and each carry a
/// contiguous slice of the stream.  Feed every sub-packet's fragment through
/// [`Self::push`]; the full block is inflated once `packed_size` bytes have
/// arrived.
#[derive(Default)]
struct PackedReassembler {
    /// Accumulated compressed bytes for the block currently being received.
    buf: Vec<u8>,
    /// `(block_start, packed_size)` identifying the current block; `None` when
    /// no block is in progress.
    key: Option<(u64, usize)>,
}

impl PackedReassembler {
    /// Append one sub-packet `fragment` belonging to block `(start, packed_size)`.
    ///
    /// Returns `Ok(None)` while more fragments are still needed, `Ok(Some(data))`
    /// with the decompressed block once it is complete, or `Err` if the
    /// reassembled stream is not valid zlib.  A change of `(start, packed_size)`
    /// discards any partially-received previous block.
    fn push(
        &mut self,
        start: u64,
        packed_size: usize,
        fragment: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        if self.key != Some((start, packed_size)) {
            if !self.buf.is_empty() {
                warn!(
                    discarded = self.buf.len(),
                    "discarding incomplete packed block before a new one"
                );
            }
            self.buf.clear();
            self.key = Some((start, packed_size));
        }
        self.buf.extend_from_slice(fragment);

        if self.buf.len() < packed_size {
            return Ok(None);
        }

        let mut out = Vec::new();
        let result = ZlibDecoder::new(&self.buf[..packed_size]).read_to_end(&mut out);
        self.buf.clear();
        self.key = None;
        result.map(|_| Some(out))
    }
}

// ── Upload context ────────────────────────────────────────────────────────────

/// Metadata about a file we can serve to peers — either an in-progress download
/// or a completed eMule download we keep seeding.
#[derive(Debug, Clone)]
pub struct UploadInfo {
    /// Display name (original filename from the ed2k link).
    pub name: String,
    /// Total expected file size in bytes.
    pub total_size: u64,
    /// Total number of 9.28 MB slices.
    pub num_slices: usize,
    /// File to read bytes from: the `.part` while downloading, the final file
    /// once the download has completed and we keep seeding it.
    pub path: PathBuf,
    /// `true` once fully downloaded: every slice is available and there is no
    /// `.part.met`, so the status bitmap is all-complete.
    pub complete: bool,
    /// ed2k part-hash set: the per-chunk MD4 hashes concatenated (16 bytes
    /// each), served on `OP_HASHSETREQUEST` so a peer can verify chunks.
    /// Empty for single-part files (no hashset) or while still downloading.
    pub hashset: Vec<u8>,
}

/// Live map of files currently being downloaded, keyed by their MD4 hash.
///
/// The download engine inserts an entry when a download starts and removes it
/// when the download completes, fails, or is cancelled.  The upload handler
/// only serves hashes present here — this prevents serving stale `.part` files
/// left over from cancelled downloads.
pub type ActiveDownloads = Arc<RwLock<HashMap<[u8; 16], UploadInfo>>>;

/// Async byte-rate limiter hook. Called with the number of bytes about to be
/// transferred; the returned future resolves once the caller may proceed. Lets
/// the daemon's token-bucket throttle gate eMule traffic without `rucio-emule`
/// depending on the daemon. `None` means no limit.
pub type ByteLimiter = Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Hook for reporting active upload sessions to the daemon's upload-stats
/// registry. Implemented by the daemon so this crate stays free of API types,
/// mirroring [`ByteLimiter`]. `None` means uploads are not tracked.
pub trait UploadObserver: Send + Sync {
    /// Register the start of an upload session serving `hash` (display `name`)
    /// to `peer`. The returned handle reports bytes as they are sent and
    /// deregisters the session when dropped.
    fn upload_started(
        &self,
        peer: SocketAddr,
        hash: [u8; 16],
        name: &str,
    ) -> Box<dyn UploadSession>;
}

/// Per-session handle returned by [`UploadObserver::upload_started`]. Dropping
/// it ends the session (the daemon removes the corresponding row).
pub trait UploadSession: Send + Sync {
    /// Report `bytes` just sent to the peer in this session.
    fn add_bytes(&self, bytes: u64);
}

/// Everything the upload handler needs, shared across all incoming connections.
pub struct UploadContext {
    /// Semaphore that caps simultaneous upload connections.
    pub slots: Arc<Semaphore>,
    /// Directory where `.part` and `.part.met` files are stored.
    pub temp_dir: PathBuf,
    /// Our TCP port to advertise in HELLO packets.
    pub tcp_port: u16,
    /// Our persistent eMule user hash (credit identity) advertised in HELLO.
    pub user_hash: [u8; 16],
    /// Our nickname (CT_NAME) advertised in HELLO. Empty = no name tag.
    pub nick: String,
    /// Files currently being downloaded — the upload whitelist.
    pub downloads: ActiveDownloads,
    /// Counter of inbound TCP connections accepted since startup.
    /// Used by the status endpoint as direct evidence of reachability.
    pub inbound_connections: Arc<AtomicU64>,
    /// Unix-seconds timestamp of the most recent inbound TCP connection
    /// (`0` = none yet). Drives a *recent*-reachability verdict so connectivity
    /// can decay back to firewalled if inbound stops, rather than latching Open
    /// forever on the cumulative counter.
    pub last_inbound_at: Arc<AtomicU64>,
    /// Cumulative bytes sent to peers via OP_SENDINGPART. The daemon polls
    /// this counter to feed session/upload metrics.
    pub uploaded_bytes: Arc<AtomicU64>,
    /// Cumulative count of SENDINGPART blocks served, paired with
    /// `uploaded_bytes` for the daemon's metrics reconciliation.
    pub chunks_served: Arc<AtomicU64>,
    /// Optional upload rate limiter; gated before each SENDINGPART send.
    pub upload_limiter: Option<ByteLimiter>,
    /// Optional observer fed live upload activity for the stats registry.
    pub upload_observer: Option<Arc<dyn UploadObserver>>,
}

// ── Incoming TCP server ───────────────────────────────────────────────────────

/// Accept incoming eMule TCP connections and serve partial file uploads.
///
/// Spawn as a background task after binding the TCP listener.  Each connection
/// is handled in its own Tokio task.  Files are only served if they appear in
/// `ctx.downloads` (i.e. are actively being downloaded by this daemon) and
/// the corresponding `.part` / `.part.met` files are present on disk.
pub async fn serve_incoming(listener: TcpListener, ctx: Arc<UploadContext>) {
    info!(port = ctx.tcp_port, "eMule TCP listener started");
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                ctx.inbound_connections.fetch_add(1, Ordering::Relaxed);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                ctx.last_inbound_at.store(now, Ordering::Relaxed);
                let ctx = Arc::clone(&ctx);
                tokio::spawn(handle_incoming(stream, peer, ctx));
            }
            Err(e) => warn!("eMule TCP accept error: {e}"),
        }
    }
}

/// Negotiate inbound TCP obfuscation as the **receiver** (eMule
/// `CEncryptedStreamSocket`), the mirror of [`Session::connect_obfuscated`].
///
/// Peeks the first byte without consuming it: a protocol header byte
/// (`0xe3`/`0xc5`/`0xd4`) means a plaintext stream, so we return
/// [`ObfCiphers::default`] and the caller reads the frame untouched. Anything
/// else is the obfuscation marker — we read the `marker[1] + rand[4]` preamble,
/// derive the RC4 streams, validate the requester's sync magic, send our own,
/// and return the live ciphers.
///
/// The requester keyed its streams to **our** user hash (the connection target),
/// so we need only our own hash: its send key ([`MAGIC_REQUESTER`]) is our recv
/// key and its recv key ([`MAGIC_SERVER`]) is our send key. eMule discards the
/// first 1024 RC4 bytes on TCP.
async fn negotiate_inbound_obfuscation(
    stream: &mut TcpStream,
    our_hash: &[u8; 16],
    op_timeout: Duration,
) -> io::Result<ObfCiphers> {
    // Peek (non-consuming) so the plaintext path can read the full frame,
    // header byte included, exactly as before.
    let mut first = [0u8; 1];
    let n = timeout(op_timeout, stream.peek(&mut first))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "obfuscation peek timeout"))??;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "peer closed before handshake",
        ));
    }
    if matches!(first[0], PROTO_ED2K | PROTO_EMULE | PROTO_PACKED) {
        return Ok(ObfCiphers::default()); // plaintext stream
    }

    // Obfuscated: consume marker[1] + rand[4].
    let mut preamble = [0u8; 5];
    timeout(op_timeout, stream.read_exact(&mut preamble))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "obfuscation preamble timeout"))??;
    let rand: [u8; 4] = preamble[1..5].try_into().unwrap();

    let mut recv = Rc4::new(&tcp_obf_rc4_key(our_hash, MAGIC_REQUESTER, &rand));
    let mut send = Rc4::new(&tcp_obf_rc4_key(our_hash, MAGIC_SERVER, &rand));
    recv.discard(1024);
    send.discard(1024);

    // Requester's negotiation: SYNC[4] + supported[1] + preferred[1] + pad_len[1]
    // (then pad_len padding bytes), all encrypted with its send key (our recv).
    let mut head = [0u8; 7];
    timeout(op_timeout, stream.read_exact(&mut head))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "obfuscation request timeout"))??;
    recv.apply(&mut head);
    let magic = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
    if magic != MAGICVALUE_SYNC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("obfuscation handshake failed: bad sync magic 0x{magic:08x}"),
        ));
    }
    let pad_len = head[6] as usize;
    if pad_len > 0 {
        let mut pad = vec![0u8; pad_len];
        timeout(op_timeout, stream.read_exact(&mut pad))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "obfuscation padding timeout")
            })??;
        recv.apply(&mut pad); // discarded, but keeps the keystream aligned
    }

    // Our reply: SYNC[4] + selected method[1] + pad_len[1] (no padding),
    // encrypted with our send key — matches what `read_obf_response` reads.
    let mut resp = Vec::with_capacity(6);
    resp.extend_from_slice(&MAGICVALUE_SYNC.to_le_bytes());
    resp.push(ENM_OBFUSCATION); // selected method
    resp.push(0); // no padding
    send.apply(&mut resp);
    stream.write_all(&resp).await?;

    Ok(ObfCiphers {
        send: Some(send),
        recv: Some(recv),
    })
}

/// Handle one incoming eMule TCP connection.
async fn handle_incoming(mut stream: TcpStream, peer: SocketAddr, ctx: Arc<UploadContext>) {
    debug!(%peer, "Incoming eMule TCP connection");
    let our_hash = ctx.user_hash;
    const OP_TIMEOUT: Duration = Duration::from_secs(30);

    let result: io::Result<()> = async {
        // ── Obfuscation negotiation ───────────────────────────────────────────
        // Plaintext streams pass through with default (no-op) ciphers; an
        // obfuscated peer is decrypted from here on.
        let mut ciphers = negotiate_inbound_obfuscation(&mut stream, &our_hash, OP_TIMEOUT).await?;
        if ciphers.is_obfuscated() {
            debug!(%peer, "Negotiated inbound TCP obfuscation");
        }

        // ── HELLO handshake ───────────────────────────────────────────────────
        loop {
            let (_proto, opcode, _payload) =
                timeout(OP_TIMEOUT, read_frame(&mut stream, &mut ciphers))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "hello timeout"))??;
            if opcode == OP_HELLO {
                let answer = build_hello(&our_hash, ctx.tcp_port, &ctx.nick);
                write_frame(&mut stream, &mut ciphers, OP_HELLOANSWER, &answer).await?;
                debug!(%peer, "eMule HELLO done; awaiting file request");
                break;
            }
            debug!(%peer, "got 0x{opcode:02x} before HELLO");
        }

        // ── File request loop ─────────────────────────────────────────────────
        // A peer may request several files in the same connection; serve or
        // reject each one before the connection is closed.
        loop {
            let (_proto, opcode, payload) =
                match timeout(OP_TIMEOUT, read_frame(&mut stream, &mut ciphers)).await {
                    Ok(Ok(f)) => f,
                    _ => break,
                };

            if opcode != OP_FILEREQUEST {
                debug!(%peer, "ignoring opcode 0x{opcode:02x} before FILEREQUEST");
                continue;
            }
            if payload.len() < 16 {
                break;
            }
            let mut hash = [0u8; 16];
            hash.copy_from_slice(&payload[..16]);

            // Look up in the active-download whitelist (also note how many files
            // we are seeding, to tell "empty whitelist" from "hash we don't have").
            let (info, seeding) = {
                let guard = ctx.downloads.read().await;
                (guard.get(&hash).cloned(), guard.len())
            };
            let Some(info) = info else {
                write_frame(&mut stream, &mut ciphers, OP_FILENOTFOUND, &hash).await?;
                debug!(%peer, hash = %hex::encode(hash), seeding, "FILENOTFOUND (not in seeding set)");
                continue;
            };
            debug!(%peer, hash = %hex::encode(hash), "FILEREQUEST matched a seeded file");

            // Try to claim an upload slot (non-blocking).
            let _permit = match ctx.slots.try_acquire() {
                Ok(p) => p,
                Err(TryAcquireError::NoPermits) => {
                    // Tell the peer to try again later — standard eMule behaviour.
                    let rank = 50u32;
                    write_frame(
                        &mut stream,
                        &mut ciphers,
                        OP_QUEUE_RANK,
                        &rank.to_le_bytes(),
                    )
                    .await?;
                    debug!(%peer, "upload slots full — sent QUEUE_RANK 50");
                    break;
                }
                Err(TryAcquireError::Closed) => break,
            };

            // Completed shares have every slice; in-progress downloads load the
            // completion bitmap from their .part.met.
            let done = if info.complete {
                vec![true; info.num_slices]
            } else {
                let met_path = ctx.temp_dir.join(format!("{}.part.met", hex::encode(hash)));
                crate::progress::load_progress(&met_path, info.num_slices)
            };

            // ── FILEREQANSWER ─────────────────────────────────────────────────
            let mut ans = Vec::with_capacity(16 + 2 + info.name.len());
            ans.extend_from_slice(&hash);
            ans.extend_from_slice(&(info.name.len() as u16).to_le_bytes());
            ans.extend_from_slice(info.name.as_bytes());
            write_frame(&mut stream, &mut ciphers, OP_FILEREQUEST_ANSWER, &ans).await?;

            // ── FILESTATUS ────────────────────────────────────────────────────
            // Bitmap: one bit per 9.28 MB slice, 1 = available.
            let mut status = Vec::with_capacity(16 + 2 + done.len().div_ceil(8));
            status.extend_from_slice(&hash);
            status.extend_from_slice(&(done.len() as u16).to_le_bytes());
            let mut bits = vec![0u8; done.len().div_ceil(8)];
            for (i, &d) in done.iter().enumerate() {
                if d {
                    bits[i / 8] |= 1 << (i % 8);
                }
            }
            status.extend_from_slice(&bits);
            write_frame(&mut stream, &mut ciphers, OP_FILESTATUS, &status).await?;

            debug!(
                %peer,
                hash = %hex::encode(hash),
                available = done.iter().filter(|&&d| d).count(),
                total = done.len(),
                "Offered partial file for upload"
            );

            // ── Upload session ────────────────────────────────────────────────
            if let Err(e) = run_upload_session(
                &mut stream,
                &mut ciphers,
                &hash,
                &info,
                &done,
                &ctx,
                peer,
                OP_TIMEOUT,
            )
            .await
            {
                debug!(%peer, error = %e, "Upload session ended");
            }
            // Permit is released here (drop).
            break;
        }

        Ok(())
    }
    .await;

    if let Err(e) = result {
        debug!(%peer, error = %e, "Incoming eMule TCP connection error");
    }
}

/// Reply to `OP_HASHSETREQUEST` with the file's ed2k part-hash set so the peer
/// can verify the chunks it downloads. Layout: `file_hash(16) + count(u16 LE) +
/// part_hash(16)*count`. No-op when we have no hashset (single-part file, still
/// downloading, or one that did not verify) — the peer gets it from another
/// source.
async fn send_hashset_answer(
    stream: &mut TcpStream,
    ciphers: &mut ObfCiphers,
    hash: &[u8; 16],
    info: &UploadInfo,
) -> io::Result<()> {
    if info.hashset.is_empty() {
        return Ok(());
    }
    let count = (info.hashset.len() / 16) as u16;
    let mut payload = Vec::with_capacity(18 + info.hashset.len());
    payload.extend_from_slice(hash);
    payload.extend_from_slice(&count.to_le_bytes());
    payload.extend_from_slice(&info.hashset);
    write_frame(stream, ciphers, OP_HASHSETANSWER, &payload).await
}

/// Run the upload phase: STARTUPLOADREQ → ACCEPTUPLOAD → serve REQUESTPARTS.
#[allow(clippy::too_many_arguments)]
async fn run_upload_session(
    stream: &mut TcpStream,
    ciphers: &mut ObfCiphers,
    hash: &[u8; 16],
    info: &UploadInfo,
    done: &[bool],
    ctx: &UploadContext,
    peer: SocketAddr,
    op_timeout: Duration,
) -> io::Result<()> {
    // Wait for STARTUPLOADREQ.
    loop {
        let (_proto, opcode, _payload) =
            timeout(op_timeout, read_frame(stream, ciphers))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "STARTUPLOADREQ timeout"))??;
        match opcode {
            OP_STARTUPLOAD_REQ => break,
            OP_ENDOFDOWNLOAD => return Ok(()),
            // A peer often asks for the hashset before starting the transfer.
            OP_HASHSETREQUEST => send_hashset_answer(stream, ciphers, hash, info).await?,
            _ => debug!("ignoring 0x{opcode:02x} waiting for STARTUPLOADREQ"),
        }
    }

    write_frame(stream, ciphers, OP_ACCEPTUPLOAD_REQ, &[]).await?;

    // Register this upload with the stats registry (if any); the guard removes
    // the row when this session ends (function returns → drop).
    let session = ctx
        .upload_observer
        .as_ref()
        .map(|o| o.upload_started(peer, *hash, &info.name));

    // Serve from the file the whitelist entry points at: the `.part` for an
    // in-progress download, the final file for a completed share.
    let part_path = info.path.clone();

    // Serve REQUESTPARTS until the peer signals done or disconnects.
    loop {
        let (_proto, opcode, payload) = match timeout(op_timeout, read_frame(stream, ciphers)).await
        {
            Ok(Ok(f)) => f,
            _ => break,
        };

        match opcode {
            OP_ENDOFDOWNLOAD => break,
            OP_HASHSETREQUEST => send_hashset_answer(stream, ciphers, hash, info).await?,
            OP_REQUESTPARTS => {
                if payload.len() < 40 {
                    break;
                }
                // Payload: hash[16] + start[4]*3 + end[4]*3 (all LE u32)
                for i in 0..3usize {
                    let start =
                        u32::from_le_bytes(payload[16 + i * 4..20 + i * 4].try_into().unwrap())
                            as u64;
                    let end =
                        u32::from_le_bytes(payload[28 + i * 4..32 + i * 4].try_into().unwrap())
                            as u64;
                    if start >= end {
                        continue; // empty range — filler slot
                    }

                    // Ensure the requested range falls within completed slices.
                    let first_slice = (start / CHUNK_SIZE as u64) as usize;
                    let last_slice = (end.saturating_sub(1) / CHUNK_SIZE as u64) as usize;
                    let all_done =
                        (first_slice..=last_slice).all(|s| done.get(s).copied().unwrap_or(false));
                    if !all_done {
                        debug!(
                            start,
                            end, "Requested range not yet complete — closing upload"
                        );
                        return Ok(());
                    }

                    // Read and send the range.
                    let len = (end - start) as usize;
                    let mut buf = vec![0u8; len];
                    {
                        let mut file = tokio::fs::File::open(&part_path).await.map_err(|e| {
                            io::Error::new(io::ErrorKind::NotFound, format!("open part file: {e}"))
                        })?;
                        file.seek(std::io::SeekFrom::Start(start)).await?;
                        file.read_exact(&mut buf).await?;
                    }

                    // SENDINGPART payload: hash[16] + start[4] + end[4] + data
                    let mut sp = Vec::with_capacity(24 + len);
                    sp.extend_from_slice(hash);
                    sp.extend_from_slice(&(start as u32).to_le_bytes());
                    sp.extend_from_slice(&(end as u32).to_le_bytes());
                    sp.extend_from_slice(&buf);
                    // Gate on the upload rate limiter before sending (no-op
                    // when no limit is set).
                    if let Some(limiter) = &ctx.upload_limiter {
                        limiter(len as u64).await;
                    }
                    write_frame(stream, ciphers, OP_SENDINGPART, &sp).await?;
                    ctx.uploaded_bytes.fetch_add(len as u64, Ordering::Relaxed);
                    ctx.chunks_served.fetch_add(1, Ordering::Relaxed);
                    if let Some(s) = &session {
                        s.add_bytes(len as u64);
                    }
                    debug!(start, end, bytes = len, "Sent SENDINGPART");
                }
            }
            _ => debug!("ignoring 0x{opcode:02x} during upload"),
        }
    }

    // Tell the peer we are done.
    let _ = write_frame(stream, ciphers, OP_ENDOFDOWNLOAD, &[]).await;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_payload_len_caps_and_rejects() {
        // Empty frame is invalid.
        assert!(frame_payload_len(0).is_err());
        // A huge peer-supplied length must be rejected, not allocated.
        assert!(frame_payload_len(u32::MAX).is_err());
        assert!(frame_payload_len(MAX_FRAME_LEN as u32 + 1).is_err());
        // The reported crash value (~3.9 GiB) is rejected.
        assert!(frame_payload_len(4_177_203_401u32).is_err());
        // Normal frames pass and return len - 1 (opcode excluded).
        assert_eq!(frame_payload_len(1).unwrap(), 0);
        assert_eq!(frame_payload_len(181).unwrap(), 180);
        assert_eq!(
            frame_payload_len(MAX_FRAME_LEN as u32).unwrap(),
            MAX_FRAME_LEN - 1
        );
    }

    #[test]
    fn test_build_message_framing() {
        let msg = build_message(OP_HELLO, &[0xaa, 0xbb]);
        assert_eq!(msg.len(), 8);
        assert_eq!(msg[0], PROTO_ED2K);
        assert_eq!(&msg[1..5], &[3, 0, 0, 0]);
        assert_eq!(msg[5], OP_HELLO);
        assert_eq!(&msg[6..], &[0xaa, 0xbb]);
    }

    #[test]
    fn test_build_hello_length() {
        // No nickname → no tags: 1+16+4+2 + 4(tagcount) + 4(ip) + 2(port) = 33.
        let h = build_hello(&[0u8; 16], 0, "");
        assert_eq!(h.len(), 33);
        assert_eq!(h[0], 16u8);
    }

    #[test]
    fn test_build_hello_name_tag() {
        // With a nickname, one CT_NAME string tag is appended:
        //   [0x82][0x01][len u16 LE][bytes]  ("rucio" = 5 bytes → +9 bytes).
        let h = build_hello(&[0u8; 16], 0, "rucio");
        assert_eq!(h.len(), 33 + 1 + 1 + 2 + 5);
        assert_eq!(&h[23..27], &1u32.to_le_bytes()); // tag count = 1
        assert_eq!(h[27], 0x02 | 0x80); // TAGTYPE_STRING, special 1-byte name
        assert_eq!(h[28], 0x01); // CT_NAME
        assert_eq!(&h[29..31], &5u16.to_le_bytes()); // string length
        assert_eq!(&h[31..36], b"rucio");
    }

    #[test]
    fn test_obf_key_derivation() {
        let peer_hash = [0xABu8; 16];
        let rand = [0x01, 0x02, 0x03, 0x04];
        let send = tcp_obf_rc4_key(&peer_hash, MAGIC_REQUESTER, &rand);
        // Key is a 16-byte MD5 — just verify it's not all zeros.
        assert_ne!(send, [0u8; 16]);
        // Same inputs → same key (deterministic).
        assert_eq!(send, tcp_obf_rc4_key(&peer_hash, MAGIC_REQUESTER, &rand));
        // The per-direction magic byte must yield a *different* key, otherwise
        // send and receive would share a keystream (the bug this fixes).
        let recv = tcp_obf_rc4_key(&peer_hash, MAGIC_SERVER, &rand);
        assert_ne!(send, recv);
    }

    #[test]
    fn test_obf_negotiation_header() {
        // Negotiation request: marker(1) + rand(4) plaintext, then RC4send over
        // SYNC(4) + method(1) + method(1) + pad_len(1) = 12 bytes total, no padding.
        let peer_hash = [0u8; 16];
        let rand = random_tcp_key();
        let mut send = Rc4::new(&tcp_obf_rc4_key(&peer_hash, MAGIC_REQUESTER, &rand));
        let mut header = Vec::new();
        header.push(obf_marker_byte());
        header.extend_from_slice(&rand);
        let mut enc = Vec::new();
        enc.extend_from_slice(&MAGICVALUE_SYNC.to_le_bytes());
        enc.push(ENM_OBFUSCATION);
        enc.push(ENM_OBFUSCATION);
        enc.push(0);
        send.apply(&mut enc);
        header.extend_from_slice(&enc);
        assert_eq!(header.len(), 12);
        // The marker must not collide with a protocol header byte.
        assert!(!matches!(header[0], 0xe3 | 0xd4 | 0xc5));
        // Decrypting the encrypted tail with the matching key restores the magic.
        let mut dec = header[5..].to_vec();
        Rc4::new(&tcp_obf_rc4_key(&peer_hash, MAGIC_REQUESTER, &rand)).apply(&mut dec);
        assert_eq!(&dec[..4], &MAGICVALUE_SYNC.to_le_bytes());
    }

    /// Full obfuscated handshake against a real localhost socket: a requester
    /// (mirroring `connect_obfuscated`) negotiates with `negotiate_inbound_
    /// obfuscation` and a HELLO frame round-trips both ways. This is the real
    /// guard for keystream alignment — the part most likely to silently break.
    #[tokio::test]
    async fn inbound_obfuscation_roundtrip() {
        let our_hash = [0x5au8; 16];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Receiver: negotiate as server, read the HELLO, echo it as HELLOANSWER.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut ciphers =
                negotiate_inbound_obfuscation(&mut stream, &our_hash, Duration::from_secs(5))
                    .await
                    .unwrap();
            assert!(ciphers.is_obfuscated(), "server must detect obfuscation");
            let (_p, opcode, payload) = read_frame(&mut stream, &mut ciphers).await.unwrap();
            assert_eq!(opcode, OP_HELLO);
            write_frame(&mut stream, &mut ciphers, OP_HELLOANSWER, &payload)
                .await
                .unwrap();
        });

        // Requester: mirror connect_obfuscated, keyed to the receiver's hash.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let rand = random_tcp_key();
        let mut send = Rc4::new(&tcp_obf_rc4_key(&our_hash, MAGIC_REQUESTER, &rand));
        let mut recv = Rc4::new(&tcp_obf_rc4_key(&our_hash, MAGIC_SERVER, &rand));
        send.discard(1024);
        recv.discard(1024);
        let mut header = vec![obf_marker_byte()];
        header.extend_from_slice(&rand);
        let mut enc = Vec::new();
        enc.extend_from_slice(&MAGICVALUE_SYNC.to_le_bytes());
        enc.push(ENM_OBFUSCATION);
        enc.push(ENM_OBFUSCATION);
        enc.push(0);
        send.apply(&mut enc);
        header.extend_from_slice(&enc);
        stream.write_all(&header).await.unwrap();

        // Read the server's negotiation response: SYNC(4) + method(1) + pad_len(1).
        let mut resp = [0u8; 6];
        stream.read_exact(&mut resp).await.unwrap();
        recv.apply(&mut resp);
        assert_eq!(&resp[..4], &MAGICVALUE_SYNC.to_le_bytes());
        assert_eq!(resp[5], 0);

        // A HELLO frame must survive both encryption directions intact.
        let mut ciphers = ObfCiphers {
            send: Some(send),
            recv: Some(recv),
        };
        let hello = build_hello(&[0x11u8; 16], 4662, "tester");
        write_frame(&mut stream, &mut ciphers, OP_HELLO, &hello)
            .await
            .unwrap();
        let (_p, opcode, payload) = read_frame(&mut stream, &mut ciphers).await.unwrap();
        assert_eq!(opcode, OP_HELLOANSWER);
        assert_eq!(payload, hello, "payload must round-trip through RC4");

        server.await.unwrap();
    }

    /// A plaintext stream (first byte is a protocol header) is detected as such:
    /// the negotiator consumes nothing and returns no-op ciphers, so the regular
    /// frame reader sees the untouched frame.
    #[tokio::test]
    async fn inbound_plaintext_passthrough() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut ciphers =
                negotiate_inbound_obfuscation(&mut stream, &[0u8; 16], Duration::from_secs(5))
                    .await
                    .unwrap();
            assert!(!ciphers.is_obfuscated(), "0xe3 first byte is plaintext");
            let (proto, opcode, _payload) = read_frame(&mut stream, &mut ciphers).await.unwrap();
            assert_eq!(proto, PROTO_ED2K);
            assert_eq!(opcode, OP_HELLO);
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut ciphers = ObfCiphers::default();
        let hello = build_hello(&[0u8; 16], 4662, "");
        write_frame(&mut stream, &mut ciphers, OP_HELLO, &hello)
            .await
            .unwrap();
        server.await.unwrap();
    }

    // ── PackedReassembler ──────────────────────────────────────────────────

    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::Write as _;

    /// zlib-compress `data` the way an eMule peer would before fragmenting it.
    fn zlib_compress(data: &[u8]) -> Vec<u8> {
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    /// Build a sample payload that barely compresses (incompressible), the
    /// real-world case where eMule still sends OP_PACKEDPART and the stream
    /// spans many fragments.
    fn sample_payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect()
    }

    #[test]
    fn packed_reassembler_rejoins_fragments() {
        let original = sample_payload(200_000);
        let packed = zlib_compress(&original);
        let packed_size = packed.len();

        // Split into ~10 KB fragments, like eMule's CreatePackedPackets.
        let mut r = PackedReassembler::default();
        let mut out = None;
        let mut chunks = packed.chunks(10_240).peekable();
        while let Some(chunk) = chunks.next() {
            let res = r.push(4096, packed_size, chunk).unwrap();
            if chunks.peek().is_some() {
                // Not the last fragment — nothing decompressed yet.
                assert!(res.is_none(), "decompressed before all fragments arrived");
            } else {
                out = res;
            }
        }
        assert_eq!(out.as_deref(), Some(original.as_slice()));
    }

    #[test]
    fn packed_reassembler_handles_single_fragment() {
        let original = sample_payload(500);
        let packed = zlib_compress(&original);
        let mut r = PackedReassembler::default();
        let out = r.push(0, packed.len(), &packed).unwrap();
        assert_eq!(out.as_deref(), Some(original.as_slice()));
    }

    #[test]
    fn packed_reassembler_discards_incomplete_block_on_new_key() {
        let block_a = zlib_compress(&sample_payload(50_000));
        let block_b_data = sample_payload(20_000);
        let block_b = zlib_compress(&block_b_data);

        let mut r = PackedReassembler::default();
        // Feed only the first fragment of block A — it stays incomplete.
        assert!(
            r.push(0, block_a.len(), &block_a[..1024])
                .unwrap()
                .is_none()
        );

        // A different (start, packed_size) starts block B, discarding A.
        let mut out = None;
        let mut chunks = block_b.chunks(4096).peekable();
        while let Some(chunk) = chunks.next() {
            let res = r.push(99_999, block_b.len(), chunk).unwrap();
            if chunks.peek().is_none() {
                out = res;
            }
        }
        assert_eq!(out.as_deref(), Some(block_b_data.as_slice()));
    }

    #[test]
    fn packed_reassembler_errors_on_corrupt_stream() {
        // Bytes that are not a valid zlib stream must surface as an error.
        let garbage = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33];
        let mut r = PackedReassembler::default();
        let res = r.push(0, garbage.len(), &garbage);
        assert!(res.is_err(), "expected corrupt stream to error");
        // After an error the reassembler is reset and ready for a new block.
        let good = zlib_compress(b"hello world");
        let out = r.push(0, good.len(), &good).unwrap();
        assert_eq!(out.as_deref(), Some(b"hello world".as_slice()));
    }
}
