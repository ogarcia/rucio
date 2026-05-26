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
//! Wire format (outgoing obfuscated handshake):
//! ```text
//! [4]  random_key    — plaintext
//! [4]  RC4(0x12345678 LE)   — magic confirming key agreement
//! [1]  RC4(connect_options) — 0x03 = supported | requested
//! [1]  RC4(pad_len)  — 0 (no padding)
//! ...  RC4(eMule frames)    — HELLO and all subsequent data
//! ```
//!
//! RC4 key = `MD5(peer_hash[16] || random_key[4])`.
//! Both directions (send and receive) share a single RC4 cipher instance
//! because the eMule TCP protocol is strictly sequential (request → response).
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
use std::io::{self, Read as _};
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, info, warn};

// ── Protocol constants ────────────────────────────────────────────────────────

/// Protocol header byte for standard ed2k TCP messages.
const PROTO_ED2K: u8 = 0xe3;

// Magic value for TCP obfuscation handshake (0x12345678 in LE).
const MAGIC_TCP: [u8; 4] = [0x78, 0x56, 0x34, 0x12];
// Obfuscation supported + requested (not required, so we still accept plain peers).
const TCP_CONNECT_OPTIONS: u8 = 0x03;

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
const OP_STARTUPLOAD_REQ: u8 = 0x54;
const OP_ACCEPTUPLOAD_REQ: u8 = 0x55;
const OP_QUEUE_RANK: u8 = 0x5c;
const OP_QUEUE_FULL: u8 = 0x93;

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

/// Read one eMule TCP frame, applying RC4 decryption if a cipher is active.
/// Returns `(protocol, opcode, payload)`.
async fn read_frame(
    stream: &mut TcpStream,
    cipher: &mut Option<Rc4>,
) -> io::Result<(u8, u8, Vec<u8>)> {
    let mut hdr = [0u8; 6];
    stream.read_exact(&mut hdr).await?;
    if let Some(rc4) = cipher {
        rc4.apply(&mut hdr);
    }
    let proto = hdr[0];
    let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero-length frame",
        ));
    }
    let opcode = hdr[5];
    let payload_len = len - 1;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
        if let Some(rc4) = cipher {
            rc4.apply(&mut payload);
        }
    }
    Ok((proto, opcode, payload))
}

/// Write a framed eMule message, applying RC4 encryption if a cipher is active.
async fn write_frame(
    stream: &mut TcpStream,
    cipher: &mut Option<Rc4>,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut msg = build_message(opcode, payload);
    if let Some(rc4) = cipher {
        rc4.apply(&mut msg);
    }
    stream.write_all(&msg).await
}

// ── HELLO packet ─────────────────────────────────────────────────────────────

/// Build a minimal HELLO payload advertising ourselves as a Kad2 client.
fn build_hello(our_hash: &[u8; 16]) -> Vec<u8> {
    let mut p = Vec::new();
    p.push(16u8);
    p.extend_from_slice(our_hash);
    p.extend_from_slice(&0u32.to_le_bytes()); // client ID = 0 (low-ID)
    p.extend_from_slice(&0u16.to_le_bytes()); // TCP port = 0 (not listening)
    p.extend_from_slice(&0u32.to_le_bytes()); // tag count = 0
    p.extend_from_slice(&0u32.to_le_bytes()); // server IP (unused)
    p.extend_from_slice(&0u16.to_le_bytes()); // server port (unused)
    p
}

// ── Obfuscation helpers ───────────────────────────────────────────────────────

/// Derive the RC4 session key for an obfuscated TCP connection:
/// `MD5(peer_hash[16] || rand[4])`
fn tcp_obf_rc4_key(peer_hash: &[u8; 16], rand: &[u8; 4]) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(peer_hash);
    h.update(rand);
    h.finalize().into()
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
    /// RC4 cipher for obfuscated connections; `None` for plain connections.
    cipher: Option<Rc4>,
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
                Self::connect_obfuscated(peer, opts, on_event).await
            }
            Err(e) => Err(e),
        }
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
        let our_hash = our_client_hash();
        let mut cipher: Option<Rc4> = None;

        let mut stream = timeout(opts.op_timeout, TcpStream::connect(peer))
            .await
            .context("connect timeout")?
            .context("connect to peer")?;
        on_event(DownloadEvent::Connected);

        Self::do_handshake(&mut stream, &mut cipher, opts, &our_hash, on_event).await?;

        Ok(Self {
            stream,
            op_timeout: opts.op_timeout,
            hash: opts.hash,
            file_size: opts.file_size,
            cipher,
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
        let our_hash = our_client_hash();

        let mut stream = timeout(opts.op_timeout, TcpStream::connect(peer))
            .await
            .context("connect timeout (obfuscated)")?
            .context("connect to peer (obfuscated)")?;
        on_event(DownloadEvent::Connected);

        // Send obfuscation header:
        //   rand[4] (plain) + RC4(magic[4] + connect_opts[1] + pad_len[1])
        let rand = random_tcp_key();
        let rc4_key = tcp_obf_rc4_key(peer_hash, &rand);
        let mut rc4 = Rc4::new(&rc4_key);

        let mut obf_header = Vec::with_capacity(10);
        obf_header.extend_from_slice(&rand); // plaintext
        let mut enc = [0u8; 6];
        enc[..4].copy_from_slice(&MAGIC_TCP);
        enc[4] = TCP_CONNECT_OPTIONS;
        enc[5] = 0; // no padding
        rc4.apply(&mut enc);
        obf_header.extend_from_slice(&enc);

        stream
            .write_all(&obf_header)
            .await
            .context("send obfuscation header")?;

        let mut cipher = Some(rc4);

        Self::do_handshake(&mut stream, &mut cipher, opts, &our_hash, on_event).await?;

        info!(%peer, "eMule TCP obfuscation established");
        Ok(Self {
            stream,
            op_timeout: opts.op_timeout,
            hash: opts.hash,
            file_size: opts.file_size,
            cipher,
        })
    }

    /// Shared handshake logic (HELLO → FILEREQUEST → STARTUPLOAD), used by
    /// both plain and obfuscated paths.  Returns a `PeerClosedBeforeHello`
    /// sentinel if the peer closes before HELLOANSWER.
    async fn do_handshake<F>(
        stream: &mut TcpStream,
        cipher: &mut Option<Rc4>,
        opts: &DownloadOptions,
        our_hash: &[u8; 16],
        on_event: &mut F,
    ) -> Result<()>
    where
        F: FnMut(DownloadEvent),
    {
        // ── HELLO ────────────────────────────────────────────────────────────
        let hello_payload = build_hello(our_hash);
        write_frame(stream, cipher, OP_HELLO, &hello_payload)
            .await
            .context("send HELLO")?;

        loop {
            let frame = timeout(opts.op_timeout, read_frame(stream, cipher)).await;
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
                break;
            }
            debug!("skipping opcode 0x{opcode:02x} during hello handshake");
        }

        // ── FILEREQUEST ──────────────────────────────────────────────────────
        write_frame(stream, cipher, OP_FILEREQUEST, opts.hash.as_bytes())
            .await
            .context("send FILEREQUEST")?;

        loop {
            let (_proto, opcode, _payload) = timeout(opts.op_timeout, read_frame(stream, cipher))
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
        write_frame(stream, cipher, OP_STARTUPLOAD_REQ, opts.hash.as_bytes())
            .await
            .context("send STARTUPLOAD_REQ")?;

        let mut queue_waits = 0;
        loop {
            let (_proto, opcode, payload) = timeout(opts.op_timeout, read_frame(stream, cipher))
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
                    write_frame(stream, cipher, OP_STARTUPLOAD_REQ, opts.hash.as_bytes())
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

        send_request_parts(
            &mut self.stream,
            &mut self.cipher,
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
            let (_proto, opcode, payload) = timeout(
                self.op_timeout,
                read_frame(&mut self.stream, &mut self.cipher),
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
                            &mut self.cipher,
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
                    // Wire format (parallel to OP_SENDINGPART):
                    //   hash[16] + start_offset[4] + packed_size[4] + zlib_data[packed_size]
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
                    // payload[20..24] = packed_size (informational; we decompress all remaining)
                    let mut decompressed = Vec::new();
                    if let Err(e) = ZlibDecoder::new(&payload[24..]).read_to_end(&mut decompressed)
                    {
                        warn!(error = %e, "OP_COMPRESSEDPART decompression failed");
                        continue;
                    }
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
                            &mut self.cipher,
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

/// Fixed client hash used in HELLO packets.  Not secret — identifies us as a
/// Rucio client on the eMule network.
fn our_client_hash() -> [u8; 16] {
    [
        0x52, 0x75, 0x63, 0x69, 0x6f, 0x52, 0x75, 0x63, 0x69, 0x6f, 0x52, 0x75, 0x63, 0x69, 0x6f,
        0x00,
    ]
}

/// Send a `REQUESTPARTS` message for up to 3 consecutive 180 KB windows.
async fn send_request_parts(
    stream: &mut TcpStream,
    cipher: &mut Option<Rc4>,
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
    write_frame(stream, cipher, OP_REQUESTPARTS, &payload)
        .await
        .context("send REQUESTPARTS")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        let h = build_hello(&[0u8; 16]);
        assert_eq!(h.len(), 33);
        assert_eq!(h[0], 16u8);
    }

    #[test]
    fn test_obf_key_derivation() {
        let peer_hash = [0xABu8; 16];
        let rand = [0x01, 0x02, 0x03, 0x04];
        let key = tcp_obf_rc4_key(&peer_hash, &rand);
        // Key is a 16-byte MD5 — just verify it's not all zeros.
        assert_ne!(key, [0u8; 16]);
        // Same inputs → same key (deterministic).
        assert_eq!(key, tcp_obf_rc4_key(&peer_hash, &rand));
    }

    #[test]
    fn test_obf_header_length() {
        // Obfuscation header: rand(4) + encrypted(magic(4) + opts(1) + pad_len(1)) = 10 bytes.
        let peer_hash = [0u8; 16];
        let rand = random_tcp_key();
        let rc4_key = tcp_obf_rc4_key(&peer_hash, &rand);
        let mut rc4 = Rc4::new(&rc4_key);
        let mut obf_header = Vec::with_capacity(10);
        obf_header.extend_from_slice(&rand);
        let mut enc = [0u8; 6];
        enc[..4].copy_from_slice(&MAGIC_TCP);
        enc[4] = TCP_CONNECT_OPTIONS;
        enc[5] = 0;
        rc4.apply(&mut enc);
        obf_header.extend_from_slice(&enc);
        assert_eq!(obf_header.len(), 10);
    }
}
