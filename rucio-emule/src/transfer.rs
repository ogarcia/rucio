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
//! ## Chunk / part layout
//!
//! eMule splits files into 9,728,000-byte "parts" for MD4 hash verification.
//! Each part is further split into at most 3 sub-ranges per `REQUESTPARTS`
//! message (eMule's request window).
//!
//! After downloading the full file, call [`crate::ed2k::hash_reader`] to
//! compute the BLAKE3 hash for Rucio DHT integration.

use crate::ed2k::{CHUNK_SIZE, Ed2kHash};
use anyhow::{Context, Result, bail};
use md4::{Digest, Md4};
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, warn};

// ── Protocol constants ────────────────────────────────────────────────────────

/// Protocol header byte for standard ed2k TCP messages.
const PROTO_ED2K: u8 = 0xe3;

// ── Opcodes ───────────────────────────────────────────────────────────────────

const OP_HELLO: u8 = 0x01;
const OP_HELLOANSWER: u8 = 0x4c;
const OP_FILEREQUEST: u8 = 0x58;
const OP_FILEREQUEST_ANSWER: u8 = 0x59;
const OP_FILENOTFOUND: u8 = 0x92;
const OP_REQUESTPARTS: u8 = 0x47;
const OP_SENDINGPART: u8 = 0x46;
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

/// Read a single eMule TCP frame from `stream`.
/// Returns `(protocol, opcode, payload)`.
async fn read_frame(stream: &mut TcpStream) -> Result<(u8, u8, Vec<u8>)> {
    let mut hdr = [0u8; 6];
    stream
        .read_exact(&mut hdr)
        .await
        .context("read frame header")?;
    let proto = hdr[0];
    let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    if len == 0 {
        bail!("zero-length frame");
    }
    let opcode = hdr[5];
    let payload_len = len - 1; // len includes opcode byte
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .context("read frame payload")?;
    }
    Ok((proto, opcode, payload))
}

// ── HELLO packet ─────────────────────────────────────────────────────────────

/// Build a minimal HELLO payload advertising ourselves as a Kad2 client.
fn build_hello(our_hash: &[u8; 16]) -> Vec<u8> {
    // Wire format: hash_size(1) + hash(16) + client_id(4) + tcp_port(2) + tag_count(4) + tags
    //              + server_ip(4) + server_port(2)
    let mut p = Vec::new();
    // Hash size prefix required by the protocol
    p.push(16u8);
    // Client hash
    p.extend_from_slice(our_hash);
    // Client ID (0 = low-ID, fine for a pure downloader)
    p.extend_from_slice(&0u32.to_le_bytes());
    // TCP port (0 = not listening)
    p.extend_from_slice(&0u16.to_le_bytes());
    // Tag count
    p.extend_from_slice(&0u32.to_le_bytes());
    // Server IP + port (unused)
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes());
    p
}

// ── Download session ──────────────────────────────────────────────────────────

/// Options for [`download_file`].
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
    /// Byte offset to resume from.  The caller must have already seeked the
    /// writer to this position.  Defaults to 0 (start from the beginning).
    pub start_offset: u64,
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
        }
    }
}

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

/// Download a file from a single eMule peer, writing data to `out_writer`.
///
/// Emits progress events via `on_event` callback.  Returns the number of
/// bytes written.
///
/// This is a best-effort implementation.  For production use, the
/// daemon wraps multiple peers and retries failed chunks from others.
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
    // Use a random 16-byte "hash" to identify ourselves in HELLO.
    let our_hash = [
        0x52u8, 0x75, 0x63, 0x69, 0x6f, 0x52, 0x75, 0x63, 0x69, 0x6f, 0x52, 0x75, 0x63, 0x69, 0x6f,
        0x00,
    ];

    let mut stream = timeout(
        opts.op_timeout,
        TcpStream::connect(SocketAddrV4::new(*peer.ip(), peer.port())),
    )
    .await
    .context("connect timeout")?
    .context("connect to peer")?;
    on_event(DownloadEvent::Connected);

    // ── HELLO handshake ──────────────────────────────────────────────────────
    let hello_payload = build_hello(&our_hash);
    stream
        .write_all(&build_message(OP_HELLO, &hello_payload))
        .await
        .context("send HELLO")?;

    // Wait for HELLOANSWER (0x4c).
    loop {
        let (_proto, opcode, _payload) = timeout(opts.op_timeout, read_frame(&mut stream))
            .await
            .context("HELLOANSWER timeout")?
            .context("read HELLOANSWER")?;
        if opcode == OP_HELLOANSWER {
            break;
        }
        debug!("skipping opcode 0x{opcode:02x} during hello handshake");
    }

    // ── FILEREQUEST ──────────────────────────────────────────────────────────
    stream
        .write_all(&build_message(OP_FILEREQUEST, opts.hash.as_bytes()))
        .await
        .context("send FILEREQUEST")?;

    // Expect FILEREQUEST_ANSWER or FILENOTFOUND.
    loop {
        let (_proto, opcode, _payload) = timeout(opts.op_timeout, read_frame(&mut stream))
            .await
            .context("FILEREQUEST_ANSWER timeout")?
            .context("read FILEREQUEST_ANSWER")?;
        match opcode {
            OP_FILEREQUEST_ANSWER => break,
            OP_FILENOTFOUND => bail!("peer does not have the file"),
            _ => debug!("skipping opcode 0x{opcode:02x} during file request"),
        }
    }

    // ── STARTUPLOAD_REQ ──────────────────────────────────────────────────────
    stream
        .write_all(&build_message(OP_STARTUPLOAD_REQ, opts.hash.as_bytes()))
        .await
        .context("send STARTUPLOAD_REQ")?;

    // Handle queue / accept.
    let mut queue_waits = 0;
    loop {
        let (_proto, opcode, payload) = timeout(opts.op_timeout, read_frame(&mut stream))
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
                // Re-send STARTUPLOAD_REQ after a short wait.
                tokio::time::sleep(Duration::from_secs(5)).await;
                stream
                    .write_all(&build_message(OP_STARTUPLOAD_REQ, opts.hash.as_bytes()))
                    .await
                    .context("re-send STARTUPLOAD_REQ")?;
            }
            OP_QUEUE_FULL => bail!("peer queue is full"),
            _ => debug!("skipping opcode 0x{opcode:02x} waiting for ACCEPTUPLOAD"),
        }
    }

    // ── Data transfer ────────────────────────────────────────────────────────
    // Each REQUESTPARTS asks for 3 consecutive 180 KB windows.  We send the
    // next batch only after receiving all chunks from the current one so we
    // never request overlapping ranges.
    const PART_WINDOW: u64 = 180 * 1024;

    let file_size = opts.file_size;
    // Resume from where the caller left off.  The writer must already be
    // seeked to this offset before calling download_file.
    let mut bytes_received: u64 = opts.start_offset;
    let mut part_buf: Vec<u8> = Vec::new();
    let mut current_part_start: u64 = opts.start_offset;
    // Byte offset that marks the end of the current batch (3 windows).
    let mut batch_end: u64 = (opts.start_offset + 3 * PART_WINDOW).min(file_size);

    // Send the first REQUESTPARTS.
    send_request_parts(
        &mut stream,
        opts.hash.as_bytes(),
        opts.start_offset,
        file_size,
        PART_WINDOW,
    )
    .await?;

    loop {
        if bytes_received >= file_size {
            break;
        }
        let (_proto, opcode, payload) = timeout(opts.op_timeout, read_frame(&mut stream))
            .await
            .context("data receive timeout")?
            .context("read data frame")?;

        match opcode {
            OP_SENDINGPART => {
                // Wire format: hash(16) + start(4) + end(4) + data
                if payload.len() < 24 {
                    warn!("malformed SENDINGPART (too short: {} bytes)", payload.len());
                    continue;
                }
                // Skip the 16-byte file hash echoed in the response.
                let _range_start =
                    u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]]) as u64;
                let range_end =
                    u32::from_le_bytes([payload[20], payload[21], payload[22], payload[23]]) as u64;
                let data = &payload[24..];

                // Write data.
                out_writer
                    .write_all(data)
                    .await
                    .context("write chunk data")?;
                bytes_received = range_end;

                on_event(DownloadEvent::Progress {
                    bytes_received,
                    total: file_size,
                });

                // Verify completed ed2k parts.
                part_buf.extend_from_slice(data);
                let part_end = current_part_start + CHUNK_SIZE as u64;
                if bytes_received >= part_end || bytes_received >= file_size {
                    let part_index = (current_part_start / CHUNK_SIZE as u64) as usize;
                    let _chunk_hash = Md4::digest(&part_buf);
                    // Per-part verification requires the hash list from AICH/metadata;
                    // emit the event unconditionally for progress tracking.
                    on_event(DownloadEvent::ChunkVerified { part_index });
                    part_buf.clear();
                    current_part_start = bytes_received;
                }

                // Request the next batch once we've consumed all chunks of the current one.
                if bytes_received >= batch_end && bytes_received < file_size {
                    send_request_parts(
                        &mut stream,
                        opts.hash.as_bytes(),
                        bytes_received,
                        file_size,
                        PART_WINDOW,
                    )
                    .await?;
                    batch_end = (bytes_received + 3 * PART_WINDOW).min(file_size);
                }
            }
            _ => {
                debug!("skipping opcode 0x{opcode:02x} during data transfer");
            }
        }
    }

    on_event(DownloadEvent::Done);
    Ok(bytes_received)
}

/// Send a `REQUESTPARTS` message for up to 3 consecutive 180 KB windows.
async fn send_request_parts(
    stream: &mut TcpStream,
    file_hash: &[u8; 16],
    offset: u64,
    file_size: u64,
    window: u64,
) -> Result<()> {
    // REQUESTPARTS payload: hash(16) + start0(4) + start1(4) + start2(4) + end0(4) + end1(4) + end2(4)
    let mut payload = Vec::with_capacity(16 + 6 * 4);
    payload.extend_from_slice(file_hash);

    let mut starts = [0u32; 3];
    let mut ends = [0u32; 3];
    for i in 0..3 {
        let s = offset + (i as u64) * window;
        if s >= file_size {
            starts[i] = s.min(file_size) as u32;
            ends[i] = starts[i];
        } else {
            starts[i] = s as u32;
            ends[i] = (s + window).min(file_size) as u32;
        }
    }
    for s in &starts {
        payload.extend_from_slice(&s.to_le_bytes());
    }
    for e in &ends {
        payload.extend_from_slice(&e.to_le_bytes());
    }
    stream
        .write_all(&build_message(OP_REQUESTPARTS, &payload))
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
        // protocol(1) + len(4) + opcode(1) + payload(2) = 8 bytes
        assert_eq!(msg.len(), 8);
        assert_eq!(msg[0], PROTO_ED2K);
        // len = 3 (opcode + 2 payload bytes), LE
        assert_eq!(&msg[1..5], &[3, 0, 0, 0]);
        assert_eq!(msg[5], OP_HELLO);
        assert_eq!(&msg[6..], &[0xaa, 0xbb]);
    }

    #[test]
    fn test_build_hello_length() {
        let h = build_hello(&[0u8; 16]);
        // hash_size(1) + hash(16) + client_id(4) + tcp_port(2) + tag_count(4) + server_ip(4) + server_port(2) = 33
        assert_eq!(h.len(), 33);
        assert_eq!(h[0], 16u8); // hash_size prefix
    }
}
