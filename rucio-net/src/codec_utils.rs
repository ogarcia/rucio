//! Shared length-prefixed postcard framing used by both the transfer and
//! manifest codecs.
//!
//! Frame format: [ 4 bytes LE length ][ postcard payload ]

use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_FRAME: usize = 8 * 1024 * 1024; // 8 MiB

/// Optional per-transfer byte counter, incremented slice-by-slice as a chunk is
/// paced/read. The daemon points it at a specific `(peer, file)` row so its
/// transfer rate reads as a flat stream rather than spiking once per 4 MiB
/// chunk. Distinct from the global limiter/progress hooks (which carry no peer).
pub type ByteSink = Option<Arc<AtomicU64>>;

/// An async byte-rate limiter: called with a byte count, its future resolves
/// once that many bytes' worth of upload allowance is available. Lets the
/// embedding application's bandwidth bucket pace the *actual wire writes* (a
/// smooth stream) instead of gating whole-chunk handoffs (which dumps a whole
/// 4 MiB chunk at link speed, then idles — bursty and slower than the cap).
pub type ByteLimiter = Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A synchronous progress hook: called with the number of bytes just read off
/// the wire for a response, so the embedding application can account download
/// speed incrementally (a flat reading) instead of in one spike when the whole
/// chunk arrives. Cheap and non-blocking (just a metric update).
pub type ReadProgress = Arc<dyn Fn(u64) + Send + Sync>;

/// Largest slice written between rate-limiter checks. Small enough to keep the
/// wire smooth at low limits, large enough to avoid excessive await overhead.
const PACING_PIECE: usize = 64 * 1024;

pub async fn read_framed<T, D>(io: &mut T) -> io::Result<D>
where
    T: AsyncRead + Unpin + Send,
    D: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;

    postcard::from_bytes::<D>(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Like [`read_framed`], but when `progress` is `Some` the payload is read in
/// [`PACING_PIECE`] slices and `progress` is called with each slice's size as
/// it arrives. Lets the caller account download speed incrementally so it reads
/// as a flat stream rather than spiking when the whole chunk completes.
pub async fn read_framed_progress<T, D>(
    io: &mut T,
    progress: Option<&ReadProgress>,
    peer_sink: &ByteSink,
) -> io::Result<D>
where
    T: AsyncRead + Unpin + Send,
    D: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    if progress.is_none() && peer_sink.is_none() {
        io.read_exact(&mut buf).await?;
    } else {
        let mut off = 0;
        while off < len {
            let end = (off + PACING_PIECE).min(len);
            io.read_exact(&mut buf[off..end]).await?;
            let n = (end - off) as u64;
            if let Some(report) = progress {
                report(n); // global download speed
            }
            if let Some(sink) = peer_sink {
                sink.fetch_add(n, Ordering::Relaxed); // this peer's rate
            }
            off = end;
        }
    }

    postcard::from_bytes::<D>(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

pub async fn write_framed<T, S>(io: &mut T, value: &S) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
    S: serde::Serialize,
{
    let encoded = postcard::to_allocvec(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    if encoded.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large to send: {} bytes", encoded.len()),
        ));
    }

    io.write_all(&(encoded.len() as u32).to_le_bytes()).await?;
    io.write_all(&encoded).await?;
    Ok(())
}

/// Like [`write_framed`], but when `limiter` is `Some` the payload is written in
/// [`PACING_PIECE`] slices, waiting for each slice's upload allowance first.
/// This paces the wire write at the limiter's rate so a large response (a 4 MiB
/// chunk) streams out smoothly instead of being dumped at link speed.
pub async fn write_framed_paced<T, S>(
    io: &mut T,
    value: &S,
    limiter: Option<&ByteLimiter>,
    peer_sink: &ByteSink,
) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
    S: serde::Serialize,
{
    let encoded = postcard::to_allocvec(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    if encoded.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large to send: {} bytes", encoded.len()),
        ));
    }

    io.write_all(&(encoded.len() as u32).to_le_bytes()).await?;
    if limiter.is_none() && peer_sink.is_none() {
        io.write_all(&encoded).await?;
    } else {
        for piece in encoded.chunks(PACING_PIECE) {
            let n = piece.len() as u64;
            // Wait for this slice's upload allowance (paces + global speed).
            if let Some(limit) = limiter {
                limit(n).await;
            }
            io.write_all(piece).await?;
            // Flush so it leaves the host paced, not buffered into one burst.
            io.flush().await?;
            // Account this peer's bytes as they go out (flat per-peer rate).
            if let Some(sink) = peer_sink {
                sink.fetch_add(n, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}
