//! Shared length-prefixed postcard framing used by both the transfer and
//! manifest codecs.
//!
//! Frame format: [ 4 bytes LE length ][ postcard payload ]

use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

pub const MAX_FRAME: usize = 8 * 1024 * 1024; // 8 MiB

/// An async byte-rate limiter: called with a byte count, its future resolves
/// once that many bytes' worth of upload allowance is available. Lets the
/// embedding application's bandwidth bucket pace the *actual wire writes* (a
/// smooth stream) instead of gating whole-chunk handoffs (which dumps a whole
/// 4 MiB chunk at link speed, then idles — bursty and slower than the cap).
pub type ByteLimiter = Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

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
    match limiter {
        None => io.write_all(&encoded).await?,
        Some(limit) => {
            for piece in encoded.chunks(PACING_PIECE) {
                // Wait for this slice's worth of upload allowance, then write it
                // and flush so it actually leaves the host paced — otherwise the
                // slices could buffer and burst out together at the final flush,
                // defeating the smoothing.
                limit(piece.len() as u64).await;
                io.write_all(piece).await?;
                io.flush().await?;
            }
        }
    }
    Ok(())
}
