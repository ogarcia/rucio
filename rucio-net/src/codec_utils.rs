//! Shared length-prefixed postcard framing used by both the transfer and
//! manifest codecs.
//!
//! Frame format: [ 4 bytes LE length ][ postcard payload ]

use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io;

pub const MAX_FRAME: usize = 8 * 1024 * 1024; // 8 MiB

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
