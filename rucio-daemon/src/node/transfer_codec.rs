//! Codec for the `/rucio/transfer/1.0.0` request-response protocol.
//!
//! Frame format: [ 4 bytes LE length ][ postcard payload ]
//!
//! Max frame size capped at 8 MiB (a chunk is at most 4 MiB of data;
//! the response envelope adds a small overhead).

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};
use std::io;

const MAX_FRAME: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct TransferProtocol;

impl AsRef<str> for TransferProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/transfer/1.0.0"
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransferCodec;

#[async_trait]
impl request_response::Codec for TransferCodec {
    type Protocol = TransferProtocol;
    type Request = ChunkRequest;
    type Response = ChunkResponse;

    async fn read_request<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &resp).await
    }
}

async fn read_framed<T, D>(io: &mut T) -> io::Result<D>
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

async fn write_framed<T, S>(io: &mut T, value: &S) -> io::Result<()>
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
