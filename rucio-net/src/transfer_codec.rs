//! Codec for the `/rucio/transfer/1.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};
use std::io;

use super::codec_utils::{ByteLimiter, read_framed, write_framed, write_framed_paced};

#[derive(Debug, Clone)]
pub struct TransferProtocol;

impl AsRef<str> for TransferProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/transfer/2.0.0"
    }
}

/// Chunk transfer codec. Holds an optional [`ByteLimiter`] used to pace the
/// *write* of chunk responses we serve, so the upload limit produces a smooth
/// stream rather than per-chunk bursts. `None` = no pacing (e.g. a node that
/// doesn't serve, or no limit configured).
#[derive(Clone, Default)]
pub struct TransferCodec {
    upload_limiter: Option<ByteLimiter>,
}

impl TransferCodec {
    pub fn new(upload_limiter: Option<ByteLimiter>) -> Self {
        Self { upload_limiter }
    }
}

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
        // Pace the chunk write at the upload limit so it streams out smoothly.
        write_framed_paced(io, &resp, self.upload_limiter.as_ref()).await
    }
}
