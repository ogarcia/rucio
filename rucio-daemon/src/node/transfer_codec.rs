//! Codec for the `/rucio/transfer/1.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};
use std::io;

use super::codec_utils::{read_framed, write_framed};

#[derive(Debug, Clone)]
pub struct TransferProtocol;

impl AsRef<str> for TransferProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/transfer/2.0.0"
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
