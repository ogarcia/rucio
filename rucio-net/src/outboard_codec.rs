//! Codec for the `/rucio/outboard/1.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::outboard::{OutboardRequest, OutboardResponse};
use std::io;

use super::codec_utils::{read_framed, write_framed};

#[derive(Debug, Clone)]
pub struct OutboardProtocol;

impl AsRef<str> for OutboardProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/outboard/1.0.0"
    }
}

#[derive(Debug, Clone, Default)]
pub struct OutboardCodec;

#[async_trait]
impl request_response::Codec for OutboardCodec {
    type Protocol = OutboardProtocol;
    type Request = OutboardRequest;
    type Response = OutboardResponse;

    async fn read_request<T>(
        &mut self,
        _: &OutboardProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &OutboardProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &OutboardProtocol,
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
        _: &OutboardProtocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &resp).await
    }
}
