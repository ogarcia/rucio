//! Codec for the `/rucio/pinset/1.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::pinset::{PinsetRequest, PinsetResponse};
use std::io;

use super::codec_utils::{read_framed, write_framed};

#[derive(Debug, Clone)]
pub struct PinsetProtocol;

impl AsRef<str> for PinsetProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/pinset/1.0.0"
    }
}

#[derive(Debug, Clone, Default)]
pub struct PinsetCodec;

#[async_trait]
impl request_response::Codec for PinsetCodec {
    type Protocol = PinsetProtocol;
    type Request = PinsetRequest;
    type Response = PinsetResponse;

    async fn read_request<T>(&mut self, _: &PinsetProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &PinsetProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &PinsetProtocol,
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
        _: &PinsetProtocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &resp).await
    }
}
