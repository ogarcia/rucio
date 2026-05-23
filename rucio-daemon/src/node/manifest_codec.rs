//! Codec for the `/rucio/manifest/1.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::manifest::{ManifestRequest, ManifestResponse};
use std::io;

use super::codec_utils::{read_framed, write_framed};

#[derive(Debug, Clone)]
pub struct ManifestProtocol;

impl AsRef<str> for ManifestProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/manifest/1.0.0"
    }
}

#[derive(Debug, Clone, Default)]
pub struct ManifestCodec;

#[async_trait]
impl request_response::Codec for ManifestCodec {
    type Protocol = ManifestProtocol;
    type Request = ManifestRequest;
    type Response = ManifestResponse;

    async fn read_request<T>(
        &mut self,
        _: &ManifestProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &ManifestProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &ManifestProtocol,
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
        _: &ManifestProtocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &resp).await
    }
}
