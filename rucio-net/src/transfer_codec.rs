//! Codec for the `/rucio/transfer/2.0.0` request-response protocol.

use async_trait::async_trait;
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::request_response;
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};
use std::io;

use super::codec_utils::{
    ByteLimiter, ByteSink, ReadProgress, read_framed, read_framed_progress, write_framed,
    write_framed_paced,
};

#[derive(Debug, Clone)]
pub struct TransferProtocol;

impl AsRef<str> for TransferProtocol {
    fn as_ref(&self) -> &str {
        "/rucio/transfer/2.0.0"
    }
}

/// A chunk request paired with an optional per-`(peer, file)` byte sink. Only
/// the [`ChunkRequest`] is serialized; the sink is a local accounting handle the
/// *downloader* attaches so the codec can count bytes as the response is read
/// (a flat per-peer download rate). The request_response handler clones the
/// codec per request, so `write_request` can stash the sink for the paired
/// `read_response` on the same clone. On the serving side the request arrives
/// with `None`.
pub type ChunkReq = (ChunkRequest, ByteSink);

/// A chunk response paired with an optional per-`(peer, file)` byte sink. Only
/// the [`ChunkResponse`] is serialized; the *uploader* attaches the sink so the
/// codec counts bytes as the chunk is paced onto the wire (a flat per-peer
/// upload rate). On the receiving side the response is produced with `None`.
pub type ChunkResp = (ChunkResponse, ByteSink);

/// Chunk transfer codec. The optional hooks make the upload limit and the
/// transfer-speed accounting work at the byte level (smooth stream / flat
/// rates) rather than per whole 4 MiB chunk:
/// - `upload_limiter` paces the *write* of chunk responses and feeds the global
///   upload speed.
/// - `download_progress` feeds the global download speed as a response is read.
/// - the per-`(peer, file)` sink (carried in the request/response, stashed for
///   the download read) drives the per-peer rate shown in the UI.
///
/// `None` hooks = no accounting (e.g. a node that doesn't transfer, or no limit).
#[derive(Clone, Default)]
pub struct TransferCodec {
    upload_limiter: Option<ByteLimiter>,
    download_progress: Option<ReadProgress>,
    /// Set by `write_request` from the outbound request and consumed by the
    /// paired `read_response` (same per-request codec clone) to count this
    /// download's bytes against its peer row.
    stashed_download_sink: ByteSink,
}

impl TransferCodec {
    pub fn new(
        upload_limiter: Option<ByteLimiter>,
        download_progress: Option<ReadProgress>,
    ) -> Self {
        Self {
            upload_limiter,
            download_progress,
            stashed_download_sink: None,
        }
    }
}

#[async_trait]
impl request_response::Codec for TransferCodec {
    type Protocol = TransferProtocol;
    type Request = ChunkReq;
    type Response = ChunkResp;

    async fn read_request<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Serving side: no per-peer sink here (the uploader attaches it to the
        // response it sends back).
        Ok((read_framed(io).await?, None))
    }

    async fn read_response<T>(
        &mut self,
        _: &TransferProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Count bytes as they arrive: globally (download speed) and against this
        // download's peer row (stashed by the paired write_request).
        let resp = read_framed_progress(
            io,
            self.download_progress.as_ref(),
            &self.stashed_download_sink,
        )
        .await?;
        Ok((resp, None))
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
        // Stash the per-peer download sink for the paired read_response (same
        // per-request codec clone); only the request itself goes on the wire.
        self.stashed_download_sink = req.1;
        write_framed(io, &req.0).await
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
        // Pace the write at the upload limit (smooth stream) and count bytes
        // against this peer's upload row as they go out.
        write_framed_paced(io, &resp.0, self.upload_limiter.as_ref(), &resp.1).await
    }
}
