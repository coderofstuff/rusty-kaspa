use tokio::io::{AsyncRead, AsyncWrite};

use kaspa_p2p_lib::MetadataConnectInfo;

use crate::transport::BoxedLibp2pStream;

pub(super) struct MetaConnectedStream {
    stream: BoxedLibp2pStream,
    info: MetadataConnectInfo,
    close: Option<Box<dyn FnOnce() + Send>>,
}

impl MetaConnectedStream {
    pub(super) fn new(stream: BoxedLibp2pStream, info: MetadataConnectInfo, close: Option<Box<dyn FnOnce() + Send>>) -> Self {
        Self { stream, info, close }
    }
}

impl Drop for MetaConnectedStream {
    fn drop(&mut self) {
        if let Some(close) = self.close.take() {
            close();
        }
    }
}

impl tonic::transport::server::Connected for MetaConnectedStream {
    type ConnectInfo = MetadataConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for MetaConnectedStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for MetaConnectedStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
