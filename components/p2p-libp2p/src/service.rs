use crate::swarm::build_base_swarm;
use crate::transport::{BoxedLibp2pStream, Libp2pIdentity, Libp2pStreamProvider, SwarmStreamProvider};
use crate::{config::Config, transport::Libp2pError};
use kaspa_p2p_lib::{ConnectionHandler, MetadataConnectInfo};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::once;

/// Placeholder libp2p service that will eventually own dial/listen/reservation logic.
#[derive(Clone)]
pub struct Libp2pService {
    config: Config,
    provider: Option<std::sync::Arc<dyn Libp2pStreamProvider>>,
}

impl Libp2pService {
    pub fn new(config: Config) -> Self {
        Self { config, provider: None }
    }

    pub fn with_provider(config: Config, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self { config, provider: Some(provider) }
    }

    /// Start the libp2p service. Currently returns `NotImplemented` when enabled.
    pub async fn start(&self) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let _provider = self.provider_or_build()?;

        // In a full implementation, this would spawn dial/listen/reservation loops.
        Err(Libp2pError::NotImplemented)
    }

    /// Start an inbound listener using the provided stream provider and connection handler.
    /// This bridges libp2p streams into the tonic server via `serve_with_incoming`.
    pub async fn start_inbound(&self, handler: std::sync::Arc<ConnectionHandler>) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let provider = self.provider_or_build()?;
        let (metadata, _close, stream) = provider.listen().await?;
        let info = MetadataConnectInfo::new(None, metadata);

        let connected = MetaConnectedStream::new(stream, info);
        handler.serve_with_incoming(once(Result::<_, std::io::Error>::Ok(connected)));

        Ok(())
    }

    fn provider_or_build(&self) -> Result<Arc<dyn Libp2pStreamProvider>, Libp2pError> {
        if let Some(p) = &self.provider {
            return Ok(p.clone());
        }

        let identity = Libp2pIdentity::from_config(&self.config).map_err(|e| Libp2pError::Identity(e.to_string()))?;
        let swarm = build_base_swarm(&identity)?;
        let provider: Arc<dyn Libp2pStreamProvider> = Arc::new(SwarmStreamProvider::new(identity, Arc::new(Mutex::new(swarm))));
        Ok(provider)
    }
}

struct MetaConnectedStream {
    stream: BoxedLibp2pStream,
    info: MetadataConnectInfo,
}

impl MetaConnectedStream {
    fn new(stream: BoxedLibp2pStream, info: MetadataConnectInfo) -> Self {
        Self { stream, info }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_disabled_returns_disabled() {
        let svc = Libp2pService::new(Config::default());
        let res = svc.start().await;
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }
}
