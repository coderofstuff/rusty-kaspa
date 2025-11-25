use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use log::info;
use log::warn;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, thiserror::Error)]
pub enum Libp2pError {
    #[error("libp2p connector not implemented yet")]
    NotImplemented,
    #[error("libp2p not enabled")]
    Disabled,
    #[error("libp2p dial failed: {0}")]
    DialFailed(String),
    #[error("libp2p listen failed: {0}")]
    ListenFailed(String),
}

/// Placeholder libp2p transport connector. Will be expanded with real libp2p dial/listen logic.
#[derive(Clone)]
pub struct Libp2pConnector {
    pub config: Config,
}

impl Default for Libp2pConnector {
    fn default() -> Self {
        Self { config: Config::default() }
    }
}

impl Libp2pConnector {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl TransportConnector for Libp2pConnector {
    type Error = Libp2pError;
    type Future<'a> = BoxFuture<'a, Result<(Arc<Router>, TransportMetadata, PeerKey), Self::Error>>;

    fn connect<'a>(&'a self, _address: NetAddress) -> Self::Future<'a> {
        let metadata = TransportMetadata::default();
        if !self.config.mode.is_enabled() {
            return Box::pin(async move { Err(Libp2pError::Disabled) });
        }

        // TODO: integrate real libp2p dial and wrap in Router, returning metadata and PeerKey.
        Box::pin(async move {
            info!("libp2p connector stub invoked; dial not implemented");
            let _ = metadata;
            Err(Libp2pError::NotImplemented)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::str::FromStr;

    #[test]
    fn libp2p_connect_disabled() {
        let connector = Libp2pConnector::default();
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }

    #[test]
    fn libp2p_connect_enabled_stubbed() {
        let mut cfg = Config::default();
        cfg.mode = crate::Mode::Full;
        let connector = Libp2pConnector::new(cfg);
        let addr = kaspa_utils::networking::NetAddress::from_str("127.0.0.1:16110").unwrap();
        let res = block_on(connector.connect(addr));
        assert!(matches!(res, Err(Libp2pError::NotImplemented)));
    }
}

/// Outbound connector that prefers libp2p when enabled, otherwise falls back to TCP.
pub struct Libp2pOutboundConnector {
    config: Config,
    fallback: Arc<dyn OutboundConnector>,
    provider: Option<Arc<dyn Libp2pStreamProvider>>,
}

impl Libp2pOutboundConnector {
    pub fn new(config: Config, fallback: Arc<dyn OutboundConnector>) -> Self {
        Self { config, fallback, provider: None }
    }

    pub fn with_provider(config: Config, fallback: Arc<dyn OutboundConnector>, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self { config, fallback, provider: Some(provider) }
    }
}

impl OutboundConnector for Libp2pOutboundConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: CoreTransportMetadata,
        handler: &'a kaspa_p2p_lib::ConnectionHandler,
    ) -> BoxFuture<'a, Result<Arc<Router>, ConnectionError>> {
        if !self.config.mode.is_enabled() {
            let mut metadata = metadata;
            metadata.capabilities.libp2p = false;
            return self.fallback.connect(address, metadata, handler);
        }

        static WARN_ONCE: OnceLock<()> = OnceLock::new();
        WARN_ONCE.get_or_init(|| warn!("libp2p mode enabled but libp2p connector is not implemented; falling back to TCP"));

        if let Some(provider) = &self.provider {
            let address = match NetAddress::from_str(&address) {
                Ok(addr) => addr,
                Err(_) => {
                    return Box::pin(async move {
                        Err(ConnectionError::ProtocolError(ProtocolError::Other("invalid libp2p address provided")))
                    })
                }
            };

            let provider = provider.clone();
            let handler = handler.clone();
            return Box::pin(async move {
                let (mut md, stream) = provider
                    .dial(address)
                    .await
                    .map_err(|_| ConnectionError::ProtocolError(ProtocolError::Other("libp2p dial failed")))?;

                md.capabilities.libp2p = true;
                md.path = kaspa_p2p_lib::PathKind::Unknown;
                handler.connect_with_stream(stream, md).await
            });
        }

        Box::pin(async move { Err(ConnectionError::ProtocolError(ProtocolError::Other("libp2p outbound connector unimplemented"))) })
    }
}

/// Bound for streams accepted/dialed via libp2p.
pub trait Libp2pStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Libp2pStream for T {}

pub type BoxedLibp2pStream = Box<dyn Libp2pStream>;

/// A provider for libp2p streams (dialed or accepted). The real implementation
/// will bridge to the libp2p swarm and return a stream plus transport metadata.
pub trait Libp2pStreamProvider: Send + Sync {
    fn dial<'a>(&'a self, address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>>;
    fn listen<'a>(&'a self) -> BoxFuture<'a, Result<(TransportMetadata, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>>;
}

/// Placeholder libp2p stream provider. Returns Disabled/NotImplemented until
/// the real libp2p bridge is wired in.
#[derive(Clone)]
pub struct PlaceholderStreamProvider {
    config: Config,
}

impl PlaceholderStreamProvider {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Libp2pStreamProvider for PlaceholderStreamProvider {
    fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }
            Err(Libp2pError::NotImplemented)
        })
    }

    fn listen<'a>(&'a self) -> BoxFuture<'a, Result<(TransportMetadata, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }
            Err(Libp2pError::NotImplemented)
        })
    }
}
