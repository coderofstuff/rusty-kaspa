use crate::{config::Config, metadata::TransportMetadata};
use futures_util::future::BoxFuture;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::TransportMetadata as CoreTransportMetadata;
use kaspa_p2p_lib::{ConnectionError, OutboundConnector, PeerKey, Router, TransportConnector};
use kaspa_utils::networking::NetAddress;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Multiaddr;
use libp2p::{identity, PeerId};
use log::info;
use log::warn;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::{fs, io, path::Path};
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
    #[error("libp2p identity error: {0}")]
    Identity(String),
    #[error("invalid multiaddr: {0}")]
    Multiaddr(String),
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
    use tempfile::tempdir;

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

    #[test]
    fn to_multiaddr_ipv4_and_ipv6() {
        let ipv4 = NetAddress::from_str("192.0.2.1:1234").unwrap();
        let m4 = to_multiaddr(ipv4).unwrap();
        assert_eq!(m4.to_string(), "/ip4/192.0.2.1/tcp/1234");

        let ipv6 = NetAddress::from_str("[2001:db8::1]:5678").unwrap();
        let m6 = to_multiaddr(ipv6).unwrap();
        assert_eq!(m6.to_string(), "/ip6/2001:db8::1/tcp/5678");
    }

    #[test]
    fn identity_ephemeral_and_persisted() {
        let cfg = Config::default();
        let id = Libp2pIdentity::from_config(&cfg).expect("ephemeral identity");
        assert!(id.persisted_path.is_none());
        assert!(!id.peer_id.to_string().is_empty());

        let dir = tempdir().unwrap();
        let key_path = dir.path().join("id.key");
        let mut cfg = Config::default();
        cfg.identity = crate::Identity::Persisted(key_path.clone());
        let id1 = Libp2pIdentity::from_config(&cfg).expect("persisted identity");
        let id2 = Libp2pIdentity::from_config(&cfg).expect("persisted identity reload");
        assert_eq!(id1.peer_id, id2.peer_id);
        assert_eq!(id1.persisted_path.as_deref(), Some(key_path.as_path()));
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
    peer_id: PeerId,
}

impl PlaceholderStreamProvider {
    pub fn new(config: Config, peer_id: PeerId) -> Self {
        Self { config, peer_id }
    }
}

impl Libp2pStreamProvider for PlaceholderStreamProvider {
    fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }
            let mut md = TransportMetadata::default();
            md.capabilities.libp2p = true;
            md.libp2p_peer_id = Some(self.peer_id.to_string());
            Err(Libp2pError::NotImplemented)
        })
    }

    fn listen<'a>(&'a self) -> BoxFuture<'a, Result<(TransportMetadata, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }
            let mut md = TransportMetadata::default();
            md.capabilities.libp2p = true;
            md.libp2p_peer_id = Some(self.peer_id.to_string());
            Err(Libp2pError::NotImplemented)
        })
    }
}

/// Libp2p identity wrapper (ed25519).
#[derive(Clone)]
pub struct Libp2pIdentity {
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub persisted_path: Option<std::path::PathBuf>,
}

impl Libp2pIdentity {
    pub fn from_config(config: &Config) -> Result<Self, Libp2pError> {
        match &config.identity {
            crate::Identity::Ephemeral => {
                let keypair = identity::Keypair::generate_ed25519();
                let peer_id = PeerId::from(keypair.public());
                Ok(Self { keypair, peer_id, persisted_path: None })
            }
            crate::Identity::Persisted(path) => {
                let keypair = load_or_generate_key(path).map_err(|e| Libp2pError::Identity(e.to_string()))?;
                let peer_id = PeerId::from(keypair.public());
                Ok(Self { keypair, peer_id, persisted_path: Some(path.clone()) })
            }
        }
    }

    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }
}

fn load_or_generate_key(path: &Path) -> io::Result<Keypair> {
    if let Ok(bytes) = fs::read(path) {
        return Keypair::from_protobuf_encoding(&bytes).map_err(map_identity_err);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let keypair = identity::Keypair::generate_ed25519();
    let bytes = keypair.to_protobuf_encoding().map_err(map_identity_err)?;
    fs::write(path, bytes)?;
    Ok(keypair)
}

fn map_identity_err(err: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}

/// Translate a NetAddress (ip:port) into a libp2p multiaddr.
pub fn to_multiaddr(address: NetAddress) -> Result<Multiaddr, Libp2pError> {
    let multiaddr: Multiaddr = match address.ip {
        kaspa_utils::networking::IpAddress(std::net::IpAddr::V4(v4)) => {
            format!("/ip4/{}/tcp/{}", v4, address.port).parse::<Multiaddr>()
        }
        kaspa_utils::networking::IpAddress(std::net::IpAddr::V6(v6)) => {
            format!("/ip6/{}/tcp/{}", v6, address.port).parse::<Multiaddr>()
        }
    }
    .map_err(|e: libp2p::multiaddr::Error| Libp2pError::Multiaddr(e.to_string()))?;
    Ok(multiaddr)
}

/// Skeleton libp2p stream provider that will eventually wrap a libp2p swarm.
/// Currently returns `NotImplemented` for both dial and listen.
pub struct SwarmStreamProvider {
    pub identity: Libp2pIdentity,
}

impl SwarmStreamProvider {
    pub fn new(identity: Libp2pIdentity) -> Self {
        Self { identity }
    }
}

impl Libp2pStreamProvider for SwarmStreamProvider {
    fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let peer_id = self.identity.peer_id.to_string();
        Box::pin(async move {
            let mut md = TransportMetadata::default();
            md.capabilities.libp2p = true;
            md.libp2p_peer_id = Some(peer_id);
            Err(Libp2pError::NotImplemented)
        })
    }

    fn listen<'a>(&'a self) -> BoxFuture<'a, Result<(TransportMetadata, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let peer_id = self.identity.peer_id.to_string();
        Box::pin(async move {
            let mut md = TransportMetadata::default();
            md.capabilities.libp2p = true;
            md.libp2p_peer_id = Some(peer_id);
            Err(Libp2pError::NotImplemented)
        })
    }
}
