use crate::core::peer::PeerKey;
use crate::Router;
use kaspa_utils::networking::{IpAddress, NetAddress, PeerId};
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

/// How a connection reached us; used for accounting and relay budgeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathKind {
    Direct,
    Relay { relay_id: Option<String> },
    Unknown,
}

impl Default for PathKind {
    fn default() -> Self {
        PathKind::Unknown
    }
}

/// Capabilities advertised by the remote transport.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Capabilities {
    pub libp2p: bool,
}

/// Metadata attached to a transport connection. This is intentionally
/// transport-agnostic and filled in by the connector (TCP/libp2p/etc).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportMetadata {
    /// Optional libp2p peer ID (used to synthesize a stable accounting address).
    pub libp2p_peer_id: Option<String>,
    /// Optional kaspad peer ID if known pre-handshake.
    pub peer_id: Option<PeerId>,
    pub reported_ip: Option<IpAddress>,
    pub path: PathKind,
    pub capabilities: Capabilities,
}

impl TransportMetadata {
    /// Produce a synthetic, stable socket address derived from the libp2p peer ID.
    ///
    /// The address is not meant to be reachable on the network; it is only used
    /// for identity/accounting (e.g., duplicate detection or bucket assignment)
    /// when no real socket address is available. The generated address is
    /// relay-agnostic and depends solely on the peer identity.
    pub fn synthetic_socket_addr(&self) -> Option<SocketAddr> {
        let peer_id = self.libp2p_peer_id.as_ref()?;

        let mut hasher = DefaultHasher::new();
        peer_id.hash(&mut hasher);
        let hash = hasher.finish();

        // Use a unique-local IPv6 prefix to avoid clashing with real addresses.
        let mut octets = [0u8; 16];
        octets[0] = 0xfd;
        octets[1] = 0xff;
        octets[8..16].copy_from_slice(&hash.to_be_bytes());

        // Keep the port in the high range to minimise overlap with well-known ports.
        let port = (((hash >> 16) as u16) | 0x8000).max(1025);

        Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_socket_addr_is_stable_and_relay_agnostic() {
        let base = TransportMetadata {
            libp2p_peer_id: Some("12D3KooWAJ7gBypRelayAgnostic".to_string()),
            path: PathKind::Direct,
            ..Default::default()
        };
        let with_relay = TransportMetadata { path: PathKind::Relay { relay_id: Some("relay-1".into()) }, ..base.clone() };

        let addr1 = base.synthetic_socket_addr().expect("peer id should produce address");
        let addr2 = with_relay.synthetic_socket_addr().expect("peer id should produce address");

        assert_eq!(addr1, addr2, "relay metadata must not affect synthetic address");
    }

    #[test]
    fn synthetic_socket_addr_differs_by_peer() {
        let a = TransportMetadata { libp2p_peer_id: Some("peer-a".to_string()), ..Default::default() };
        let b = TransportMetadata { libp2p_peer_id: Some("peer-b".to_string()), ..Default::default() };

        let addr_a = a.synthetic_socket_addr().unwrap();
        let addr_b = b.synthetic_socket_addr().unwrap();

        assert_ne!(addr_a, addr_b);
        assert!(addr_a.ip().is_ipv6());
        assert!(addr_b.ip().is_ipv6());
    }

    #[test]
    fn synthetic_socket_addr_absent_without_peer_id() {
        let metadata = TransportMetadata::default();
        assert!(metadata.synthetic_socket_addr().is_none());
    }
}

/// Abstract transport connector used by the P2P connection handler.
///
/// This trait allows bridging different underlying transports (e.g., TCP, libp2p)
/// into the Router without coupling the handler to a specific transport.
pub trait TransportConnector: Send + Sync {
    type Error: Send + 'static;
    type Future<'a>: Future<Output = Result<(Arc<Router>, TransportMetadata, PeerKey), Self::Error>> + Send
    where
        Self: 'a;

    fn connect<'a>(&'a self, address: NetAddress) -> Self::Future<'a>;
}
