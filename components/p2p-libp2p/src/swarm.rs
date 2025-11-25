use crate::transport::Libp2pError;
use crate::transport::Libp2pIdentity;
use libp2p::core::upgrade;
use libp2p::noise;
use libp2p::ping;
use libp2p::swarm::NetworkBehaviour;
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{identity, Multiaddr, Swarm, Transport};
use log::info;

/// Minimal libp2p behaviour used as a base transport placeholder.
#[derive(NetworkBehaviour)]
pub struct BaseBehaviour {
    pub ping: ping::Behaviour,
}

impl Default for BaseBehaviour {
    fn default() -> Self {
        Self { ping: ping::Behaviour::default() }
    }
}

/// Build a baseline libp2p swarm (TCP + Noise + Yamux + Ping).
pub fn build_base_swarm(identity: &Libp2pIdentity) -> Result<Swarm<BaseBehaviour>, Libp2pError> {
    let local_key: identity::Keypair = identity.keypair.clone();
    let peer_id = identity.peer_id;

    let noise_keys = noise::Config::new(&local_key).map_err(|e| Libp2pError::Identity(e.to_string()))?;

    let transport = TcpTransport::new(libp2p::tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise_keys)
        .multiplex(yamux::Config::default())
        .boxed();

    let behaviour = BaseBehaviour::default();

    info!("libp2p base swarm peer id: {}", peer_id);

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Ok(Swarm::new(transport, behaviour, peer_id, cfg))
}

/// Convert a multiaddr to string for logging.
pub fn fmt_multiaddr(addr: &Multiaddr) -> String {
    addr.to_string()
}
