use crate::transport::{Libp2pError, Libp2pIdentity};
use libp2p::core::upgrade;
use libp2p::noise;
use libp2p::ping;
use libp2p::swarm::NetworkBehaviour;
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{identity, Swarm, Transport};
use log::info;

/// Minimal libp2p behaviour (Ping only) to validate swarm construction.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "PingEvent")]
pub struct BaseBehaviour {
    pub ping: ping::Behaviour,
}

#[allow(clippy::large_enum_variant)]
pub enum PingEvent {
    Ping(ping::Event),
}

impl From<ping::Event> for PingEvent {
    fn from(event: ping::Event) -> Self {
        PingEvent::Ping(event)
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

    info!("libp2p base swarm peer id: {}", peer_id);

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Ok(Swarm::new(transport, BaseBehaviour { ping: ping::Behaviour::default() }, peer_id, cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_swarm_succeeds() {
        let id = Libp2pIdentity::from_config(&crate::config::Config::default()).expect("identity");
        let swarm = build_base_swarm(&id).expect("swarm");
        assert_eq!(swarm.local_peer_id(), &id.peer_id);
    }
}
