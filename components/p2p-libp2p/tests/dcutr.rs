use futures::StreamExt;
use kaspa_p2p_libp2p::Libp2pIdentity;
use kaspa_p2p_libp2p::config::{ConfigBuilder, Mode};
use libp2p::core::transport::choice::OrTransport;
use libp2p::core::upgrade;
use libp2p::dcutr;
use libp2p::identify;
use libp2p::noise;
use libp2p::ping;
use libp2p::relay::{self, client as relay_client};
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{Transport, identity};
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;

#[derive(NetworkBehaviour)]
struct ClientBehaviour {
    relay_client: relay_client::Behaviour,
    identify: identify::Behaviour,
    dcutr: dcutr::Behaviour,
    ping: ping::Behaviour,
}

fn build_client_behaviour(id: &Libp2pIdentity, relay_client_behaviour: relay_client::Behaviour) -> ClientBehaviour {
    let peer_id = id.peer_id;
    ClientBehaviour {
        relay_client: relay_client_behaviour,
        identify: identify::Behaviour::new(identify::Config::new(
            format!("/kaspad/libp2p/{}", env!("CARGO_PKG_VERSION")),
            id.keypair.public(),
        )),
        dcutr: dcutr::Behaviour::new(peer_id),
        ping: ping::Behaviour::default(),
    }
}

#[derive(NetworkBehaviour)]
struct RelayBehaviour {
    relay_server: relay::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

fn build_client_swarm(id: &Libp2pIdentity) -> Swarm<ClientBehaviour> {
    let local_key: identity::Keypair = id.keypair.clone();
    let noise_keys = noise::Config::new(&local_key).expect("noise");
    let tcp = TcpTransport::new(libp2p::tcp::Config::default().nodelay(true));
    let (relay_transport, relay_client_behaviour) = relay_client::new(id.peer_id);
    let transport = OrTransport::new(tcp, relay_transport)
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise_keys)
        .multiplex(yamux::Config::default())
        .boxed();

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Swarm::new(transport, build_client_behaviour(id, relay_client_behaviour), id.peer_id, cfg)
}

fn build_relay_swarm(id: &Libp2pIdentity) -> Swarm<RelayBehaviour> {
    let local_key: identity::Keypair = id.keypair.clone();
    let noise_keys = noise::Config::new(&local_key).expect("noise");
    let tcp = TcpTransport::new(libp2p::tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise_keys)
        .multiplex(yamux::Config::default())
        .boxed();

    let behaviour = RelayBehaviour {
        relay_server: relay::Behaviour::new(id.peer_id, relay::Config::default()),
        identify: identify::Behaviour::new(identify::Config::new(
            format!("/kaspad/libp2p/{}", env!("CARGO_PKG_VERSION")),
            id.keypair.public(),
        )),
        ping: ping::Behaviour::default(),
    };

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Swarm::new(tcp, behaviour, id.peer_id, cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dcutr_client_relay_smoke() {
    let cfg = ConfigBuilder::new().mode(Mode::Full).build();
    let relay_id = Libp2pIdentity::from_config(&cfg).expect("relay id");
    let client_id = Libp2pIdentity::from_config(&cfg).expect("client id");

    let mut relay = build_relay_swarm(&relay_id);
    let mut client = build_client_swarm(&client_id);

    relay.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).expect("relay listen");

    let relay_addr = loop {
        match tokio::time::timeout(Duration::from_secs(5), relay.select_next_some()).await {
            Ok(SwarmEvent::NewListenAddr { address, .. }) => break address,
            Ok(_) => {}
            Err(_) => panic!("relay did not produce a listen address"),
        }
    };

    client.dial(relay_addr).expect("client dial relay");

    let mut relay_established = false;
    let mut client_established = false;
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if Instant::now() > deadline {
            panic!("DCUtR relay smoke test timed out");
        }

        if relay_established && client_established {
            break;
        }

        select! {
            event = relay.select_next_some() => {
                if matches!(event, SwarmEvent::ConnectionEstablished { .. }) {
                    relay_established = true;
                }
            }
            event = client.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == relay_id.peer_id => client_established = true,
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Dcutr(_)) => {}
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    assert!(relay_established, "relay did not establish the client connection");
    assert!(client_established, "client did not establish a connection to relay");
}
