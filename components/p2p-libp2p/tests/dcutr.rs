use futures::StreamExt;
use kaspa_p2p_libp2p::Libp2pIdentity;
use kaspa_p2p_libp2p::config::{ConfigBuilder, Mode};
use kaspa_p2p_libp2p::swarm::DcutrBootstrapBehaviour;
use libp2p::core::transport::choice::OrTransport;
use libp2p::core::upgrade;
use libp2p::dcutr;
use libp2p::identify;
use libp2p::multiaddr::Protocol;
use libp2p::noise;
use libp2p::ping;
use libp2p::relay::{self, client as relay_client};
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{PeerId, Transport, identity};
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;

#[derive(NetworkBehaviour)]
struct ClientBehaviour {
    relay_client: relay_client::Behaviour,
    identify: identify::Behaviour,
    dcutr: dcutr::Behaviour,
    dcutr_bootstrap: DcutrBootstrapBehaviour,
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
        dcutr_bootstrap: DcutrBootstrapBehaviour::default(),
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
    let dst_id = Libp2pIdentity::from_config(&cfg).expect("dst id");
    let src_id = Libp2pIdentity::from_config(&cfg).expect("src id");

    let mut relay = build_relay_swarm(&relay_id);
    let mut dst = build_client_swarm(&dst_id);
    let mut src = build_client_swarm(&src_id);

    relay.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).expect("relay listen");
    dst.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).expect("dst listen");
    src.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).expect("src listen");

    let relay_addr = wait_for_listen_addr(&mut relay, "relay").await;
    relay.add_external_address(relay_addr.clone());
    let dst_addr = wait_for_listen_addr(&mut dst, "dst").await;
    let src_addr = wait_for_listen_addr(&mut src, "src").await;
    dst.add_external_address(dst_addr);
    src.add_external_address(src_addr);

    let relay_peer_addr = relay_addr.clone().with(Protocol::P2p(relay_id.peer_id));
    // Official DCUtR sequence: Identify with the relay first so a freshly started
    // relay learns a public address before anyone requests a reservation.
    dst.dial(relay_peer_addr.clone()).expect("dst dial relay");
    wait_for_identify_with_relay(&mut relay, &mut dst, &mut src, relay_id.peer_id, "dst").await;

    let dst_relay_base_addr = relay_peer_addr.clone().with(Protocol::P2pCircuit);
    let dst_relay_addr = dst_relay_base_addr.clone().with(Protocol::P2p(dst_id.peer_id));
    dst.listen_on(dst_relay_base_addr).expect("dst relay listen");
    wait_for_reservation_accepted(&mut relay, &mut dst, &mut src).await;

    src.dial(relay_peer_addr).expect("src dial relay");
    wait_for_identify_with_relay(&mut relay, &mut src, &mut dst, relay_id.peer_id, "src").await;
    src.dial(dst_relay_addr.clone()).expect("src dial dst via relay");

    // Localhost hole-punch (direct DCUtR upgrade) is timing-sensitive under
    // parallel CI. This smoke asserts the reliable path: Identify, reservation,
    // then a relayed src→dst connection. Direct upgrade remains covered by
    // `dcutr_advertisement` and lab `--ignored` hole-punch runs.
    let mut relayed_established = false;
    let mut dial_attempts = 1usize;
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        if Instant::now() > deadline {
            panic!("src did not establish a relayed connection to dst after reservation");
        }
        if relayed_established {
            break;
        }

        select! {
            event = relay.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { .. }
                    | SwarmEvent::Behaviour(RelayBehaviourEvent::Identify(_))
                    | SwarmEvent::Behaviour(RelayBehaviourEvent::Ping(_)) => {}
                    _ => {}
                }
            }
            _ = dst.select_next_some() => {}
            event = src.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. }
                        if peer_id == dst_id.peer_id
                            && endpoint.get_remote_address().iter().any(|protocol| matches!(protocol, Protocol::P2pCircuit)) =>
                    {
                        relayed_established = true;
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id: Some(peer_id), .. }
                        if peer_id == dst_id.peer_id && dial_attempts < 4 && src.dial(dst_relay_addr.clone()).is_ok() =>
                    {
                        dial_attempts += 1;
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    assert!(relayed_established, "source did not establish a relayed dst connection");
}

async fn wait_for_listen_addr<TBehaviour>(swarm: &mut Swarm<TBehaviour>, name: &str) -> libp2p::Multiaddr
where
    TBehaviour: NetworkBehaviour,
{
    loop {
        match tokio::time::timeout(Duration::from_secs(5), swarm.select_next_some()).await {
            Ok(SwarmEvent::NewListenAddr { address, .. }) => break address,
            Ok(_) => {}
            Err(_) => panic!("{name} did not produce a listen address"),
        }
    }
}

async fn wait_for_identify_with_relay(
    relay: &mut Swarm<RelayBehaviour>,
    client: &mut Swarm<ClientBehaviour>,
    other: &mut Swarm<ClientBehaviour>,
    relay_peer: PeerId,
    client_name: &str,
) {
    let mut sent = false;
    let mut received = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !(sent && received) {
        if Instant::now() > deadline {
            panic!("{client_name} did not finish Identify with relay (sent={sent}, received={received})");
        }
        select! {
            _ = relay.select_next_some() => {}
            event = client.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. }))
                        if peer_id == relay_peer =>
                    {
                        sent = true;
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(identify::Event::Received { peer_id, .. }))
                        if peer_id == relay_peer =>
                    {
                        received = true;
                    }
                    _ => {}
                }
            }
            _ = other.select_next_some() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

async fn wait_for_reservation_accepted(
    relay: &mut Swarm<RelayBehaviour>,
    dst: &mut Swarm<ClientBehaviour>,
    src: &mut Swarm<ClientBehaviour>,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() > deadline {
            panic!("dst relay reservation was not accepted");
        }
        select! {
            _ = relay.select_next_some() => {}
            event = dst.select_next_some() => {
                if let SwarmEvent::Behaviour(ClientBehaviourEvent::RelayClient(relay_client::Event::ReservationReqAccepted { .. })) = event {
                    return;
                }
            }
            _ = src.select_next_some() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}
