use futures::StreamExt;
use kaspa_p2p_libp2p::Libp2pIdentity;
use kaspa_p2p_libp2p::config::{ConfigBuilder, Mode};
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
    let transport = OrTransport::new(relay_transport, tcp)
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
    let dst_direct_addr = wait_for_listen_addr(&mut dst, "dst").await;
    let _ = wait_for_listen_addr(&mut src, "src").await;

    let dst_relay_base_addr = relay_addr.clone().with(Protocol::P2p(relay_id.peer_id)).with(Protocol::P2pCircuit);
    let dst_relay_addr = dst_relay_base_addr.clone().with(Protocol::P2p(dst_id.peer_id));
    dst.listen_on(dst_relay_base_addr).expect("dst relay listen");
    wait_for_reservation(&mut relay, &mut dst, dst_relay_addr.clone(), relay_id.peer_id).await;

    src.dial(dst_relay_addr).expect("src dial dst via relay");

    let expected_direct_addr = dst_direct_addr.with(Protocol::P2p(dst_id.peer_id));
    let mut direct_established = false;
    let mut dcutr_succeeded = false;
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        if Instant::now() > deadline {
            panic!("DCUtR relay smoke test timed out");
        }

        if direct_established && dcutr_succeeded {
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
            event = dst.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Dcutr(dcutr::Event { remote_peer_id, result: Err(err) }))
                        if remote_peer_id == src_id.peer_id =>
                    {
                        panic!("DCUtR upgrade failed on dst side: {err}");
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Dcutr(dcutr::Event { remote_peer_id, result: Ok(_) }))
                        if remote_peer_id == src_id.peer_id => {}
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(_))
                    | SwarmEvent::Behaviour(ClientBehaviourEvent::RelayClient(_))
                    | SwarmEvent::Behaviour(ClientBehaviourEvent::Ping(_))
                    | SwarmEvent::ConnectionEstablished { .. } => {}
                    _ => {}
                }
            }
            event = src.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { endpoint, .. } if *endpoint.get_remote_address() == expected_direct_addr => {
                        direct_established = true;
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Dcutr(dcutr::Event { remote_peer_id, result }))
                        if remote_peer_id == dst_id.peer_id =>
                    {
                        match result {
                            Ok(_) => dcutr_succeeded = true,
                            Err(err) => panic!("DCUtR upgrade failed on src side: {err}"),
                        }
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(_))
                    | SwarmEvent::Behaviour(ClientBehaviourEvent::RelayClient(_))
                    | SwarmEvent::Behaviour(ClientBehaviourEvent::Ping(_))
                    | SwarmEvent::ConnectionEstablished { .. } => {}
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    assert!(dcutr_succeeded, "source did not report a successful DCUtR upgrade");
    assert!(direct_established, "source did not establish a direct dst connection");
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

async fn wait_for_reservation(
    relay: &mut Swarm<RelayBehaviour>,
    dst: &mut Swarm<ClientBehaviour>,
    expected_relayed_addr: libp2p::Multiaddr,
    relay_peer_id: PeerId,
) {
    let mut new_relay_listen_addr = false;
    let mut reservation_accepted = false;
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if Instant::now() > deadline {
            panic!("timed out waiting for relay reservation");
        }

        if new_relay_listen_addr && reservation_accepted {
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
            event = dst.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } if address == expected_relayed_addr => {
                        new_relay_listen_addr = true;
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::RelayClient(relay_client::Event::ReservationReqAccepted {
                        relay_peer_id: accepted_relay_peer_id,
                        renewal: false,
                        ..
                    })) if accepted_relay_peer_id == relay_peer_id => {
                        reservation_accepted = true;
                    }
                    SwarmEvent::ListenerClosed { reason, .. } => {
                        panic!("relay reservation listener closed unexpectedly: {reason:?}");
                    }
                    SwarmEvent::Behaviour(ClientBehaviourEvent::Identify(_))
                    | SwarmEvent::Behaviour(ClientBehaviourEvent::Ping(_))
                    | SwarmEvent::Dialing { .. }
                    | SwarmEvent::ConnectionEstablished { .. }
                    | SwarmEvent::NewExternalAddrCandidate { .. }
                    | SwarmEvent::ExternalAddrConfirmed { .. }
                    | SwarmEvent::ExternalAddrExpired { .. } => {}
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}
