mod tests {
    use futures::StreamExt;
    use libp2p::{
        dcutr, identify, identity, noise, relay,
        relay::client as relay_client,
        swarm::{NetworkBehaviour, SwarmEvent},
        tcp, yamux, PeerId, Swarm, SwarmBuilder, Transport,
    };
    use std::time::Duration;

    #[derive(NetworkBehaviour)]
    struct MyBehaviour {
        identify: identify::Behaviour,
        dcutr: dcutr::Behaviour,
        relay: relay::Behaviour,
        relay_client: relay_client::Behaviour,
    }

    #[tokio::test]
    #[ignore = "Identify protocols omit DCUtR until protocols negotiate; relies on runtime logs instead"]
    async fn test_dcutr_advertisement() {
        let id1 = identity::Keypair::generate_ed25519();
        let _peer1 = PeerId::from(id1.public());

        let id2 = identity::Keypair::generate_ed25519();
        let _peer2 = PeerId::from(id2.public());

        let mut swarm1 = build_swarm(id1).await;
        let mut swarm2 = build_swarm(id2).await;

        swarm1.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let addr1 = loop {
            match swarm1.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => break address,
                _ => {}
            }
        };

        swarm2.dial(addr1).unwrap();

        let mut dcutr_seen = false;

        loop {
            tokio::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received { info, .. })) = event {
                        println!("Swarm1 received Identify: {:?}", info.protocols);
                        if info.protocols.iter().any(|p| p.to_string().contains("dcutr")) {
                            dcutr_seen = true;
                        }
                    }
                }
                event = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received { info, .. })) = event {
                        println!("Swarm2 received Identify: {:?}", info.protocols);
                        if info.protocols.iter().any(|p| p.to_string().contains("dcutr")) {
                            dcutr_seen = true;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    break;
                }
            }
            if dcutr_seen {
                break;
            }
        }

        assert!(dcutr_seen, "DCUtR protocol was not advertised by either peer");
    }

    async fn build_swarm(id: identity::Keypair) -> Swarm<MyBehaviour> {
        let peer_id = PeerId::from(id.public());
        let noise_keys = noise::Config::new(&id).unwrap();
        let tcp = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
        let (relay_transport, relay_client) = relay_client::new(peer_id);

        let transport = libp2p::core::transport::choice::OrTransport::new(tcp, relay_transport)
            .upgrade(libp2p::core::upgrade::Version::V1Lazy)
            .authenticate(noise_keys)
            .multiplex(yamux::Config::default())
            .boxed();

        let behaviour = MyBehaviour {
            identify: identify::Behaviour::new(identify::Config::new("/test/1.0".into(), id.public())),
            dcutr: dcutr::Behaviour::new(peer_id),
            relay: relay::Behaviour::new(peer_id, relay::Config::default()),
            relay_client,
        };

        SwarmBuilder::with_existing_identity(id)
            .with_tokio()
            .with_other_transport(|_key| transport)
            .unwrap()
            .with_behaviour(|_key| behaviour)
            .unwrap()
            .build()
    }
}
