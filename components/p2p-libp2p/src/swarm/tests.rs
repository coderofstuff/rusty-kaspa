use super::*;

#[tokio::test]
async fn build_swarm_succeeds() {
    let id = Libp2pIdentity::from_config(&crate::config::Config::default()).expect("identity");
    let swarm = build_base_swarm(&id).expect("swarm");
    assert_eq!(swarm.local_peer_id(), &id.peer_id);
}
