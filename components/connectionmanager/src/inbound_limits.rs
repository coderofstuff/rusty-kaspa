use std::collections::HashMap;

use futures_util::future::join_all;
use itertools::Itertools;
use kaspa_p2p_lib::{PathKind, Peer};
use rand::{seq::SliceRandom, thread_rng};

use crate::ConnectionManager;

impl ConnectionManager {
    pub(super) fn is_libp2p_peer(peer: &Peer) -> bool {
        let md = peer.metadata();
        md.capabilities.libp2p || matches!(md.path, PathKind::Relay { .. })
    }

    pub(super) async fn terminate_peers(adaptor: &kaspa_p2p_lib::Adaptor, peers: &[&Peer]) {
        let futures = peers.iter().map(|peer| adaptor.terminate(peer.key()));
        join_all(futures).await;
    }

    pub(super) fn drop_libp2p_over_cap<'a>(peers: &'a [&Peer], cap: usize) -> Vec<&'a Peer> {
        if peers.len() > cap { peers.choose_multiple(&mut thread_rng(), peers.len() - cap).cloned().collect_vec() } else { Vec::new() }
    }

    pub(super) fn private_libp2p_cap(inbound_limit: usize, private_role_cap: usize) -> usize {
        private_role_cap.min(inbound_limit.max(1))
    }

    pub(super) fn public_libp2p_cap(inbound_limit: usize) -> usize {
        (inbound_limit / 2).max(1)
    }

    pub(super) fn relay_overflow<'a>(peers: &'a [&Peer], per_relay_cap: usize, unknown_cap: usize) -> Vec<&'a Peer> {
        let mut buckets: HashMap<Option<String>, Vec<&Peer>> = HashMap::new();
        for peer in peers {
            let relay_id = match &peer.metadata().path {
                PathKind::Relay { relay_id } => relay_id.clone(),
                _ => None,
            };
            buckets.entry(relay_id).or_default().push(*peer);
        }

        let mut to_drop = Vec::new();
        for (relay_id, peers) in buckets {
            let cap = if relay_id.is_some() { per_relay_cap } else { unknown_cap };
            if peers.len() > cap {
                let drop = peers.choose_multiple(&mut thread_rng(), peers.len() - cap).cloned().collect_vec();
                to_drop.extend(drop);
            }
        }
        to_drop
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use kaspa_p2p_lib::PeerProperties;
    use kaspa_p2p_lib::transport::{Capabilities, TransportMetadata};
    use kaspa_utils::networking::PeerId;
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Instant;

    fn make_peer_with_path(path: PathKind, ip: Ipv4Addr, caps: Capabilities) -> Peer {
        let metadata = TransportMetadata { path, capabilities: caps, ..Default::default() };
        Peer::new(
            PeerId::default(),
            SocketAddr::from((ip, 16000)),
            false,
            Instant::now(),
            Arc::new(PeerProperties::default()),
            0,
            metadata,
        )
    }

    fn make_relay_peer(relay_id: Option<&str>) -> Peer {
        let path = PathKind::Relay { relay_id: relay_id.map(|id| id.to_string()) };
        make_peer_with_path(path, Ipv4Addr::new(127, 0, 0, 1), Capabilities { libp2p: true })
    }

    #[test]
    fn relay_overflow_drops_expected_counts() {
        let r1 = vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r1")), make_relay_peer(Some("r1"))];
        let r2 = vec![make_relay_peer(Some("r2")), make_relay_peer(Some("r2"))];
        let unknown = vec![make_relay_peer(None), make_relay_peer(None)];

        let mut all = Vec::new();
        all.extend(r1.iter());
        all.extend(r2.iter());
        all.extend(unknown.iter());

        let to_drop = ConnectionManager::relay_overflow(&all, 2, 1);
        let mut counts: HashMap<Option<String>, usize> = HashMap::new();
        for peer in to_drop {
            let relay = match &peer.metadata().path {
                PathKind::Relay { relay_id } => relay_id.clone(),
                _ => None,
            };
            *counts.entry(relay).or_default() += 1;
        }

        assert_eq!(counts.get(&Some("r1".to_string())), Some(&1));
        assert_eq!(counts.get(&Some("r2".to_string())), None);
        assert_eq!(counts.get(&None), Some(&1));
    }

    #[test]
    fn relay_overflow_enforces_per_relay_cap() {
        let relay_a = PathKind::Relay { relay_id: Some("relay-a".into()) };
        let relay_b = PathKind::Relay { relay_id: Some("relay-b".into()) };
        let peers = vec![
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 1), Capabilities { libp2p: true }),
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 2), Capabilities { libp2p: true }),
            make_peer_with_path(relay_a.clone(), Ipv4Addr::new(10, 0, 0, 3), Capabilities { libp2p: true }),
            make_peer_with_path(relay_b.clone(), Ipv4Addr::new(10, 0, 0, 4), Capabilities { libp2p: true }),
        ];
        let refs: Vec<&Peer> = peers.iter().collect();
        let dropped = ConnectionManager::relay_overflow(&refs, 2, 8);
        assert_eq!(dropped.len(), 1, "only one peer from relay-a should be dropped");
        assert!(dropped.iter().all(|p| matches!(p.metadata().path, PathKind::Relay { .. })));
    }

    #[test]
    fn relay_overflow_enforces_unknown_bucket() {
        let relay_unknown = PathKind::Relay { relay_id: None };
        let peers = vec![
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 1), Capabilities { libp2p: true }),
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 2), Capabilities { libp2p: true }),
            make_peer_with_path(relay_unknown.clone(), Ipv4Addr::new(10, 0, 1, 3), Capabilities { libp2p: true }),
        ];
        let refs: Vec<&Peer> = peers.iter().collect();
        let dropped = ConnectionManager::relay_overflow(&refs, 4, 2);
        assert_eq!(dropped.len(), 1, "unknown relay bucket should drop overflow");
        assert!(matches!(dropped[0].metadata().path, PathKind::Relay { relay_id: None }));
    }

    #[test]
    fn libp2p_classification_detects_capability_and_relay_path() {
        let direct_libp2p = make_peer_with_path(PathKind::Direct, Ipv4Addr::new(10, 0, 2, 1), Capabilities { libp2p: true });
        assert!(ConnectionManager::is_libp2p_peer(&direct_libp2p));

        let relay_path =
            make_peer_with_path(PathKind::Relay { relay_id: None }, Ipv4Addr::new(10, 0, 2, 2), Capabilities { libp2p: false });
        assert!(ConnectionManager::is_libp2p_peer(&relay_path));

        let direct_plain = make_peer_with_path(PathKind::Direct, Ipv4Addr::new(10, 0, 2, 3), Capabilities { libp2p: false });
        assert!(!ConnectionManager::is_libp2p_peer(&direct_plain));
    }

    #[test]
    fn drop_libp2p_over_cap_limits_set() {
        let peers =
            vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r2")), make_relay_peer(Some("r3")), make_relay_peer(Some("r4"))];
        let refs: Vec<&Peer> = peers.iter().collect();
        let to_drop = ConnectionManager::drop_libp2p_over_cap(&refs, 2);
        assert_eq!(to_drop.len(), 2);
    }

    #[test]
    fn drop_libp2p_over_cap_no_drop_when_under_cap() {
        let peers = vec![make_relay_peer(Some("r1")), make_relay_peer(Some("r2"))];
        let refs: Vec<&Peer> = peers.iter().collect();
        let to_drop = ConnectionManager::drop_libp2p_over_cap(&refs, 4);
        assert!(to_drop.is_empty());
    }

    #[test]
    fn private_libp2p_cap_respects_inbound_limit() {
        assert_eq!(ConnectionManager::private_libp2p_cap(16, 8), 8);
        assert_eq!(ConnectionManager::private_libp2p_cap(4, 8), 4);
        assert_eq!(ConnectionManager::private_libp2p_cap(0, 8), 1);
    }

    #[test]
    fn public_libp2p_cap_has_floor_of_one() {
        assert_eq!(ConnectionManager::public_libp2p_cap(16), 8);
        assert_eq!(ConnectionManager::public_libp2p_cap(3), 1);
        assert_eq!(ConnectionManager::public_libp2p_cap(0), 1);
    }
}
