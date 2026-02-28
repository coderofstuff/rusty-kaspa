use super::*;

pub(super) struct IncomingStream {
    pub(super) metadata: TransportMetadata,
    pub(super) direction: StreamDirection,
    pub(super) stream: BoxedLibp2pStream,
}

pub(super) enum SwarmCommand {
    Dial {
        address: Multiaddr,
        respond_to: oneshot::Sender<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>,
    },
    ProbeRelay {
        address: Multiaddr,
        respond_to: oneshot::Sender<Result<PeerId, Libp2pError>>,
    },
    EnsureListening {
        respond_to: oneshot::Sender<Result<(), Libp2pError>>,
    },
    Reserve {
        target: Multiaddr,
        respond_to: oneshot::Sender<Result<ListenerId, Libp2pError>>,
    },
    ReleaseReservation {
        listener_id: ListenerId,
        respond_to: oneshot::Sender<()>,
    },
    PeersSnapshot {
        respond_to: oneshot::Sender<Vec<PeerSnapshot>>,
    },
}

pub(super) struct DialRequest {
    pub(super) respond_to: oneshot::Sender<Result<(TransportMetadata, StreamDirection, BoxedLibp2pStream), Libp2pError>>,
    pub(super) started_at: Instant,
    pub(super) via: DialVia,
}

pub(super) struct PendingProbe {
    pub(super) respond_to: oneshot::Sender<Result<PeerId, Libp2pError>>,
    pub(super) started_at: Instant,
}

pub(super) fn metadata_from_endpoint(peer_id: &PeerId, endpoint: &libp2p::core::ConnectedPoint) -> TransportMetadata {
    let mut md = TransportMetadata::default();
    md.capabilities.libp2p = true;
    md.libp2p_peer_id = Some(peer_id.to_string());
    let (addr, path) = connected_point_to_metadata(endpoint);
    md.path = path;
    md.reported_ip = addr.map(|a| a.ip);

    md
}

pub(super) fn connected_point_to_metadata(endpoint: &libp2p::core::ConnectedPoint) -> (Option<NetAddress>, kaspa_p2p_lib::PathKind) {
    match endpoint {
        libp2p::core::ConnectedPoint::Dialer { address, .. } => multiaddr_to_metadata(address),
        libp2p::core::ConnectedPoint::Listener { local_addr, send_back_addr } => {
            // For relay circuit listeners, send_back_addr may be just "/p2p/<peer_id>" without
            // the circuit marker. Check local_addr for circuit information since it contains
            // the full relay path (e.g., "/ip4/.../p2p-circuit").
            let (_, local_path) = multiaddr_to_metadata(local_addr);
            if matches!(local_path, kaspa_p2p_lib::PathKind::Relay { .. }) {
                // local_addr has circuit info; use its path. Address from send_back_addr is
                // typically unusable for relay circuits (just peer ID, no IP).
                let (addr, _) = multiaddr_to_metadata(send_back_addr);
                (addr, local_path)
            } else {
                // No circuit in local_addr; use send_back_addr as before
                multiaddr_to_metadata(send_back_addr)
            }
        }
    }
}
