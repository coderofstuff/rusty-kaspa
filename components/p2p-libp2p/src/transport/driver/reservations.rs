impl SwarmDriver {
    pub(super) fn handle_relay_client_event(&mut self, event: relay::client::Event) {
        #[allow(unreachable_patterns)]
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, renewal, .. } => {
                info!("libp2p reservation accepted by {relay_peer_id}, renewal={renewal}");
            }
            relay::client::Event::OutboundCircuitEstablished { relay_peer_id, .. } => {
                info!("libp2p outbound circuit established via {relay_peer_id}");
            }
            relay::client::Event::InboundCircuitEstablished { src_peer_id, .. } => {
                info!("libp2p inbound circuit established from {src_peer_id}");
                self.mark_relay_path(src_peer_id);
                self.maybe_request_dialback(src_peer_id);
            }
            _ => {}
        }
    }
}
