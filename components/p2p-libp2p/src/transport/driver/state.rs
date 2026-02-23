impl SwarmDriver {
    pub(super) fn bootstrap(&mut self) {
        info!("libp2p bootstrap: adding {} external addresses", self.external_addrs.len());
        for addr in self.external_addrs.clone() {
            info!("libp2p bootstrap: registering external address: {}", addr);
            self.swarm.add_external_address(addr);
        }
        // Log the swarm's external addresses after adding
        let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
        info!("libp2p bootstrap: swarm now has {} external addresses: {:?}", external_addrs.len(), external_addrs);
        let _ = self.start_listening();
        self.publish_relay_hint();
    }
    pub(super) fn publish_relay_hint(&self) {
        let hint = self.active_relay.as_ref().map(|relay| relay.circuit_base.to_string());
        let _ = self.relay_hint_tx.send(hint);
    }
    pub(super) fn set_active_relay(&mut self, relay: RelayInfo, listener: Option<ListenerId>) {
        let relay_changed = self.active_relay.as_ref().map(|current| current.relay_peer) != Some(relay.relay_peer);
        self.active_relay = Some(relay);
        self.active_relay_listener = listener;
        if relay_changed {
            self.invalidate_dcutr_cached_candidates("relay_changed");
        }
        self.publish_relay_hint();
    }
    pub(super) fn clear_active_relay(&mut self) {
        if self.active_relay.is_some() {
            self.invalidate_dcutr_cached_candidates("relay_cleared");
        }
        self.active_relay = None;
        self.active_relay_listener = None;
        self.publish_relay_hint();
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        swarm: libp2p::Swarm<Libp2pBehaviour>,
        command_rx: mpsc::Receiver<SwarmCommand>,
        incoming_tx: mpsc::Sender<IncomingStream>,
        listen_addrs: Vec<Multiaddr>,
        external_addrs: Vec<Multiaddr>,
        allow_private_addrs: bool,
        reservations: Vec<ReservationTarget>,
        role_tx: watch::Sender<crate::Role>,
        relay_hint_tx: watch::Sender<Option<String>>,
        config_role: crate::Role,
        max_peers_per_relay: usize,
        auto_role_window: Duration,
        auto_role_required_autonat: usize,
        auto_role_required_direct: usize,
        shutdown: Listener,
        metrics: Option<Arc<Libp2pMetrics>>,
    ) -> Self {
        let local_peer_id = *swarm.local_peer_id();
        let active_relay = reservations.into_iter().find_map(|r| relay_info_from_multiaddr(&r.multiaddr, local_peer_id));
        let auto_role = if matches!(config_role, crate::Role::Auto) {
            Some(AutoRoleState::new(role_tx, auto_role_window, auto_role_required_autonat, auto_role_required_direct))
        } else {
            None
        };
        let now = Instant::now();
        let local_candidate_meta = external_addrs
            .iter()
            .cloned()
            .map(|addr| (addr, LocalCandidateMeta { source: LocalCandidateSource::Config, updated_at: now }))
            .collect();

        Self {
            swarm,
            command_rx,
            incoming_tx,
            pending_dials: HashMap::new(),
            pending_probes: HashMap::new(),
            dialback_cooldowns: HashMap::new(),
            direct_upgrade_cooldowns: HashMap::new(),
            listen_addrs,
            external_addrs,
            allow_private_addrs,
            local_candidate_meta,
            peer_states: HashMap::new(),
            dcutr_retries: HashMap::new(),
            reservation_listeners: HashSet::new(),
            active_relay,
            active_relay_listener: None,
            auto_role,
            max_peers_per_relay: max_peers_per_relay.max(1),
            autonat_private_until: None,
            metrics,
            listening: false,
            shutdown,
            connections: HashMap::new(),
            relay_hint_tx,
        }
    }
    pub(super) fn start_listening(&mut self) -> Result<(), Libp2pError> {
        if self.listening {
            return Ok(());
        }

        let addrs = if self.listen_addrs.is_empty() { vec![default_listen_addr()] } else { self.listen_addrs.clone() };
        info!("libp2p starting listen on {:?}", addrs);

        for addr in addrs {
            if let Err(err) = self.swarm.listen_on(addr) {
                warn!("libp2p failed to listen: {err}");
                return Err(Libp2pError::ListenFailed(err.to_string()));
            }
        }

        self.listening = true;
        Ok(())
    }
    pub(super) fn track_established(&mut self, peer_id: PeerId, endpoint: &libp2p::core::ConnectedPoint) {
        let state = self.peer_states.entry(peer_id).or_default();
        if matches!(endpoint, libp2p::core::ConnectedPoint::Dialer { .. }) {
            state.outgoing = state.outgoing.saturating_add(1);
        }
        if endpoint_uses_relay(endpoint) {
            state.connected_via_relay = true;
            debug!("libp2p track_established: peer {} connected via relay", peer_id);
        } else {
            debug!("libp2p track_established: peer {} connected DIRECTLY (no relay)", peer_id);
        }
    }
    pub(super) fn track_closed(&mut self, peer_id: PeerId, endpoint: &libp2p::core::ConnectedPoint) {
        if let Some(state) = self.peer_states.get_mut(&peer_id) {
            if matches!(endpoint, libp2p::core::ConnectedPoint::Dialer { .. }) && state.outgoing > 0 {
                state.outgoing -= 1;
            }
            if endpoint_uses_relay(endpoint) {
                state.connected_via_relay = false;
            }
        }
    }
    pub(super) fn has_usable_external_addr(&self) -> bool {
        self.swarm.external_addresses().any(|addr| self.is_usable_external_addr(addr))
    }
    pub(super) fn is_usable_external_addr(&self, addr: &Multiaddr) -> bool {
        if !is_tcp_dialable(addr) {
            return false;
        }
        let mut ip: Option<std::net::IpAddr> = None;
        for protocol in addr.iter() {
            match protocol {
                Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
                Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
                Protocol::P2pCircuit => return false,
                _ => {}
            }
        }
        let Some(ip) = ip else {
            return false;
        };
        if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
            return false;
        }
        if self.allow_private_addrs {
            return true;
        }
        kaspa_utils::networking::IpAddress::new(ip).is_publicly_routable()
    }
    pub(super) fn has_relay_connection(&self, peer_id: PeerId) -> bool {
        self.connections.values().any(|conn| conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. }))
    }
    pub(super) fn note_direct_upgrade(&mut self, peer_id: PeerId, had_pending_relay: bool) {
        self.clear_dcutr_retry(peer_id);
        if !had_pending_relay && !self.has_relay_connection(peer_id) {
            return;
        }
        self.direct_upgrade_cooldowns.insert(peer_id, Instant::now() + DIRECT_UPGRADE_COOLDOWN);
    }
    pub(super) fn record_connection(
        &mut self,
        connection_id: StreamRequestId,
        peer_id: PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
        dcutr_upgraded: bool,
    ) {
        let path = if endpoint_uses_relay(endpoint) { PathKind::Relay { relay_id: None } } else { PathKind::Direct };
        let relay_id = match endpoint {
            libp2p::core::ConnectedPoint::Dialer { address, .. } => relay_id_from_multiaddr(address),
            libp2p::core::ConnectedPoint::Listener { send_back_addr, local_addr } => {
                relay_id_from_multiaddr(send_back_addr).or_else(|| relay_id_from_multiaddr(local_addr))
            }
        };
        let outbound = endpoint.is_dialer();
        self.connections
            .insert(connection_id, ConnectionEntry { peer_id, path, relay_id, outbound, since: Instant::now(), dcutr_upgraded });
    }
    pub(super) fn close_relay_connections_for_peer(&mut self, peer_id: PeerId, keep: StreamRequestId) {
        let relay_ids: Vec<_> = self
            .connections
            .iter()
            .filter(|(id, conn)| **id != keep && conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. }))
            .map(|(id, _)| *id)
            .collect();
        for id in relay_ids {
            if self.swarm.close_connection(id) {
                info!("libp2p: closing relay connection {id:?} to {peer_id} after direct path established");
            }
            self.connections.remove(&id);
        }
    }
    pub(super) fn enforce_relay_cap(&mut self, connection_id: StreamRequestId) {
        let Some(conn) = self.connections.get(&connection_id) else {
            return;
        };
        if !matches!(conn.path, PathKind::Relay { .. }) {
            return;
        }
        let relay_id = conn.relay_id.clone();
        let mut relay_peers: HashSet<PeerId> = self
            .connections
            .iter()
            .filter(|(id, entry)| **id != connection_id && matches!(entry.path, PathKind::Relay { .. }) && entry.relay_id == relay_id)
            .map(|(_, entry)| entry.peer_id)
            .collect();
        relay_peers.insert(conn.peer_id);
        let unique_peers = relay_peers.len();
        if unique_peers > self.max_peers_per_relay {
            if self.swarm.close_connection(connection_id) {
                info!("libp2p: closing relay connection {connection_id:?} for relay cap");
            }
            self.connections.remove(&connection_id);
        }
    }
    pub(super) fn peers_snapshot(&self) -> Vec<PeerSnapshot> {
        let now = Instant::now();
        self.connections
            .values()
            .map(|entry| PeerSnapshot {
                peer_id: entry.peer_id.to_string(),
                path: match &entry.path {
                    PathKind::Direct => "direct".to_string(),
                    PathKind::Relay { .. } => "relay".to_string(),
                    PathKind::Unknown => "unknown".to_string(),
                },
                relay_id: entry.relay_id.clone(),
                direction: if entry.outbound { "outbound".to_string() } else { "inbound".to_string() },
                duration_ms: now.saturating_duration_since(entry.since).as_millis(),
                libp2p: true,
                dcutr_upgraded: entry.dcutr_upgraded,
            })
            .collect()
    }
}
