impl SwarmDriver {
    pub(super) fn expire_pending_dials(&mut self, reason: &str) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending_dials
            .iter()
            .filter(|(_, pending)| now.saturating_duration_since(pending.started_at) >= PENDING_DIAL_TIMEOUT)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.fail_pending(id, reason);
        }
    }
    pub(super) fn expire_pending_probes(&mut self, reason: &str) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, pending)| now.saturating_duration_since(pending.started_at) >= PENDING_DIAL_TIMEOUT)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.fail_probe(id, reason);
        }
    }
    pub(super) fn fail_pending(&mut self, request_id: StreamRequestId, err: impl ToString) {
        if let Some(pending) = self.pending_dials.remove(&request_id) {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
        }
    }
    pub(super) fn fail_probe(&mut self, request_id: StreamRequestId, err: impl ToString) {
        if let Some(pending) = self.pending_probes.remove(&request_id) {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
        }
    }
    pub(super) fn enqueue_incoming(&self, incoming: IncomingStream) {
        let direction = incoming.direction;
        if let Err(err) = self.incoming_tx.try_send(incoming) {
            match err {
                TrySendError::Full(_) => warn!("libp2p_bridge: dropping {:?} stream because channel is full", direction),
                TrySendError::Closed(_) => warn!("libp2p_bridge: dropping {:?} stream because receiver is closed", direction),
            }
        }
    }
    pub(super) fn take_pending_relay_by_peer(&mut self, peer_id: &PeerId) -> Option<(StreamRequestId, DialRequest)> {
        let candidate = self
            .pending_dials
            .iter()
            .filter(|(_, pending)| matches!(pending.via, DialVia::Relay { target_peer } if target_peer == *peer_id))
            .min_by_key(|(_, pending)| pending.started_at)
            .map(|(id, _)| *id)?;
        // Use FIFO ordering when multiple relay dials are outstanding for the same peer.
        self.pending_dials.remove(&candidate).map(|req| (candidate, req))
    }
    pub(super) async fn handle_command(&mut self, command: SwarmCommand) {
        match command {
            SwarmCommand::Dial { address, respond_to } => {
                info!("libp2p dial request to {address}");

                // For relay addresses, track by target peer so DCUtR success can resolve the dial
                let is_relay = addr_uses_relay(&address);
                let target_peer = if is_relay { extract_circuit_target_peer(&address) } else { None };

                let dial_opts = DialOpts::unknown_peer_id().address(address).build();
                let request_id = dial_opts.connection_id();
                let started_at = Instant::now();
                let via = target_peer.map_or(DialVia::Direct, |peer| DialVia::Relay { target_peer: peer });
                match self.swarm.dial(dial_opts) {
                    Ok(()) => {
                        self.pending_dials.insert(request_id, DialRequest { respond_to, started_at, via });
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
                    }
                }
            }
            SwarmCommand::ProbeRelay { address, respond_to } => {
                info!("libp2p relay probe request to {address}");
                let dial_opts = DialOpts::unknown_peer_id().address(address).build();
                let request_id = dial_opts.connection_id();
                let started_at = Instant::now();
                match self.swarm.dial(dial_opts) {
                    Ok(()) => {
                        self.pending_probes.insert(request_id, PendingProbe { respond_to, started_at });
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(Libp2pError::DialFailed(err.to_string())));
                    }
                }
            }
            SwarmCommand::EnsureListening { respond_to } => {
                let _ = respond_to.send(self.start_listening());
            }
            SwarmCommand::Reserve { mut target, respond_to } => {
                if !target.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
                    target.push(Protocol::P2pCircuit);
                }
                let target_for_info = target.clone();
                match self.swarm.listen_on(target) {
                    Ok(listener) => {
                        self.reservation_listeners.insert(listener);
                        if let Some(info) = relay_info_from_multiaddr(&target_for_info, *self.swarm.local_peer_id()) {
                            self.set_active_relay(info, Some(listener));
                        }
                        let _ = respond_to.send(Ok(listener));
                    }
                    Err(err) => {
                        let _ = respond_to.send(Err(Libp2pError::ReservationFailed(err.to_string())));
                    }
                }
            }
            SwarmCommand::ReleaseReservation { listener_id, respond_to } => {
                self.reservation_listeners.remove(&listener_id);
                let _ = self.swarm.remove_listener(listener_id);
                if self.active_relay_listener == Some(listener_id) {
                    self.clear_active_relay();
                }
                let _ = respond_to.send(());
            }
            SwarmCommand::PeersSnapshot { respond_to } => {
                let _ = respond_to.send(self.peers_snapshot());
            }
        }
    }
    pub(super) fn handle_connection_established_event(
        &mut self,
        peer_id: PeerId,
        connection_id: StreamRequestId,
        endpoint: libp2p::core::ConnectedPoint,
    ) {
        debug!("libp2p connection established with {peer_id} on {connection_id:?}");
        self.track_established(peer_id, &endpoint);

        let has_external_addr = self.has_usable_external_addr();
        if let Some(auto_role) = self.auto_role.as_mut()
            && !endpoint.is_dialer()
            && !endpoint_uses_relay(&endpoint)
        {
            auto_role.record_direct_inbound(Instant::now());
        }
        if let Some(auto_role) = self.auto_role.as_mut() {
            match auto_role.update_role(Instant::now(), has_external_addr) {
                Some(crate::Role::Public) => info!("libp2p auto role: promoted to public after direct inbound"),
                Some(crate::Role::Private) => info!("libp2p auto role: demoted to private"),
                _ => {}
            }
        }

        if let Some(pending) = self.pending_probes.remove(&connection_id) {
            info!("libp2p relay probe connected to {peer_id}");
            self.record_connection(connection_id, peer_id, &endpoint, false);
            let _ = pending.respond_to.send(Ok(peer_id));
            return;
        }

        // For DCUtR direct connections spawned from relay dials, transfer the earliest
        // pending relay dial for this peer onto the new connection_id.
        let mut had_pending_relay = false;
        if !endpoint_uses_relay(&endpoint)
            && let Some((_old_req, pending)) = self.take_pending_relay_by_peer(&peer_id)
        {
            info!("libp2p DCUtR success: direct connection to {peer_id} resolves pending relay dial");
            self.pending_dials.insert(connection_id, pending);
            had_pending_relay = true;
        }
        if had_pending_relay && let Some(metrics) = self.metrics.as_ref() {
            metrics.dcutr().record_dialback_success();
        }

        if endpoint.is_dialer() {
            info!("libp2p initiating stream to {peer_id} (as dialer)");
            self.request_stream_bridge(peer_id, connection_id);
        } else if had_pending_relay {
            // DCUtR succeeded but we're the listener - still need to initiate stream
            // because we had a pending outbound dial that needs to be resolved
            info!("libp2p DCUtR: initiating stream to {peer_id} (as listener with pending dial)");
            self.request_stream_bridge(peer_id, connection_id);
        } else {
            debug!("libp2p waiting for stream from {peer_id} (as listener)");
            // If we are only a listener on a relayed connection and the peer supports DCUtR,
            // initiate a bidirectional dial-back via the active relay so we become a dialer too.
            self.maybe_request_dialback(peer_id);
        }
        let direct_upgrade = !endpoint_uses_relay(&endpoint) && (had_pending_relay || self.has_relay_connection(peer_id));
        self.record_connection(connection_id, peer_id, &endpoint, direct_upgrade);
        if !endpoint_uses_relay(&endpoint) {
            self.note_direct_upgrade(peer_id, direct_upgrade);
        }
        if endpoint_uses_relay(&endpoint) {
            self.enforce_relay_cap(connection_id);
        } else {
            self.close_relay_connections_for_peer(peer_id, connection_id);
        }
    }
    pub(super) fn handle_connection_closed_event(
        &mut self,
        peer_id: PeerId,
        connection_id: StreamRequestId,
        endpoint: libp2p::core::ConnectedPoint,
    ) {
        self.fail_pending(connection_id, "connection closed before stream");
        self.fail_probe(connection_id, "relay probe connection closed");
        self.connections.remove(&connection_id);
        self.track_closed(peer_id, &endpoint);
        let active_relay_closed =
            self.active_relay.as_ref().is_some_and(|relay| relay.relay_peer == peer_id) && !self.has_direct_connection(peer_id);
        if active_relay_closed {
            info!("libp2p active relay connection to {peer_id} closed");
            self.clear_active_relay();
        }
    }
    pub(super) fn handle_outgoing_connection_error_event(&mut self, connection_id: StreamRequestId, error: String) {
        self.fail_pending(connection_id, &error);
        self.fail_probe(connection_id, &error);
        self.connections.remove(&connection_id);
    }
    pub(super) async fn handle_stream_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::Inbound { peer_id, _connection_id: connection_id, endpoint, stream } => {
                // For dialed connections, we normally skip inbound streams to avoid duplicates.
                // However, for DCUtR-upgraded connections where this node has role_override = Listener,
                // we ARE the server and MUST accept inbound streams - the remote will send us data.
                if let libp2p::core::ConnectedPoint::Dialer { role_override, .. } = &endpoint {
                    if !matches!(role_override, libp2p::core::Endpoint::Listener) {
                        debug!("libp2p_bridge: skipping inbound stream on dialed connection to {peer_id} (no role_override)");
                        return;
                    }
                    info!("libp2p_bridge: accepting inbound stream on DCUtR connection (role_override=Listener) from {peer_id}");
                }
                info!("libp2p_bridge: StreamEvent::Inbound peer={} endpoint={:?}", peer_id, endpoint);
                let mut metadata = metadata_from_endpoint(&peer_id, &endpoint);
                // If endpoint-based path detection returned Unknown, fall back to our
                // connection records which track whether a connection uses a relay circuit.
                // This is needed because the send_back_addr for relay circuit listeners
                // may not contain the P2pCircuit protocol marker.
                if matches!(metadata.path, kaspa_p2p_lib::PathKind::Unknown)
                    && let Some(conn) = self.connections.get(&connection_id)
                {
                    metadata.path = conn.path.clone();
                }
                info!("libp2p_bridge: inbound stream from {peer_id} over {:?}, handing to Kaspa", metadata.path);
                let incoming = IncomingStream { metadata, direction: StreamDirection::Inbound, stream: Box::new(stream.compat()) };
                self.enqueue_incoming(incoming);
            }
            StreamEvent::Outbound { peer_id, request_id, endpoint, stream, .. } => {
                let metadata = metadata_from_endpoint(&peer_id, &endpoint);
                let direction = match &endpoint {
                    libp2p::core::ConnectedPoint::Dialer { role_override: libp2p::core::Endpoint::Listener, .. } => {
                        info!(
                            "libp2p_bridge: DCUtR role_override detected, treating outbound stream as inbound (h2 server) for {peer_id}"
                        );
                        StreamDirection::Inbound
                    }
                    _ => StreamDirection::Outbound,
                };
                info!(
                    "libp2p_bridge: StreamEvent::Outbound peer={} req_id={:?} endpoint={:?} direction={:?}",
                    peer_id, request_id, endpoint, direction
                );
                if let Some(pending) = self.pending_dials.remove(&request_id) {
                    let _ = pending.respond_to.send(Ok((metadata, direction, Box::new(stream.compat()))));
                } else {
                    info!(
                        "libp2p_bridge: outbound stream with no pending dial (req {request_id:?}) from {peer_id}; handing to Kaspa (direction={:?})",
                        direction
                    );
                    let incoming = IncomingStream { metadata, direction, stream: Box::new(stream.compat()) };
                    let _ = self.incoming_tx.send(incoming).await;
                }
            }
        }
    }
    pub(super) fn request_stream_bridge(&mut self, peer_id: PeerId, connection_id: StreamRequestId) {
        // Check if there's already a pending dial for this connection (e.g., from relay dial transfer)
        // If so, don't create a new channel - the existing one will be resolved via StreamEvent::Outbound
        if self.pending_dials.contains_key(&connection_id) {
            debug!("libp2p request_stream_bridge: reusing existing pending dial for {connection_id:?}");
            self.swarm.behaviour_mut().streams.request_stream(peer_id, connection_id, connection_id);
            return;
        }
        info!("libp2p_bridge: request_stream_bridge peer={} conn_id={:?} (requesting substream)", peer_id, connection_id);

        let (respond_to, rx) = oneshot::channel();
        self.pending_dials.insert(connection_id, DialRequest { respond_to, started_at: Instant::now(), via: DialVia::Direct });
        self.swarm.behaviour_mut().streams.request_stream(peer_id, connection_id, connection_id);

        let tx = self.incoming_tx.clone();
        spawn(async move {
            if let Ok(Ok((metadata, direction, stream))) = rx.await {
                info!(
                    "libp2p_bridge: established stream with {peer_id} (req {connection_id:?}); handing to Kaspa (direction={:?})",
                    direction
                );
                let incoming = IncomingStream { metadata, direction, stream };
                if let Err(err) = tx.try_send(incoming) {
                    match err {
                        TrySendError::Full(_) => {
                            warn!("libp2p_bridge: dropping outbound stream for {peer_id} because channel is full")
                        }
                        TrySendError::Closed(_) => {
                            warn!("libp2p_bridge: dropping outbound stream for {peer_id} because receiver is closed")
                        }
                    }
                }
            }
        });
    }
}
