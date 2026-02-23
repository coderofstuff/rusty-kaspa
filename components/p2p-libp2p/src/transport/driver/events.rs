impl SwarmDriver {
    pub(super) async fn run(mut self) {
        self.bootstrap();
        let mut cleanup = interval(PENDING_DIAL_CLEANUP_INTERVAL);
        cleanup.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.shutdown.clone() => {
                    debug!("libp2p swarm driver received shutdown signal");
                    break;
                }
                _ = cleanup.tick() => {
                    self.expire_pending_dials("dial timed out");
                    self.expire_pending_probes("relay probe timed out");
                    self.process_scheduled_dcutr_retries();
                }
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => break,
                    }
                }
                event = self.swarm.select_next_some() => {
                    self.handle_event(event).await;
                }
            }
        }

        for listener in self.reservation_listeners.drain() {
            let _ = self.swarm.remove_listener(listener);
        }
        for (_, pending) in self.pending_dials.drain() {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed("libp2p driver stopped".into())));
        }
        for (_, pending) in self.pending_probes.drain() {
            let _ = pending.respond_to.send(Err(Libp2pError::DialFailed("libp2p driver stopped".into())));
        }
    }
    pub(super) async fn handle_event(&mut self, event: SwarmEvent<Libp2pEvent>) {
        match event {
            SwarmEvent::Behaviour(event) => self.handle_behaviour_event(event).await,
            SwarmEvent::NewListenAddr { address, .. } => self.handle_new_listen_addr_event(address),
            SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, .. } => {
                self.handle_connection_established_event(peer_id, connection_id, endpoint)
            }
            SwarmEvent::ConnectionClosed { peer_id, connection_id, endpoint, .. } => {
                self.handle_connection_closed_event(peer_id, connection_id, endpoint)
            }
            SwarmEvent::OutgoingConnectionError { connection_id, error, .. } => {
                self.handle_outgoing_connection_error_event(connection_id, error.to_string())
            }
            _ => {}
        }
    }
    pub(super) async fn handle_behaviour_event(&mut self, event: Libp2pEvent) {
        match event {
            Libp2pEvent::Stream(event) => self.handle_stream_event(event).await,
            Libp2pEvent::Ping(event) => {
                let _ = event;
            }
            Libp2pEvent::Identify(event) => self.handle_identify_event(event),
            Libp2pEvent::RelayClient(event) => self.handle_relay_client_event(event),
            Libp2pEvent::RelayServer(event) => {
                debug!("libp2p relay server event: {:?}", event);
            }
            Libp2pEvent::Dcutr(event) => self.handle_dcutr_event(event),
            Libp2pEvent::DcutrBootstrap(_) => {}
            Libp2pEvent::Autonat(event) => self.handle_autonat_event(event),
        }
    }
    pub(super) fn handle_identify_event(&mut self, event: identify::Event) {
        match event {
            identify::Event::Received { peer_id, ref info, .. } => {
                let supports_dcutr = info.protocols.iter().any(|p| p.as_ref() == dcutr::PROTOCOL_NAME.as_ref());
                info!(
                    target: "libp2p_identify",
                    "identify received from {peer_id}: protocols={:?} (dcutr={supports_dcutr}) listen_addrs={:?}",
                    info.protocols,
                    info.listen_addrs
                );
                // Only add observed address if it's a valid TCP address (has IP+TCP).
                // Relay circuit peers may report observed addresses like `/p2p/<peer_id>` without
                // any IP information, which are useless for DCUtR hole punching and pollute
                // the external address set.
                self.update_remote_dcutr_candidates(peer_id, &info.listen_addrs);
                let observed_refreshed = self.refresh_local_dcutr_candidates(peer_id, &info.observed_addr);
                // NOTE: We intentionally do NOT add info.listen_addrs as external addresses.
                // Those are the REMOTE peer's addresses, not ours. Adding them would pollute
                // our external address set with unreachable addresses, breaking DCUtR hole punch.
                self.prune_unusable_external_addrs("identify_received");
                self.prune_stale_external_addrs("identify_received");
                self.mark_dcutr_support(peer_id, supports_dcutr);
                if self.active_relay.as_ref().is_some_and(|relay| relay.relay_peer == peer_id) {
                    debug!("libp2p dcutr: skipping dial-back trigger for active relay peer {}", peer_id);
                    return;
                }
                if observed_refreshed {
                    self.request_dialback(peer_id, true, "observed_addr_refresh");
                } else {
                    self.maybe_request_dialback(peer_id);
                }
            }
            identify::Event::Pushed { peer_id, ref info, .. } => {
                let supports_dcutr = info.protocols.iter().any(|p| p.as_ref() == dcutr::PROTOCOL_NAME.as_ref());
                info!(
                    target: "libp2p_identify",
                    "identify pushed to {peer_id}: protocols={:?} (dcutr={supports_dcutr}) listen_addrs={:?}",
                    info.protocols,
                    info.listen_addrs
                );
            }
            identify::Event::Sent { peer_id, .. } => {
                info!(
                    target: "libp2p_identify",
                    "identify sent to {peer_id}; expecting advertisement of {}",
                    dcutr::PROTOCOL_NAME
                );
            }
            other => debug!("libp2p identify event: {:?}", other),
        }
    }
    pub(super) fn handle_dcutr_event(&mut self, event: dcutr::Event) {
        let external_addrs: Vec<_> = self.swarm.external_addresses().collect();
        let local_candidates = self.local_dcutr_candidates().len();
        let remote_candidates = self.peer_states.get(&event.remote_peer_id).map_or(0, |state| state.remote_dcutr_candidates.len());
        let is_spurious_attempts = self
            .connections
            .values()
            .any(|conn| conn.peer_id == event.remote_peer_id && matches!(conn.path, PathKind::Direct) && conn.dcutr_upgraded);
        match &event.result {
            Err(e) if is_dcutr_retry_trigger_error(e) && !(is_attempts_exceeded(e) && is_spurious_attempts) => {
                warn!(
                    "libp2p dcutr retry-trigger failure for {}: {:?} (local_candidates={} remote_candidates={})",
                    event.remote_peer_id, e, local_candidates, remote_candidates
                );
                self.refresh_dcutr_retry_path(event.remote_peer_id, e);
            }
            Err(e) if is_attempts_exceeded(e) && is_spurious_attempts => {
                debug!(
                    "Ignored spurious DCUtR error for connected peer {}: {:?} (swarm has {} external addrs: {:?})",
                    event.remote_peer_id,
                    e,
                    external_addrs.len(),
                    external_addrs
                );
            }
            _ => {
                info!("libp2p dcutr event: {:?} (swarm has {} external addrs: {:?})", event, external_addrs.len(), external_addrs);
            }
        }
        info!(
            "libp2p dcutr summary: peer={} result={:?} local_candidates={} remote_candidates={} upgraded_direct={}",
            event.remote_peer_id, event.result, local_candidates, remote_candidates, is_spurious_attempts
        );
    }
    pub(super) fn handle_autonat_event(&mut self, event: autonat::Event) {
        debug!("libp2p autonat event: {:?}", event);
        let has_external_addr = self.has_usable_external_addr();
        match &event {
            autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Response { .. }) => {
                self.autonat_private_until = None;
            }
            autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Error { error, .. }) => {
                if !self.allow_private_addrs
                    && matches!(error, autonat::OutboundProbeError::Response(autonat::ResponseError::DialError))
                {
                    self.autonat_private_until = Some(Instant::now() + AUTONAT_PRIVATE_COOLDOWN);
                }
            }
            _ => {}
        }
        if let Some(auto_role) = self.auto_role.as_mut() {
            let is_public_probe = matches!(event, autonat::Event::OutboundProbe(autonat::OutboundProbeEvent::Response { .. }));
            if is_public_probe {
                auto_role.record_autonat_public(Instant::now());
            }
            match auto_role.update_role(Instant::now(), has_external_addr) {
                Some(crate::Role::Public) => info!("libp2p autonat: role auto-promoted to public"),
                Some(crate::Role::Private) => info!("libp2p autonat: role auto-demoted to private"),
                _ => {}
            }
        }
    }
    pub(super) fn handle_new_listen_addr_event(&mut self, address: Multiaddr) {
        info!("libp2p listening on {address}");
        if self.active_relay.is_none()
            && let Some(info) = relay_info_from_multiaddr(&address, *self.swarm.local_peer_id())
        {
            self.set_active_relay(info, None);
        }
        if self.is_usable_external_addr(&address) {
            self.record_local_candidate(address.clone(), LocalCandidateSource::Dynamic);
        }
        self.prune_unusable_external_addrs("new_listen_addr");
        self.prune_stale_external_addrs("new_listen_addr");
        self.listening = true;
    }
}
