impl SwarmDriver {
    pub(super) fn invalidate_dcutr_cached_candidates(&mut self, source: &str) {
        let peers_with_candidates = self.peer_states.values().filter(|state| !state.remote_dcutr_candidates.is_empty()).count();
        for state in self.peer_states.values_mut() {
            state.remote_dcutr_candidates.clear();
            state.remote_candidates_last_seen = None;
        }

        let observed_addrs: Vec<_> = self
            .local_candidate_meta
            .iter()
            .filter(|(_, meta)| matches!(meta.source, LocalCandidateSource::Observed | LocalCandidateSource::Dynamic))
            .map(|(addr, _)| addr.clone())
            .collect();
        for addr in &observed_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }

        self.dialback_cooldowns.clear();
        self.direct_upgrade_cooldowns.clear();
        self.dcutr_retries.clear();

        info!(
            "libp2p dcutr candidate invalidation ({source}): peers_cleared={} local_removed={} local_remaining={}",
            peers_with_candidates,
            observed_addrs.len(),
            self.local_dcutr_candidates().len()
        );
    }
    pub(super) fn process_scheduled_dcutr_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<_> =
            self.dcutr_retries.iter().filter(|(_, state)| state.next_retry_at <= now).map(|(peer_id, _)| *peer_id).collect();

        for peer_id in due {
            let Some(state) = self.dcutr_retries.remove(&peer_id) else {
                continue;
            };
            info!(
                "libp2p dcutr scheduled retry firing for {}: failures={} last_reason={}",
                peer_id, state.failures, state.last_reason
            );
            self.force_identify_refresh(peer_id, "scheduled_retry");
            self.refresh_relay_connection(peer_id, "scheduled_retry");
            self.request_dialback(peer_id, true, "scheduled_retry");
        }
    }
    pub(super) fn schedule_dcutr_retry(&mut self, peer_id: PeerId, reason: &str, count_failure: bool) {
        let now = Instant::now();
        let state = self.dcutr_retries.entry(peer_id).or_insert_with(|| DcutrRetryState {
            failures: 0,
            next_retry_at: now + DCUTR_PREFLIGHT_RETRY_DELAY,
            last_reason: String::new(),
        });

        if count_failure {
            state.failures = state.failures.saturating_add(1);
        }

        let attempt_index =
            if count_failure { usize::from(state.failures.saturating_sub(1)).min(DCUTR_RETRY_BACKOFFS_SECS.len() - 1) } else { 0 };
        let backoff = Duration::from_secs(DCUTR_RETRY_BACKOFFS_SECS[attempt_index]);
        let jitter = dcutr_retry_jitter(peer_id, state.failures);
        let next_retry = now + backoff + jitter;

        if state.next_retry_at <= now || next_retry < state.next_retry_at {
            state.next_retry_at = next_retry;
        }
        state.last_reason = reason.to_string();

        info!(
            "libp2p dcutr retry scheduled for {}: reason={} failures={} next_retry_in_ms={}",
            peer_id,
            reason,
            state.failures,
            state.next_retry_at.saturating_duration_since(now).as_millis()
        );
    }
    pub(super) fn clear_dcutr_retry(&mut self, peer_id: PeerId) {
        if self.dcutr_retries.remove(&peer_id).is_some() {
            debug!("libp2p dcutr retry state cleared for {}", peer_id);
        }
    }
    pub(super) fn force_identify_refresh(&mut self, peer_id: PeerId, reason: &str) {
        if tokio::runtime::Handle::try_current().is_err() {
            debug!("libp2p dcutr identify refresh skipped for {} (reason={}): no tokio runtime", peer_id, reason);
            return;
        }
        self.swarm.behaviour_mut().identify.push([peer_id]);
        info!("libp2p dcutr identify refresh triggered for {} (reason={})", peer_id, reason);
    }
    pub(super) fn refresh_relay_connection(&mut self, peer_id: PeerId, reason: &str) {
        if tokio::runtime::Handle::try_current().is_err() {
            debug!("libp2p dcutr relay refresh skipped for {} (reason={}): no tokio runtime", peer_id, reason);
            return;
        }
        let Some(relay) = &self.active_relay else {
            debug!("libp2p dcutr relay refresh skipped for {}: no active relay (reason={})", peer_id, reason);
            return;
        };

        let relay_addr = relay_probe_base(&relay.circuit_base);
        let opts = DialOpts::peer_id(relay.relay_peer)
            .addresses(vec![relay_addr.clone()])
            .condition(PeerCondition::Disconnected)
            .extend_addresses_through_behaviour()
            .build();
        match self.swarm.dial(opts) {
            Ok(()) => {
                info!(
                    "libp2p dcutr relay refresh dial started for {} via {} using {} (reason={})",
                    peer_id, relay.relay_peer, relay_addr, reason
                );
            }
            Err(err) => {
                warn!(
                    "libp2p dcutr relay refresh dial failed for {} via {} using {} (reason={}): {}",
                    peer_id, relay.relay_peer, relay_addr, reason, err
                );
            }
        }
    }
    pub(super) fn mark_dcutr_support(&mut self, peer_id: PeerId, supports: bool) {
        if supports {
            self.peer_states.entry(peer_id).or_default().supports_dcutr = true;
        }
    }
    pub(super) fn mark_relay_path(&mut self, peer_id: PeerId) {
        self.peer_states.entry(peer_id).or_default().connected_via_relay = true;
    }
    pub(super) fn update_remote_dcutr_candidates(&mut self, peer_id: PeerId, listen_addrs: &[Multiaddr]) {
        let remote_dcutr_candidates = extract_remote_dcutr_candidates(listen_addrs, self.allow_private_addrs);
        let remote_count = remote_dcutr_candidates.len();
        let state = self.peer_states.entry(peer_id).or_default();
        state.remote_dcutr_candidates = remote_dcutr_candidates;
        state.remote_candidates_last_seen = Some(Instant::now());
        info!(
            "libp2p dcutr candidates refreshed from identify for {peer_id}: local_candidates={} remote_candidates={remote_count}",
            self.local_dcutr_candidates().len()
        );
    }
    pub(super) fn local_dcutr_candidates(&self) -> Vec<Multiaddr> {
        let now = Instant::now();
        let mut candidates: Vec<_> =
            self.swarm.external_addresses().filter(|addr| self.is_usable_external_addr(addr)).cloned().collect();
        candidates.sort_by_key(|addr| {
            let meta = self
                .local_candidate_meta
                .get(addr)
                .copied()
                .unwrap_or(LocalCandidateMeta { source: LocalCandidateSource::Dynamic, updated_at: fallback_old_instant(now) });
            (
                std::cmp::Reverse(local_candidate_priority(meta.source)),
                now.saturating_duration_since(meta.updated_at),
                addr.to_string(),
            )
        });
        candidates.dedup();
        candidates
    }
    pub(super) fn prune_unusable_external_addrs(&mut self, source: &str) {
        let stale_addrs: Vec<_> =
            self.swarm.external_addresses().filter(|addr| !self.is_usable_external_addr(addr)).cloned().collect();
        if stale_addrs.is_empty() {
            return;
        }
        for addr in &stale_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }
        info!(
            "libp2p dcutr candidate prune ({source}): removed={} remaining_usable={} removed_addrs={:?}",
            stale_addrs.len(),
            self.local_dcutr_candidates().len(),
            stale_addrs
        );
    }
    pub(super) fn prune_stale_external_addrs(&mut self, source: &str) {
        let now = Instant::now();
        let stale_addrs: Vec<_> = self
            .local_candidate_meta
            .iter()
            .filter_map(|(addr, meta)| {
                let max_age = match meta.source {
                    LocalCandidateSource::Config => return None,
                    LocalCandidateSource::Observed => DCUTR_OBSERVED_CANDIDATE_TTL,
                    LocalCandidateSource::Dynamic => DCUTR_DYNAMIC_CANDIDATE_TTL,
                };
                if now.saturating_duration_since(meta.updated_at) > max_age { Some(addr.clone()) } else { None }
            })
            .collect();
        if stale_addrs.is_empty() {
            return;
        }
        for addr in &stale_addrs {
            self.swarm.remove_external_address(addr);
            self.local_candidate_meta.remove(addr);
        }
        info!(
            "libp2p dcutr stale candidate prune ({source}): removed={} remaining_usable={} removed_addrs={:?}",
            stale_addrs.len(),
            self.local_dcutr_candidates().len(),
            stale_addrs
        );
    }
    pub(super) fn record_local_candidate(&mut self, addr: Multiaddr, source: LocalCandidateSource) {
        if matches!(source, LocalCandidateSource::Observed)
            && let Some(candidate_ip) = candidate_ip_addr(&addr)
        {
            let has_config_same_ip = self.local_candidate_meta.iter().any(|(existing, meta)| {
                meta.source == LocalCandidateSource::Config
                    && candidate_ip_addr(existing).is_some_and(|existing_ip| existing_ip == candidate_ip)
            });
            if has_config_same_ip {
                // If observed equals the configured candidate exactly, keep config
                // authoritative and ignore the observed duplicate.
                let is_exact_config =
                    matches!(self.local_candidate_meta.get(&addr).map(|meta| meta.source), Some(LocalCandidateSource::Config));
                if is_exact_config {
                    info!(
                        "libp2p dcutr local candidate ignored: addr={} source={:?} reason=config_exact_addr local_candidates={}",
                        addr,
                        source,
                        self.local_dcutr_candidates().len()
                    );
                    return;
                }
            }

            // Keep one observed candidate per IP so frequent observed port updates
            // cannot grow the local candidate set without bound.
            let stale_same_ip: Vec<_> = self
                .local_candidate_meta
                .iter()
                .filter_map(|(existing, meta)| {
                    if existing != &addr
                        && meta.source == LocalCandidateSource::Observed
                        && candidate_ip_addr(existing).is_some_and(|existing_ip| existing_ip == candidate_ip)
                    {
                        Some(existing.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for stale in stale_same_ip {
                self.swarm.remove_external_address(&stale);
                self.local_candidate_meta.remove(&stale);
            }
        }
        let replaced = self.local_candidate_meta.get(&addr).map(|meta| meta.source);
        self.local_candidate_meta.insert(addr.clone(), LocalCandidateMeta { source, updated_at: Instant::now() });
        info!(
            "libp2p dcutr local candidate recorded: addr={} source={:?} replaced={:?} local_candidates={}",
            addr,
            source,
            replaced,
            self.local_dcutr_candidates().len()
        );
    }
    pub(super) fn has_config_local_candidate(&self) -> bool {
        self.local_candidate_meta
            .iter()
            .any(|(addr, meta)| matches!(meta.source, LocalCandidateSource::Config) && self.is_usable_external_addr(addr))
    }
    pub(super) fn has_fresh_local_observed_candidate(&self, now: Instant) -> bool {
        self.local_candidate_meta.iter().any(|(addr, meta)| {
            matches!(meta.source, LocalCandidateSource::Observed)
                && self.is_usable_external_addr(addr)
                && now.saturating_duration_since(meta.updated_at) <= DCUTR_LOCAL_OBSERVED_FRESHNESS
        })
    }
    pub(super) fn refresh_local_dcutr_candidates(&mut self, peer_id: PeerId, observed_addr: &Multiaddr) -> bool {
        if !self.is_usable_external_addr(observed_addr) {
            return false;
        }
        let now = Instant::now();
        let was_fresh = self
            .local_candidate_meta
            .get(observed_addr)
            .is_some_and(|meta| now.saturating_duration_since(meta.updated_at) <= Duration::from_secs(1));
        self.swarm.add_external_address(observed_addr.clone());
        self.record_local_candidate(observed_addr.clone(), LocalCandidateSource::Observed);
        // Force immediate re-attempt for this peer when a fresh observed address appears.
        self.dialback_cooldowns.remove(&peer_id);
        self.direct_upgrade_cooldowns.remove(&peer_id);
        let remote_count = self.peer_states.get(&peer_id).map_or(0, |state| state.remote_dcutr_candidates.len());
        info!(
            "libp2p dcutr local candidate refresh for {peer_id}: observed_addr={} local_candidates={} remote_candidates={remote_count}",
            observed_addr,
            self.local_dcutr_candidates().len()
        );
        !was_fresh
    }
    pub(super) fn refresh_dcutr_retry_path(&mut self, peer_id: PeerId, error: &dcutr::Error) {
        let relay_connections =
            self.connections.values().filter(|conn| conn.peer_id == peer_id && matches!(conn.path, PathKind::Relay { .. })).count();
        self.mark_relay_path(peer_id);
        self.dialback_cooldowns.remove(&peer_id);
        self.direct_upgrade_cooldowns.remove(&peer_id);
        info!(
            "libp2p dcutr retry-path refresh for {peer_id}: error={error} relay_connections={relay_connections} local_candidates={} remote_candidates={}",
            self.local_dcutr_candidates().len(),
            self.peer_states.get(&peer_id).map_or(0, |state| state.remote_dcutr_candidates.len())
        );
        self.force_identify_refresh(peer_id, "dcutr_error");
        self.refresh_relay_connection(peer_id, "dcutr_error");
        self.schedule_dcutr_retry(peer_id, &error.to_string(), true);
        self.request_dialback(peer_id, true, "retryable_dcutr_error");
    }
    pub(super) fn maybe_request_dialback(&mut self, peer_id: PeerId) {
        self.request_dialback(peer_id, false, "standard");
    }
    pub(super) fn request_dialback(&mut self, peer_id: PeerId, force: bool, reason: &str) {
        self.prune_unusable_external_addrs("request_dialback");
        self.prune_stale_external_addrs("request_dialback");
        if self.active_relay.as_ref().is_some_and(|relay| relay.relay_peer == peer_id) {
            self.clear_dcutr_retry(peer_id);
            debug!("libp2p dcutr: skipping dial-back to active relay peer {}", peer_id);
            return;
        }
        if force {
            let has_relay_connection = self.has_relay_connection(peer_id);
            let state = self.peer_states.entry(peer_id).or_default();
            state.supports_dcutr = true;
            if has_relay_connection {
                state.connected_via_relay = true;
            }
        }

        let Some(state) = self.peer_states.get(&peer_id) else {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no peer state");
            return;
        };
        let supports_dcutr = state.supports_dcutr;
        let connected_via_relay = state.connected_via_relay;
        let outgoing = state.outgoing;
        let remote_candidates_count = state.remote_dcutr_candidates.len();
        let remote_candidates_last_seen = state.remote_candidates_last_seen;

        if !supports_dcutr {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: peer does not support dcutr");
            return;
        }
        if !connected_via_relay {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: not connected via relay");
            return;
        }
        if !force && outgoing > 0 {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: already have outgoing connection");
            return;
        }
        if !self.has_relay_connection(peer_id) {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no active relay circuit connection");
            self.force_identify_refresh(peer_id, "missing_relay_circuit");
            self.refresh_relay_connection(peer_id, "missing_relay_circuit");
            self.schedule_dcutr_retry(peer_id, "missing_relay_circuit", false);
            return;
        }

        let now = Instant::now();
        if !self.allow_private_addrs
            && let Some(until) = self.autonat_private_until
        {
            if until > now {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.dcutr().record_dialback_skipped_private();
                }
                debug!("libp2p dcutr: skipping dial-back to {peer_id}: autonat private until {:?}", until);
                return;
            }
            self.autonat_private_until = None;
        }
        if !force
            && let Some(next_allowed) = self.dialback_cooldowns.get(&peer_id)
            && *next_allowed > now
        {
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: cooldown until {:?}", *next_allowed);
            return;
        }
        if let Some(until) = self.direct_upgrade_cooldowns.get(&peer_id).copied() {
            if until > now {
                if force {
                    self.direct_upgrade_cooldowns.remove(&peer_id);
                } else {
                    debug!("libp2p dcutr: skipping dial-back to {peer_id}: direct-upgrade cooldown until {:?}", until);
                    return;
                }
            } else {
                self.direct_upgrade_cooldowns.remove(&peer_id);
            }
        }

        let local_has_fresh_observed = self.has_fresh_local_observed_candidate(now);
        let local_has_config_candidate = self.has_config_local_candidate();
        let local_candidate_ready = local_has_fresh_observed || local_has_config_candidate;
        let remote_is_fresh =
            remote_candidates_last_seen.is_some_and(|seen| now.saturating_duration_since(seen) <= DCUTR_REMOTE_CANDIDATE_FRESHNESS);
        if !local_candidate_ready || remote_candidates_count == 0 || !remote_is_fresh {
            info!(
                "libp2p dcutr preflight defer for {}: reason={} local_fresh_observed={} local_has_config={} local_ready={} local_candidates={} remote_candidates={} remote_fresh={}",
                peer_id,
                reason,
                local_has_fresh_observed,
                local_has_config_candidate,
                local_candidate_ready,
                self.local_dcutr_candidates().len(),
                remote_candidates_count,
                remote_is_fresh
            );
            self.force_identify_refresh(peer_id, "preflight_defer");
            self.refresh_relay_connection(peer_id, "preflight_defer");
            self.schedule_dcutr_retry(peer_id, "preflight_defer", false);
            return;
        }

        let local_candidates = self.local_dcutr_candidates();
        if local_candidates.is_empty() {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.dcutr().record_dialback_skipped_no_external();
            }
            debug!("libp2p dcutr: skipping dial-back to {peer_id}: no usable external address");
            self.schedule_dcutr_retry(peer_id, "no_usable_local_candidates", false);
            return;
        }

        info!(
            "libp2p dcutr preflight pass for {}: reason={} force={} local_fresh_observed={} local_has_config={} local_candidates={} remote_candidates={} remote_fresh={}",
            peer_id,
            reason,
            force,
            local_has_fresh_observed,
            local_has_config_candidate,
            local_candidates.len(),
            remote_candidates_count,
            remote_is_fresh
        );

        let Some(relay) = &self.active_relay else {
            debug!("libp2p dcutr: no active relay available for dial-back to {peer_id}");
            self.schedule_dcutr_retry(peer_id, "no_active_relay", false);
            return;
        };
        let relay_peer = relay.relay_peer;

        let mut circuit_addr = relay.circuit_base.clone();
        strip_peer_suffix(&mut circuit_addr, *self.swarm.local_peer_id());
        if !circuit_addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
            circuit_addr.push(Protocol::P2pCircuit);
        }
        circuit_addr.push(Protocol::P2p(peer_id));
        let chosen_dial_addrs = vec![circuit_addr.clone()];

        info!(
            "libp2p dcutr dial-back attempt to {peer_id} reason={reason} relay_peer={} circuit_base={} local_candidates={} remote_candidates={} local_candidate_addrs={:?} chosen_dial_addrs={:?}",
            relay_peer,
            relay.circuit_base,
            local_candidates.len(),
            remote_candidates_count,
            local_candidates,
            chosen_dial_addrs
        );

        let opts = DialOpts::peer_id(peer_id)
            .addresses(chosen_dial_addrs)
            .condition(PeerCondition::Always)
            .extend_addresses_through_behaviour()
            .build();

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.dcutr().record_dialback_attempt();
        }
        match self.swarm.dial(opts) {
            Ok(()) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                self.clear_dcutr_retry(peer_id);
                info!("libp2p dcutr: initiated dial-back to {peer_id} via relay {}", relay_peer);
            }
            Err(err) => {
                self.dialback_cooldowns.insert(peer_id, now + DIALBACK_COOLDOWN);
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.dcutr().record_dialback_failure();
                }
                warn!("libp2p dcutr: failed to dial {peer_id} via relay {}: {err}", relay_peer);
                self.force_identify_refresh(peer_id, "dialback_dial_error");
                self.refresh_relay_connection(peer_id, "dialback_dial_error");
                self.schedule_dcutr_retry(peer_id, "dialback_dial_error", true);
            }
        }
    }
}
