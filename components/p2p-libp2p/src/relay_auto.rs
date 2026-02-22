use crate::config::{Config, Role};
use crate::relay_pool::{RelayCandidateSource, RelayPool, RelayPoolConfig};
use crate::reservations::ReservationManager;
use crate::transport::{Libp2pStreamProvider, ReservationHandle};
use libp2p::{PeerId, multiaddr::Protocol};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use triggered::Listener;

const AUTO_RELAY_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const AUTO_RELAY_ROTATE_INTERVAL: Duration = Duration::from_secs(45 * 60);
const AUTO_RELAY_BASE_BACKOFF: Duration = Duration::from_secs(10);
const AUTO_RELAY_MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);

struct ActiveReservation {
    handle: ReservationHandle,
    reserved_at: Instant,
    relay_peer_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CandidateLogState {
    Selected,
    Backoff,
    InsufficientSources,
    EligibleNotSelected,
}

pub async fn run_relay_auto_worker(
    provider: Arc<dyn Libp2pStreamProvider>,
    source: Arc<dyn RelayCandidateSource>,
    config: Config,
    mut shutdown: Option<Listener>,
) {
    if !config.mode.is_enabled() {
        return;
    }

    if !matches!(config.role, Role::Private | Role::Auto) {
        return;
    }

    let mut pool_config = RelayPoolConfig::new(config.max_relays, config.max_peers_per_relay);
    pool_config.rotation_interval = AUTO_RELAY_ROTATE_INTERVAL;
    pool_config.backoff_base = AUTO_RELAY_BASE_BACKOFF;
    pool_config.backoff_max = AUTO_RELAY_MAX_BACKOFF;
    pool_config.min_sources = config.relay_min_sources.max(1);
    pool_config.rng_seed = config.relay_rng_seed;
    let min_sources = pool_config.min_sources;
    let mut pool = RelayPool::new(pool_config);
    let mut backoff = ReservationManager::new(AUTO_RELAY_BASE_BACKOFF, AUTO_RELAY_MAX_BACKOFF);
    let mut active: HashMap<String, ActiveReservation> = HashMap::new();
    let mut candidate_log_states: HashMap<String, CandidateLogState> = HashMap::new();
    let metrics = provider.metrics();

    loop {
        let now = Instant::now();
        let candidates = source.fetch_candidates().await;
        let has_candidates = !candidates.is_empty();
        if candidates.is_empty() {
            debug!("libp2p relay auto: no relay candidates available");
        }
        pool.update_candidates(now, candidates);
        pool.prune_expired(now);
        if let Some(metrics) = metrics.as_ref() {
            let stats = pool.candidate_stats(now);
            metrics.relay_auto().set_candidate_counts(stats.total, stats.eligible, stats.high_confidence);
            metrics.relay_auto().set_active_reservations(active.len());
        }
        let connected_peers: HashSet<String> =
            provider.peers_snapshot().await.into_iter().filter(|peer| peer.libp2p).map(|peer| peer.peer_id).collect();
        let mut disconnected = Vec::new();
        for (key, reservation) in active.iter() {
            if let Some(peer_id) = reservation.relay_peer_id.as_deref()
                && !connected_peers.contains(peer_id)
            {
                disconnected.push(key.clone());
            }
        }
        for key in disconnected {
            if let Some(reservation) = active.remove(&key) {
                info!("libp2p relay auto: releasing reservation on {key} (relay disconnected)");
                reservation.handle.release().await;
                backoff.record_failure(&key, now);
                pool.record_failure(&key, now);
                if let Some(metrics) = metrics.as_ref() {
                    metrics.relay_auto().record_rotation();
                    metrics.relay_auto().record_backoff();
                }
            }
        }

        let desired = pool.select_relays(now);
        let desired_keys: HashSet<String> = desired.iter().map(|sel| sel.key.clone()).collect();
        if let Some(metrics) = metrics.as_ref() {
            metrics.relay_auto().record_selection_cycle(desired.len());
        }
        if !desired_keys.is_empty() {
            debug!("libp2p relay auto: selected relays {:?}", desired_keys);
        } else if has_candidates && min_sources > 1 {
            debug!("libp2p relay auto: insufficient multi-source relay candidates (min_sources={})", min_sources);
        }
        let mut observed_candidate_keys: HashSet<String> = HashSet::new();
        for candidate in pool.candidate_observability(now) {
            observed_candidate_keys.insert(candidate.key.clone());
            let state = if desired_keys.contains(&candidate.key) {
                CandidateLogState::Selected
            } else if candidate.in_backoff {
                CandidateLogState::Backoff
            } else if candidate.source_count < min_sources {
                CandidateLogState::InsufficientSources
            } else {
                CandidateLogState::EligibleNotSelected
            };

            if candidate_log_states.get(&candidate.key).copied() == Some(state) {
                continue;
            }
            candidate_log_states.insert(candidate.key.clone(), state);

            match state {
                CandidateLogState::Selected => info!(
                    "libp2p relay auto: candidate {} state=selected sources={}/{} has_peer_id={} score={:.2}",
                    candidate.key, candidate.source_count, min_sources, candidate.has_peer_id, candidate.score
                ),
                CandidateLogState::Backoff => info!(
                    "libp2p relay auto: candidate {} state=backoff sources={}/{} has_peer_id={} score={:.2}",
                    candidate.key, candidate.source_count, min_sources, candidate.has_peer_id, candidate.score
                ),
                CandidateLogState::InsufficientSources => info!(
                    "libp2p relay auto: candidate {} state=insufficient_sources sources={}/{} has_peer_id={} score={:.2}",
                    candidate.key, candidate.source_count, min_sources, candidate.has_peer_id, candidate.score
                ),
                CandidateLogState::EligibleNotSelected => info!(
                    "libp2p relay auto: candidate {} state=eligible_not_selected sources={}/{} has_peer_id={} score={:.2}",
                    candidate.key, candidate.source_count, min_sources, candidate.has_peer_id, candidate.score
                ),
            }
        }
        candidate_log_states.retain(|key, _| observed_candidate_keys.contains(key));

        // Release reservations that are no longer desired or are rotated out.
        let mut released = Vec::new();
        for (key, reservation) in active.iter() {
            let rotate = pool.is_rotation_due(key, now);
            if !desired_keys.contains(key) || rotate {
                released.push((key.clone(), reservation.reserved_at));
            }
        }

        for (key, reserved_at) in released {
            if let Some(reservation) = active.remove(&key) {
                info!("libp2p relay auto: releasing reservation on {key} (age {:?})", now.saturating_duration_since(reserved_at));
                reservation.handle.release().await;
                if let Some(metrics) = metrics.as_ref() {
                    metrics.relay_auto().record_rotation();
                }
            }
        }

        for selection in desired {
            if active.contains_key(&selection.key) {
                continue;
            }

            if !backoff.should_attempt(&selection.key, now) {
                debug!("libp2p relay auto: skipping {} due to backoff", selection.key);
                continue;
            }

            let mut reservation_addr = pool.reservation_multiaddr(&selection.key);
            if reservation_addr.is_none()
                && let Some(probe_addr) = pool.probe_multiaddr(&selection.key)
            {
                if let Some(metrics) = metrics.as_ref() {
                    metrics.relay_auto().record_probe_attempt();
                }
                let probe_started = Instant::now();
                match provider.probe_relay(probe_addr).await {
                    Ok(peer_id) => {
                        pool.set_peer_id(&selection.key, peer_id);
                        let latency = probe_started.elapsed();
                        pool.record_success(&selection.key, Some(latency), now);
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.relay_auto().record_probe_success();
                        }
                        reservation_addr = pool.reservation_multiaddr(&selection.key);
                    }
                    Err(err) => {
                        warn!("libp2p relay auto: probe failed for {}: {err}", selection.key);
                        backoff.record_failure(&selection.key, now);
                        pool.record_failure(&selection.key, now);
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.relay_auto().record_probe_failure();
                            metrics.relay_auto().record_backoff();
                        }
                        continue;
                    }
                }
            }

            let Some(target) = reservation_addr else {
                debug!("libp2p relay auto: missing peer id for {}", selection.key);
                backoff.record_failure(&selection.key, now);
                pool.record_failure(&selection.key, now);
                if let Some(metrics) = metrics.as_ref() {
                    metrics.relay_auto().record_backoff();
                }
                continue;
            };

            let started = Instant::now();
            if let Some(metrics) = metrics.as_ref() {
                metrics.relay_auto().record_reservation_attempt();
            }
            let relay_peer_id = resolve_relay_peer_id(selection.relay_peer_id, &target);
            match provider.reserve(target).await {
                Ok(handle) => {
                    let latency = started.elapsed();
                    info!("libp2p relay auto: reservation accepted for {}", selection.key);
                    backoff.record_success(&selection.key);
                    pool.record_success(&selection.key, Some(latency), now);
                    pool.mark_selected(&selection.key, now);
                    active.insert(selection.key.clone(), ActiveReservation { handle, reserved_at: now, relay_peer_id });
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.relay_auto().record_reservation_success();
                        metrics.relay_auto().set_active_reservations(active.len());
                    }
                }
                Err(err) => {
                    warn!("libp2p relay auto: reservation failed for {}: {err}", selection.key);
                    backoff.record_failure(&selection.key, now);
                    pool.record_failure(&selection.key, now);
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.relay_auto().record_reservation_failure();
                        metrics.relay_auto().record_backoff();
                    }
                }
            }
        }

        if let Some(shutdown) = shutdown.as_mut() {
            tokio::select! {
                _ = shutdown.clone() => {
                    break;
                }
                _ = sleep(AUTO_RELAY_REFRESH_INTERVAL) => {}
            }
        } else {
            sleep(AUTO_RELAY_REFRESH_INTERVAL).await;
        }
    }

    let active_reservations = std::mem::take(&mut active);
    for (_, reservation) in active_reservations {
        reservation.handle.release().await;
    }
}

fn resolve_relay_peer_id(selection_peer_id: Option<PeerId>, reservation_target: &libp2p::Multiaddr) -> Option<String> {
    selection_peer_id
        .or_else(|| {
            reservation_target.iter().find_map(|protocol| match protocol {
                Protocol::P2p(peer_id) => Some(peer_id),
                _ => None,
            })
        })
        .map(|peer_id| peer_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::resolve_relay_peer_id;
    use libp2p::{Multiaddr, PeerId};
    use std::str::FromStr;

    #[test]
    fn resolve_relay_peer_id_uses_selection_when_available() {
        let selected = PeerId::random();
        let from_target = PeerId::random();
        let target = Multiaddr::from_str(&format!("/ip4/10.0.3.26/tcp/16112/p2p/{from_target}")).expect("valid target");

        let resolved = resolve_relay_peer_id(Some(selected), &target);
        assert_eq!(resolved.as_deref(), Some(selected.to_string().as_str()));
    }

    #[test]
    fn resolve_relay_peer_id_falls_back_to_target() {
        let from_target = PeerId::random();
        let target = Multiaddr::from_str(&format!("/ip4/10.0.3.26/tcp/16112/p2p/{from_target}")).expect("valid target");

        let resolved = resolve_relay_peer_id(None, &target);
        assert_eq!(resolved.as_deref(), Some(from_target.to_string().as_str()));
    }
}
