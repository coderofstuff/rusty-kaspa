use crate::config::{Config, Role};
use crate::relay_pool::{RelayCandidateSource, RelayPool, RelayPoolConfig};
use crate::reservations::ReservationManager;
use crate::transport::{Libp2pStreamProvider, ReservationHandle};
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
            if let Some(peer_id) = reservation.relay_peer_id.as_deref() {
                if !connected_peers.contains(peer_id) {
                    disconnected.push(key.clone());
                }
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
            if reservation_addr.is_none() {
                if let Some(probe_addr) = pool.probe_multiaddr(&selection.key) {
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
            match provider.reserve(target).await {
                Ok(handle) => {
                    let latency = started.elapsed();
                    info!("libp2p relay auto: reservation accepted for {}", selection.key);
                    backoff.record_success(&selection.key);
                    pool.record_success(&selection.key, Some(latency), now);
                    pool.mark_selected(&selection.key, now);
                    active.insert(
                        selection.key.clone(),
                        ActiveReservation {
                            handle,
                            reserved_at: now,
                            relay_peer_id: selection.relay_peer_id.map(|id| id.to_string()),
                        },
                    );
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
