use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use libp2p::Multiaddr;
use log::{debug, warn};
use tokio::time::{Duration, sleep};

use kaspa_p2p_lib::PathKind;

use crate::reservations::ReservationManager;
use crate::transport::{Libp2pError, Libp2pStreamProvider, ReservationHandle, multiaddr_to_metadata};

use triggered::Listener;

const RESERVATION_POLL_INTERVAL: Duration = Duration::from_secs(30);

pub(super) async fn reservation_worker(
    provider: Arc<dyn Libp2pStreamProvider>,
    reservations: Vec<String>,
    mut state: ReservationState,
    mut shutdown: Option<Listener>,
) {
    if reservations.is_empty() {
        return;
    }

    loop {
        let now = Instant::now();
        refresh_reservations(provider.as_ref(), &reservations, &mut state, now).await;

        if let Some(shutdown) = shutdown.as_mut() {
            tokio::select! {
                _ = shutdown.clone() => {
                    debug!("libp2p reservation worker shutting down");
                    state.release_all().await;
                    break;
                }
                _ = sleep(RESERVATION_POLL_INTERVAL) => {}
            }
        } else {
            sleep(RESERVATION_POLL_INTERVAL).await;
        }
    }
}

pub(super) struct ReservationState {
    backoff: ReservationManager,
    active: HashMap<String, ActiveReservation>,
    refresh_interval: Duration,
}

impl ReservationState {
    pub(super) fn new(backoff: ReservationManager, refresh_interval: Duration) -> Self {
        Self { backoff, active: HashMap::new(), refresh_interval }
    }

    async fn release_all(&mut self) {
        let active = std::mem::take(&mut self.active);
        for (_, reservation) in active {
            reservation.handle.release().await;
        }
    }
}

struct ActiveReservation {
    refresh_at: Instant,
    handle: ReservationHandle,
}

pub(super) async fn refresh_reservations(
    provider: &dyn Libp2pStreamProvider,
    reservations: &[String],
    state: &mut ReservationState,
    now: Instant,
) {
    for raw in reservations {
        let parsed = match ParsedReservation::parse(raw) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!("invalid reservation target {raw}: {err}");
                state.backoff.record_failure(raw, now);
                continue;
            }
        };

        let key = parsed.key.clone();

        let refresh_due = match state.active.get(&key) {
            Some(active) => now >= active.refresh_at,
            None => true,
        };

        if !refresh_due {
            continue;
        }

        if !state.backoff.should_attempt(&key, now) {
            continue;
        }

        if let Some(active) = state.active.remove(&key) {
            active.handle.release().await;
        }

        match attempt_reservation(provider, &parsed).await {
            Ok(handle) => {
                state.backoff.record_success(&key);
                let refresh_at = now + state.refresh_interval;
                state.active.insert(key, ActiveReservation { refresh_at, handle });
            }
            Err(err) => {
                warn!("libp2p reservation attempt to {raw} failed: {err}");
                state.backoff.record_failure(&key, now);
            }
        }
    }
}

async fn attempt_reservation(
    provider: &dyn Libp2pStreamProvider,
    target: &ParsedReservation,
) -> Result<ReservationHandle, Libp2pError> {
    let handle = provider.reserve(target.multiaddr.clone()).await?;
    debug!("libp2p reservation attempt to {} via relay {:?}", target.raw, target.key);
    Ok(handle)
}

#[derive(Clone)]
struct ParsedReservation {
    raw: String,
    key: String,
    multiaddr: Multiaddr,
}

impl ParsedReservation {
    fn parse(raw: &str) -> Result<Self, Libp2pError> {
        let multiaddr: Multiaddr = Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))?;
        let (_net, path) = multiaddr_to_metadata(&multiaddr);
        let key = match path {
            PathKind::Relay { relay_id: Some(id) } => id,
            _ => raw.to_string(),
        };

        Ok(Self { raw: raw.to_string(), key, multiaddr })
    }
}
