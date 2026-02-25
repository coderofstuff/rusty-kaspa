use super::*;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct ReservationTarget {
    pub(crate) multiaddr: Multiaddr,
    pub(crate) peer_id: PeerId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DialVia {
    Direct,
    Relay { target_peer: PeerId },
}

pub(crate) fn is_attempts_exceeded(err: &dcutr::Error) -> bool {
    // Upstream error is opaque, relying on Display string for now
    err.to_string().contains("AttemptsExceeded")
}

pub(crate) fn is_dcutr_retry_trigger_error(err: &dcutr::Error) -> bool {
    is_dcutr_retry_trigger_error_text(&err.to_string())
}

pub(crate) fn is_retryable_dcutr_error_text(err: &str) -> bool {
    err.contains("NoAddresses") || err.contains("UnexpectedEof")
}

pub(crate) fn is_dcutr_retry_trigger_error_text(err: &str) -> bool {
    is_retryable_dcutr_error_text(err) || err.contains("AttemptsExceeded")
}

pub(crate) fn local_candidate_priority(source: LocalCandidateSource) -> u8 {
    match source {
        LocalCandidateSource::Observed => 3,
        LocalCandidateSource::Config => 2,
        LocalCandidateSource::Dynamic => 1,
    }
}

pub(crate) fn fallback_old_instant(now: Instant) -> Instant {
    now.checked_sub(Duration::from_secs(24 * 60 * 60)).unwrap_or(now)
}

pub(crate) fn dcutr_retry_jitter(peer_id: PeerId, failures: u8) -> Duration {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    peer_id.hash(&mut hasher);
    failures.hash(&mut hasher);
    Duration::from_millis(hasher.finish() % DCUTR_RETRY_JITTER_MS)
}

#[derive(Clone)]
pub(crate) struct RelayInfo {
    pub(crate) relay_peer: PeerId,
    pub(crate) circuit_base: Multiaddr,
}

#[derive(Default)]
pub(crate) struct PeerState {
    pub(crate) supports_dcutr: bool,
    pub(crate) outgoing: usize,
    pub(crate) connected_via_relay: bool,
    pub(crate) remote_dcutr_candidates: Vec<Multiaddr>,
    pub(crate) remote_candidates_last_seen: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalCandidateSource {
    Config,
    Observed,
    Dynamic,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LocalCandidateMeta {
    pub(crate) source: LocalCandidateSource,
    pub(crate) updated_at: Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct DcutrRetryState {
    pub(crate) failures: u8,
    pub(crate) next_retry_at: Instant,
    pub(crate) last_reason: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionEntry {
    pub(crate) peer_id: PeerId,
    pub(crate) path: PathKind,
    pub(crate) relay_id: Option<String>,
    pub(crate) outbound: bool,
    pub(crate) since: Instant,
    pub(crate) dcutr_upgraded: bool,
}

pub(crate) struct PendingReservation {
    pub(crate) listener_id: ListenerId,
    pub(crate) relay: RelayInfo,
    pub(crate) respond_to: oneshot::Sender<Result<ListenerId, Libp2pError>>,
    pub(crate) started_at: Instant,
}
