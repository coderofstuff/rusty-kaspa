use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Libp2pMetrics {
    relay_auto: RelayAutoMetrics,
    dcutr: DcutrMetrics,
}

impl Libp2pMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn relay_auto(&self) -> &RelayAutoMetrics {
        &self.relay_auto
    }

    pub fn dcutr(&self) -> &DcutrMetrics {
        &self.dcutr
    }

    pub fn snapshot(&self) -> Libp2pMetricsSnapshot {
        Libp2pMetricsSnapshot { relay_auto: self.relay_auto.snapshot(), dcutr: self.dcutr.snapshot() }
    }
}

#[derive(Default)]
pub struct RelayAutoMetrics {
    candidates_total: AtomicU64,
    candidates_eligible: AtomicU64,
    candidates_high_confidence: AtomicU64,
    selection_cycles: AtomicU64,
    selected_relays: AtomicU64,
    reservations_active: AtomicU64,
    reservation_attempts: AtomicU64,
    reservation_successes: AtomicU64,
    reservation_failures: AtomicU64,
    probe_attempts: AtomicU64,
    probe_successes: AtomicU64,
    probe_failures: AtomicU64,
    rotations: AtomicU64,
    backoffs: AtomicU64,
}

impl RelayAutoMetrics {
    pub fn set_candidate_counts(&self, total: usize, eligible: usize, high_confidence: usize) {
        self.candidates_total.store(total as u64, Ordering::Relaxed);
        self.candidates_eligible.store(eligible as u64, Ordering::Relaxed);
        self.candidates_high_confidence.store(high_confidence as u64, Ordering::Relaxed);
    }

    pub fn set_active_reservations(&self, active: usize) {
        self.reservations_active.store(active as u64, Ordering::Relaxed);
    }

    pub fn record_selection_cycle(&self, selected: usize) {
        self.selection_cycles.fetch_add(1, Ordering::Relaxed);
        self.selected_relays.fetch_add(selected as u64, Ordering::Relaxed);
    }

    pub fn record_reservation_attempt(&self) {
        self.reservation_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reservation_success(&self) {
        self.reservation_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reservation_failure(&self) {
        self.reservation_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_probe_attempt(&self) {
        self.probe_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_probe_success(&self) {
        self.probe_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_probe_failure(&self) {
        self.probe_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rotation(&self) {
        self.rotations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backoff(&self) {
        self.backoffs.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RelayAutoMetricsSnapshot {
        RelayAutoMetricsSnapshot {
            candidates_total: self.candidates_total.load(Ordering::Relaxed),
            candidates_eligible: self.candidates_eligible.load(Ordering::Relaxed),
            candidates_high_confidence: self.candidates_high_confidence.load(Ordering::Relaxed),
            selection_cycles: self.selection_cycles.load(Ordering::Relaxed),
            selected_relays: self.selected_relays.load(Ordering::Relaxed),
            reservations_active: self.reservations_active.load(Ordering::Relaxed),
            reservation_attempts: self.reservation_attempts.load(Ordering::Relaxed),
            reservation_successes: self.reservation_successes.load(Ordering::Relaxed),
            reservation_failures: self.reservation_failures.load(Ordering::Relaxed),
            probe_attempts: self.probe_attempts.load(Ordering::Relaxed),
            probe_successes: self.probe_successes.load(Ordering::Relaxed),
            probe_failures: self.probe_failures.load(Ordering::Relaxed),
            rotations: self.rotations.load(Ordering::Relaxed),
            backoffs: self.backoffs.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub struct DcutrMetrics {
    dialback_attempts: AtomicU64,
    dialback_successes: AtomicU64,
    dialback_failures: AtomicU64,
    dialback_skipped_private: AtomicU64,
    dialback_skipped_no_external: AtomicU64,
}

impl DcutrMetrics {
    pub fn record_dialback_attempt(&self) {
        self.dialback_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dialback_success(&self) {
        self.dialback_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dialback_failure(&self) {
        self.dialback_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dialback_skipped_private(&self) {
        self.dialback_skipped_private.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dialback_skipped_no_external(&self) {
        self.dialback_skipped_no_external.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DcutrMetricsSnapshot {
        DcutrMetricsSnapshot {
            dialback_attempts: self.dialback_attempts.load(Ordering::Relaxed),
            dialback_successes: self.dialback_successes.load(Ordering::Relaxed),
            dialback_failures: self.dialback_failures.load(Ordering::Relaxed),
            dialback_skipped_private: self.dialback_skipped_private.load(Ordering::Relaxed),
            dialback_skipped_no_external: self.dialback_skipped_no_external.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Libp2pMetricsSnapshot {
    pub relay_auto: RelayAutoMetricsSnapshot,
    pub dcutr: DcutrMetricsSnapshot,
}

#[derive(Clone, Serialize)]
pub struct RelayAutoMetricsSnapshot {
    pub candidates_total: u64,
    pub candidates_eligible: u64,
    pub candidates_high_confidence: u64,
    pub selection_cycles: u64,
    pub selected_relays: u64,
    pub reservations_active: u64,
    pub reservation_attempts: u64,
    pub reservation_successes: u64,
    pub reservation_failures: u64,
    pub probe_attempts: u64,
    pub probe_successes: u64,
    pub probe_failures: u64,
    pub rotations: u64,
    pub backoffs: u64,
}

#[derive(Clone, Serialize)]
pub struct DcutrMetricsSnapshot {
    pub dialback_attempts: u64,
    pub dialback_successes: u64,
    pub dialback_failures: u64,
    pub dialback_skipped_private: u64,
    pub dialback_skipped_no_external: u64,
}
