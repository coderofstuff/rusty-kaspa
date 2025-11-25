//! Reservation management (relay/DCUtR) placeholder.
//!
//! This module tracks per-relay backoff to avoid hammering relays on failure.

use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Default, Clone)]
pub struct ReservationManager {
    backoff: HashMap<String, ReservationBackoff>,
    base_delay: Duration,
    max_delay: Duration,
}

impl ReservationManager {
    pub fn new(base_delay: Duration, max_delay: Duration) -> Self {
        Self { backoff: HashMap::new(), base_delay, max_delay }
    }

    pub fn should_attempt(&mut self, relay: &str, now: Instant) -> bool {
        let state = self.backoff.entry(relay.to_string()).or_insert_with(|| ReservationBackoff::new(self.base_delay, self.max_delay));
        state.should_attempt(now)
    }

    pub fn record_failure(&mut self, relay: &str, now: Instant) {
        let state = self.backoff.entry(relay.to_string()).or_insert_with(|| ReservationBackoff::new(self.base_delay, self.max_delay));
        state.record_failure(now);
    }

    pub fn record_success(&mut self, relay: &str) {
        if let Some(state) = self.backoff.get_mut(relay) {
            state.reset();
        }
    }
}

#[derive(Debug, Clone)]
struct ReservationBackoff {
    base: Duration,
    max: Duration,
    current: Duration,
    next_allowed: Instant,
}

impl ReservationBackoff {
    fn new(base: Duration, max: Duration) -> Self {
        Self { base, max, current: Duration::from_secs(0), next_allowed: Instant::now() - Duration::from_secs(86400) }
    }

    fn should_attempt(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }

    fn record_failure(&mut self, now: Instant) {
        if self.current.is_zero() {
            self.current = self.base;
        } else {
            self.current = (self.current * 2).min(self.max);
        }
        self.next_allowed = now + self.current;
    }

    fn reset(&mut self) {
        self.current = Duration::from_secs(0);
        self.next_allowed = Instant::now() - Duration::from_secs(86400);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(30);
        let mut mgr = ReservationManager::new(base, max);
        let now = Instant::now();

        assert!(mgr.should_attempt("r1", now));
        mgr.record_failure("r1", now);
        assert!(!mgr.should_attempt("r1", now + Duration::from_secs(1)));
        assert!(mgr.should_attempt("r1", now + base));

        mgr.record_failure("r1", now + base);
        assert_eq!(mgr.backoff.get("r1").unwrap().current, base * 2);

        mgr.record_failure("r1", now + base * 3);
        mgr.record_failure("r1", now + base * 7);
        assert_eq!(mgr.backoff.get("r1").unwrap().current, max);
    }

    #[test]
    fn backoff_resets_on_success() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(30);
        let mut mgr = ReservationManager::new(base, max);
        let now = Instant::now();

        mgr.record_failure("r1", now);
        assert!(!mgr.should_attempt("r1", now + Duration::from_secs(1)));
        mgr.record_success("r1");
        assert!(mgr.should_attempt("r1", now));
    }
}
