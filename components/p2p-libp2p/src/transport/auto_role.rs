use std::collections::VecDeque;

use tokio::sync::watch;
use tokio::time::{Duration, Instant};

pub(super) const AUTO_ROLE_WINDOW: Duration = Duration::from_secs(10 * 60);
pub(super) const AUTO_ROLE_REQUIRED_DIRECT: usize = 1;

pub(super) struct AutoRoleState {
    current: crate::Role,
    window: Duration,
    required_autonat: usize,
    required_direct: usize,
    autonat_public_hits: VecDeque<Instant>,
    direct_inbound_hits: VecDeque<Instant>,
    role_tx: watch::Sender<crate::Role>,
}

impl AutoRoleState {
    pub(super) fn new(role_tx: watch::Sender<crate::Role>, window: Duration, required_autonat: usize, required_direct: usize) -> Self {
        Self {
            current: crate::Role::Private,
            window,
            required_autonat: required_autonat.max(1),
            required_direct: required_direct.max(1),
            autonat_public_hits: VecDeque::new(),
            direct_inbound_hits: VecDeque::new(),
            role_tx,
        }
    }

    pub(super) fn record_autonat_public(&mut self, now: Instant) {
        self.autonat_public_hits.push_back(now);
        self.prune(now);
    }

    pub(super) fn record_direct_inbound(&mut self, now: Instant) {
        self.direct_inbound_hits.push_back(now);
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while self.autonat_public_hits.front().map(|t| now.saturating_duration_since(*t) > self.window).unwrap_or(false) {
            self.autonat_public_hits.pop_front();
        }
        while self.direct_inbound_hits.front().map(|t| now.saturating_duration_since(*t) > self.window).unwrap_or(false) {
            self.direct_inbound_hits.pop_front();
        }
    }

    pub(super) fn update_role(&mut self, now: Instant, has_external_addr: bool) -> Option<crate::Role> {
        self.prune(now);
        let should_be_public = self.autonat_public_hits.len() >= self.required_autonat
            && self.direct_inbound_hits.len() >= self.required_direct
            && has_external_addr;
        if should_be_public && self.current != crate::Role::Public {
            self.current = crate::Role::Public;
            let _ = self.role_tx.send(crate::Role::Public);
            return Some(crate::Role::Public);
        }
        if !should_be_public && self.current != crate::Role::Private {
            self.current = crate::Role::Private;
            let _ = self.role_tx.send(crate::Role::Private);
            return Some(crate::Role::Private);
        }
        None
    }
}
