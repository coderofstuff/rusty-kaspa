use super::*;

#[test]
fn auto_role_promotes_after_signals() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    assert_eq!(state.update_role(now, true), None);

    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);
}

#[test]
fn auto_role_requires_external_addr() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, false), None);
    assert_eq!(*role_rx.borrow(), crate::Role::Private);
}

#[test]
fn auto_role_requires_hysteresis_hits() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, AUTO_ROLE_WINDOW, 2, 2);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), None);

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);
}

#[test]
fn auto_role_demotes_when_window_signal_expires() {
    let (role_tx, role_rx) = watch::channel(crate::Role::Private);
    let mut state = AutoRoleState::new(role_tx, Duration::from_secs(1), 1, 1);
    let now = Instant::now();

    state.record_autonat_public(now);
    state.record_direct_inbound(now);
    assert_eq!(state.update_role(now, true), Some(crate::Role::Public));
    assert_eq!(*role_rx.borrow(), crate::Role::Public);

    let later = now + Duration::from_secs(2);
    assert_eq!(state.update_role(later, true), Some(crate::Role::Private));
    assert_eq!(*role_rx.borrow(), crate::Role::Private);
}

#[test]
fn usable_external_addr_respects_private_setting() {
    let (driver_public, _) = test_driver_with_allow_private(1, false);
    let global: Multiaddr = "/ip4/8.8.8.8/tcp/1234".parse().unwrap();
    let private: Multiaddr = "/ip4/192.168.1.10/tcp/1234".parse().unwrap();
    let loopback: Multiaddr = "/ip4/127.0.0.1/tcp/1234".parse().unwrap();

    assert!(driver_public.is_usable_external_addr(&global));
    assert!(!driver_public.is_usable_external_addr(&private));
    assert!(!driver_public.is_usable_external_addr(&loopback));

    let (driver_private, _) = test_driver_with_allow_private(1, true);
    assert!(driver_private.is_usable_external_addr(&private));
    assert!(!driver_private.is_usable_external_addr(&loopback));
}
