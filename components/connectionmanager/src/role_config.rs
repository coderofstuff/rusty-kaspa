use std::sync::{OnceLock, RwLock};

const DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE: usize = 8;

#[derive(Clone, Debug)]
pub struct Libp2pRoleConfig {
    pub is_private: bool,
    pub libp2p_inbound_cap_private: usize,
}

static ROLE_CONFIG: OnceLock<RwLock<Libp2pRoleConfig>> = OnceLock::new();

fn role_config() -> &'static RwLock<Libp2pRoleConfig> {
    ROLE_CONFIG.get_or_init(|| {
        RwLock::new(Libp2pRoleConfig { is_private: false, libp2p_inbound_cap_private: DEFAULT_LIBP2P_INBOUND_CAP_PRIVATE })
    })
}

pub fn set_libp2p_role_config(config: Libp2pRoleConfig) {
    *role_config().write().unwrap() = config;
}

pub(crate) fn current_role_config() -> Libp2pRoleConfig {
    role_config().read().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::{Libp2pRoleConfig, current_role_config, set_libp2p_role_config};
    use std::sync::{Mutex, OnceLock};

    static ROLE_CONFIG_TEST_GUARD: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn role_config_updates_are_visible_immediately() {
        let guard = ROLE_CONFIG_TEST_GUARD.get_or_init(|| Mutex::new(())).lock().unwrap();
        let original = current_role_config();

        set_libp2p_role_config(Libp2pRoleConfig { is_private: true, libp2p_inbound_cap_private: 5 });
        let updated_private = current_role_config();
        assert!(updated_private.is_private);
        assert_eq!(updated_private.libp2p_inbound_cap_private, 5);

        set_libp2p_role_config(Libp2pRoleConfig { is_private: false, libp2p_inbound_cap_private: 11 });
        let updated_public = current_role_config();
        assert!(!updated_public.is_private);
        assert_eq!(updated_public.libp2p_inbound_cap_private, 11);

        set_libp2p_role_config(original);
        drop(guard);
    }
}
