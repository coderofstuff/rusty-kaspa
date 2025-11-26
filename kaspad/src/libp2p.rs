use clap::ValueEnum;
use kaspa_core::task::service::{AsyncService, AsyncServiceError, AsyncServiceFuture};
#[cfg(feature = "libp2p")]
use kaspa_p2p_flows::flow_context::FlowContext;
use kaspa_p2p_lib::{OutboundConnector, TcpConnector};
use kaspa_p2p_libp2p::SwarmStreamProvider;
use kaspa_p2p_libp2p::{
    Config as AdapterConfig, ConfigBuilder as AdapterConfigBuilder, Identity as AdapterIdentity, Libp2pOutboundConnector,
    Mode as AdapterMode,
};
use kaspa_rpc_core::{GetLibp2pStatusResponse, RpcLibp2pIdentity, RpcLibp2pMode};
#[cfg(feature = "libp2p")]
use kaspa_utils::triggers::SingleTrigger;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::OnceCell;
#[cfg(feature = "libp2p")]
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Libp2pMode {
    #[default]
    Off,
    Full,
    /// Alias for full until a narrower helper-only mode is introduced.
    Helper,
}

impl From<Libp2pMode> for AdapterMode {
    fn from(value: Libp2pMode) -> Self {
        match value {
            Libp2pMode::Off => AdapterMode::Off,
            Libp2pMode::Full => AdapterMode::Full,
            Libp2pMode::Helper => AdapterMode::Helper,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
pub struct Libp2pArgs {
    #[serde(default)]
    pub libp2p_mode: Libp2pMode,
    pub libp2p_identity_path: Option<PathBuf>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub libp2p_helper_listen: Option<SocketAddr>,
    /// Optional dedicated libp2p listen port (defaults to p2p port + 1).
    pub libp2p_listen_port: Option<u16>,
    /// Optional inbound caps for libp2p relay connections (per relay / unknown relay bucket).
    pub libp2p_relay_inbound_cap: Option<usize>,
    pub libp2p_relay_inbound_unknown_cap: Option<usize>,
    /// Relay reservation multiaddrs.
    pub libp2p_reservations: Vec<String>,
    /// External multiaddrs to announce.
    pub libp2p_external_multiaddrs: Vec<String>,
    /// Addresses to advertise (non-libp2p aware).
    pub libp2p_advertise_addresses: Vec<SocketAddr>,
}

impl Default for Libp2pArgs {
    fn default() -> Self {
        Self {
            libp2p_mode: Libp2pMode::Off,
            libp2p_identity_path: None,
            libp2p_helper_listen: None,
            libp2p_listen_port: None,
            libp2p_relay_inbound_cap: None,
            libp2p_relay_inbound_unknown_cap: None,
            libp2p_reservations: Vec::new(),
            libp2p_external_multiaddrs: Vec::new(),
            libp2p_advertise_addresses: Vec::new(),
        }
    }
}

/// Translate CLI/config args into the adapter config.
pub fn libp2p_config_from_args(args: &Libp2pArgs, app_dir: &Path, p2p_listen: SocketAddr) -> AdapterConfig {
    let env_mode = env::var("KASPAD_LIBP2P_MODE").ok().and_then(|s| parse_libp2p_mode(&s));
    let env_identity_path = env::var("KASPAD_LIBP2P_IDENTITY_PATH").ok().map(PathBuf::from);
    let env_helper_listen = env::var("KASPAD_LIBP2P_HELPER_LISTEN").ok().and_then(|s| s.parse::<SocketAddr>().ok());
    let env_listen_port = env::var("KASPAD_LIBP2P_LISTEN_PORT").ok().and_then(|s| s.parse::<u16>().ok());

    let mode = if args.libp2p_mode != Libp2pMode::default() { args.libp2p_mode } else { env_mode.unwrap_or(args.libp2p_mode) };

    let identity_path = args.libp2p_identity_path.clone().or(env_identity_path);
    let helper_listen = args.libp2p_helper_listen.or(env_helper_listen);
    let listen_port = args.libp2p_listen_port.or(env_listen_port).unwrap_or_else(|| p2p_listen.port().saturating_add(1));
    let listen_addr = SocketAddr::new(p2p_listen.ip(), listen_port);
    let relay_inbound_cap =
        args.libp2p_relay_inbound_cap.or(env::var("KASPAD_LIBP2P_RELAY_INBOUND_CAP").ok().and_then(|s| s.parse().ok()));
    let relay_inbound_unknown_cap = args
        .libp2p_relay_inbound_unknown_cap
        .or(env::var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP").ok().and_then(|s| s.parse().ok()));
    let reservations =
        merge_list(&args.libp2p_reservations, env::var("KASPAD_LIBP2P_RESERVATIONS").ok().as_deref(), |s| Some(s.to_string()));
    let external_multiaddrs =
        merge_list(&args.libp2p_external_multiaddrs, env::var("KASPAD_LIBP2P_EXTERNAL_MULTIADDRS").ok().as_deref(), |s| {
            Some(s.to_string())
        });
    let advertise_addresses =
        merge_list(&args.libp2p_advertise_addresses, env::var("KASPAD_LIBP2P_ADVERTISE_ADDRESSES").ok().as_deref(), |s| {
            s.parse::<SocketAddr>().ok()
        });

    let identity = identity_path
        .as_ref()
        .map(|path| resolve_identity_path(path, app_dir))
        .map(AdapterIdentity::Persisted)
        .unwrap_or(AdapterIdentity::Ephemeral);

    AdapterConfigBuilder::new()
        .mode(AdapterMode::from(mode).effective())
        .identity(identity)
        .helper_listen(helper_listen)
        .listen_addresses(vec![listen_addr])
        .relay_inbound_cap(relay_inbound_cap)
        .relay_inbound_unknown_cap(relay_inbound_unknown_cap)
        .reservations(reservations)
        .external_multiaddrs(external_multiaddrs)
        .advertise_addresses(advertise_addresses)
        .build()
}

pub fn libp2p_status_from_config(config: &AdapterConfig, peer_id: Option<String>) -> GetLibp2pStatusResponse {
    let identity = match &config.identity {
        AdapterIdentity::Ephemeral => RpcLibp2pIdentity::Ephemeral,
        AdapterIdentity::Persisted(path) => RpcLibp2pIdentity::Persisted { path: path.display().to_string() },
    };

    let mode = match config.mode.effective() {
        AdapterMode::Off => RpcLibp2pMode::Off,
        AdapterMode::Full => RpcLibp2pMode::Full,
        AdapterMode::Helper => RpcLibp2pMode::Helper,
    };

    GetLibp2pStatusResponse { mode, peer_id, identity }
}

pub struct Libp2pRuntime {
    pub outbound: Arc<dyn OutboundConnector>,
    pub peer_id: Option<String>,
    pub identity: Option<kaspa_p2p_libp2p::Libp2pIdentity>,
    pub(crate) init_service: Option<Arc<Libp2pInitService>>,
    pub(crate) provider_cell: Option<Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>>,
}

pub fn libp2p_runtime_from_config(config: &AdapterConfig) -> Libp2pRuntime {
    if config.mode.is_enabled() {
        match kaspa_p2p_libp2p::Libp2pIdentity::from_config(config) {
            Ok(identity) => {
                let peer_id = Some(identity.peer_id_string());
                let provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>> = Arc::new(OnceCell::new());
                let outbound = Arc::new(Libp2pOutboundConnector::with_provider_cell(
                    config.clone(),
                    Arc::new(TcpConnector),
                    provider_cell.clone(),
                ));
                let init_service = Some(Arc::new(Libp2pInitService {
                    config: config.clone(),
                    identity: identity.clone(),
                    provider_cell: provider_cell.clone(),
                }));
                Libp2pRuntime { outbound, peer_id, identity: Some(identity), init_service, provider_cell: Some(provider_cell.clone()) }
            }
            Err(err) => {
                log::warn!("libp2p identity setup failed: {err}; falling back to TCP only");
                Libp2pRuntime {
                    outbound: Arc::new(TcpConnector),
                    peer_id: None,
                    identity: None,
                    init_service: None,
                    provider_cell: None,
                }
            }
        }
    } else {
        Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None, identity: None, init_service: None, provider_cell: None }
    }
}

#[cfg(not(feature = "libp2p"))]
pub fn libp2p_runtime_from_config(_config: &AdapterConfig) -> Libp2pRuntime {
    Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None, identity: None, init_service: None, provider_cell: None }
}

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
}

fn parse_libp2p_mode(s: &str) -> Option<Libp2pMode> {
    match s.to_ascii_lowercase().as_str() {
        "off" => Some(Libp2pMode::Off),
        "full" => Some(Libp2pMode::Full),
        "helper" => Some(Libp2pMode::Helper),
        _ => None,
    }
}

fn merge_list<T, F: Fn(&str) -> Option<T>>(cli: &[T], env_val: Option<&str>, parse: F) -> Vec<T>
where
    T: Clone,
{
    if !cli.is_empty() {
        return cli.to_vec();
    }
    env_val.map(|s| s.split(',').filter_map(|item| parse(item.trim())).collect()).unwrap_or_default()
}

pub(crate) struct Libp2pInitService {
    config: AdapterConfig,
    identity: kaspa_p2p_libp2p::Libp2pIdentity,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
}

impl AsyncService for Libp2pInitService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-init"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            let handle = tokio::runtime::Handle::current();
            log::info!("libp2p init: listen addresses {:?}", self.config.listen_addresses);
            let provider = match SwarmStreamProvider::with_handle(self.config.clone(), self.identity.clone(), handle) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("libp2p init failed to build provider: {e}");
                    return Err(AsyncServiceError::Service(e.to_string()));
                }
            };
            let _ = self.provider_cell.set(Arc::new(provider));
            log::info!("libp2p init: provider ready");
            Ok(())
        })
    }

    fn signal_exit(self: Arc<Self>) {}

    fn stop(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(feature = "libp2p")]
pub struct Libp2pNodeService {
    config: AdapterConfig,
    provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
    flow_context: Arc<FlowContext>,
    shutdown: SingleTrigger,
}

#[cfg(feature = "libp2p")]
impl Libp2pNodeService {
    pub fn new(
        config: AdapterConfig,
        provider_cell: Arc<OnceCell<Arc<dyn kaspa_p2p_libp2p::Libp2pStreamProvider>>>,
        flow_context: Arc<FlowContext>,
    ) -> Self {
        Self { config, provider_cell, flow_context, shutdown: SingleTrigger::new() }
    }
}

#[cfg(feature = "libp2p")]
impl AsyncService for Libp2pNodeService {
    fn ident(self: Arc<Self>) -> &'static str {
        "libp2p-node"
    }

    fn start(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move {
            if !self.config.mode.is_enabled() {
                return Ok(());
            }
            log::info!("libp2p node service starting; waiting for provider and connection handler");

            let provider = loop {
                if let Some(provider) = self.provider_cell.get() {
                    log::info!("libp2p provider initialised; starting node service");
                    break provider.clone();
                }
                sleep(Duration::from_millis(50)).await;
            };

            let handler = loop {
                if let Some(handler) = self.flow_context.connection_handler() {
                    log::info!("libp2p connection handler available; wiring inbound bridge");
                    break handler;
                }
                sleep(Duration::from_millis(50)).await;
            };

            let svc = kaspa_p2p_libp2p::Libp2pService::with_provider(self.config.clone(), provider);
            svc.start().await.map_err(|e| AsyncServiceError::Service(e.to_string()))?;
            if let Err(err) = svc.start_inbound(Arc::new(handler)).await {
                log::error!("libp2p inbound bridge failed to start: {err}");
                return Err(AsyncServiceError::Service(err.to_string()));
            }
            log::info!("libp2p node service started: bridging libp2p streams into Kaspa");

            self.shutdown.listener.clone().await;
            Ok(())
        })
    }

    fn signal_exit(self: Arc<Self>) {
        self.shutdown.trigger.trigger();
    }

    fn stop(self: Arc<Self>) -> AsyncServiceFuture {
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::AdapterIdentity;
    use super::*;
    use std::env;

    #[test]
    fn libp2p_env_overrides_defaults() {
        env::set_var("KASPAD_LIBP2P_MODE", "full");
        env::set_var("KASPAD_LIBP2P_IDENTITY_PATH", "/tmp/libp2p-id.key");
        env::set_var("KASPAD_LIBP2P_HELPER_LISTEN", "127.0.0.1:12345");
        env::set_var("KASPAD_LIBP2P_RELAY_INBOUND_CAP", "5");
        env::set_var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP", "7");

        let cfg = libp2p_config_from_args(&Libp2pArgs::default(), Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        assert_eq!(cfg.mode, AdapterMode::Full);
        assert!(matches!(cfg.identity, AdapterIdentity::Persisted(ref path) if path.ends_with("libp2p-id.key")));
        assert_eq!(cfg.helper_listen, Some("127.0.0.1:12345".parse().unwrap()));
        assert_eq!(cfg.relay_inbound_cap, Some(5));
        assert_eq!(cfg.relay_inbound_unknown_cap, Some(7));

        env::remove_var("KASPAD_LIBP2P_MODE");
        env::remove_var("KASPAD_LIBP2P_IDENTITY_PATH");
        env::remove_var("KASPAD_LIBP2P_HELPER_LISTEN");
        env::remove_var("KASPAD_LIBP2P_RELAY_INBOUND_CAP");
        env::remove_var("KASPAD_LIBP2P_RELAY_INBOUND_UNKNOWN_CAP");
    }

    #[test]
    fn libp2p_cli_mode_overrides_env() {
        env::set_var("KASPAD_LIBP2P_MODE", "full");
        let mut args = Libp2pArgs::default();
        args.libp2p_mode = Libp2pMode::Helper;

        let cfg = libp2p_config_from_args(&args, Path::new("/tmp/app"), "0.0.0.0:16111".parse().unwrap());
        // Helper aliases to Full in the adapter config (effective mode).
        assert_eq!(cfg.mode, AdapterMode::Full);

        env::remove_var("KASPAD_LIBP2P_MODE");
    }
}
