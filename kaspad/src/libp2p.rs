use clap::ValueEnum;
use kaspa_p2p_lib::{OutboundConnector, TcpConnector};
use kaspa_p2p_libp2p::Libp2pIdentity;
use kaspa_p2p_libp2p::{Config as AdapterConfig, Identity as AdapterIdentity, Libp2pOutboundConnector, Mode as AdapterMode};
use kaspa_rpc_core::{GetLibp2pStatusResponse, RpcLibp2pIdentity, RpcLibp2pMode};
use libp2p::PeerId as Libp2pPeerId;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::sync::Arc;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

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
}

impl Default for Libp2pArgs {
    fn default() -> Self {
        Self { libp2p_mode: Libp2pMode::Off, libp2p_identity_path: None, libp2p_helper_listen: None }
    }
}

/// Translate CLI/config args into the adapter config.
pub fn libp2p_config_from_args(args: &Libp2pArgs, app_dir: &Path) -> AdapterConfig {
    let identity = args
        .libp2p_identity_path
        .as_ref()
        .map(|path| resolve_identity_path(path, app_dir))
        .map(AdapterIdentity::Persisted)
        .unwrap_or(AdapterIdentity::Ephemeral);

    AdapterConfig { mode: AdapterMode::from(args.libp2p_mode).effective(), identity, helper_listen: args.libp2p_helper_listen }
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
}

pub fn libp2p_runtime_from_config(config: &AdapterConfig) -> Libp2pRuntime {
    if config.mode.is_enabled() {
        let (provider, peer_id) = match kaspa_p2p_libp2p::Libp2pIdentity::from_config(config) {
            Ok(identity) => {
                let provider = kaspa_p2p_libp2p::PlaceholderStreamProvider::new(config.clone(), identity.peer_id);
                (provider, Some(identity.peer_id_string()))
            }
            Err(err) => {
                log::warn!("libp2p identity setup failed: {err}; falling back to TCP only");
                let random_peer = Libp2pPeerId::random();
                let provider = kaspa_p2p_libp2p::PlaceholderStreamProvider::new(config.clone(), random_peer);
                (provider, None)
            }
        };
        let outbound = Arc::new(Libp2pOutboundConnector::with_provider(config.clone(), Arc::new(TcpConnector), Arc::new(provider)));
        Libp2pRuntime { outbound, peer_id }
    } else {
        Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None }
    }
}

#[cfg(not(feature = "libp2p"))]
pub fn libp2p_runtime_from_config(_config: &AdapterConfig) -> Libp2pRuntime {
    Libp2pRuntime { outbound: Arc::new(TcpConnector), peer_id: None }
}

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
}
