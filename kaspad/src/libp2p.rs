use clap::ValueEnum;
use kaspa_p2p_libp2p::{Config as AdapterConfig, Identity as AdapterIdentity, Mode as AdapterMode};
use kaspa_rpc_core::{GetLibp2pStatusResponse, RpcLibp2pIdentity, RpcLibp2pMode};
use kaspa_utils::networking::PeerId;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
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

pub fn libp2p_status_from_config(config: &AdapterConfig, peer_id: Option<PeerId>) -> GetLibp2pStatusResponse {
    let peer_id = peer_id.map(|id| id.to_string());

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

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
}
