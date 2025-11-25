use clap::ValueEnum;
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

impl Libp2pMode {
    pub fn effective(self) -> Self {
        match self {
            Self::Helper => Self::Full,
            mode => mode,
        }
    }

    pub fn is_enabled(self) -> bool {
        !matches!(self.effective(), Self::Off)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentitySource {
    Ephemeral,
    Persisted(PathBuf),
}

impl Default for IdentitySource {
    fn default() -> Self {
        IdentitySource::Ephemeral
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

#[derive(Debug, Clone)]
pub struct Libp2pConfig {
    pub mode: Libp2pMode,
    pub identity: IdentitySource,
    pub helper_listen: Option<SocketAddr>,
}

impl Default for Libp2pConfig {
    fn default() -> Self {
        Self { mode: Libp2pMode::Off, identity: IdentitySource::Ephemeral, helper_listen: None }
    }
}

impl Libp2pConfig {
    pub fn from_args(args: &Libp2pArgs, app_dir: &Path) -> Self {
        let identity = args
            .libp2p_identity_path
            .as_ref()
            .map(|path| resolve_identity_path(path, app_dir))
            .map(IdentitySource::Persisted)
            .unwrap_or_default();

        Self { mode: args.libp2p_mode.effective(), identity, helper_listen: args.libp2p_helper_listen }
    }

    pub fn is_enabled(&self) -> bool {
        self.mode.is_enabled()
    }
}

fn resolve_identity_path(path: &Path, app_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    app_dir.join(path)
}
