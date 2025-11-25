use std::{net::SocketAddr, path::PathBuf};

/// Runtime configuration for the libp2p adapter.
#[derive(Debug, Clone)]
pub struct Config {
    pub mode: Mode,
    pub identity: Identity,
    pub helper_listen: Option<SocketAddr>,
}

impl Default for Config {
    fn default() -> Self {
        Self { mode: Mode::Off, identity: Identity::Ephemeral, helper_listen: None }
    }
}

/// Identity source for the libp2p adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    Ephemeral,
    Persisted(PathBuf),
}

/// Runtime mode for the libp2p adapter.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum Mode {
    #[default]
    Off,
    Full,
    /// Helper-only is currently an alias for full until a narrower mode is defined.
    Helper,
}

impl Mode {
    pub fn effective(self) -> Self {
        match self {
            Mode::Helper => Mode::Full,
            other => other,
        }
    }

    pub fn is_enabled(self) -> bool {
        !matches!(self.effective(), Mode::Off)
    }
}
