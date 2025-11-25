//! Libp2p transport adapter placeholder.
//!
//! This crate is feature-gated and intentionally lean; the actual libp2p
//! wiring lives here to keep the core kaspad build free from libp2p deps
//! unless explicitly requested.

pub mod config;
pub mod helper_api;
pub mod metadata;
pub mod reservations;
pub mod service;
pub mod swarm;
pub mod transport;

pub use config::{Config, ConfigBuilder, Identity, Mode};
pub use service::Libp2pService;
pub use swarm::{build_base_swarm, BaseBehaviour};
pub use transport::{
    BoxedLibp2pStream, Libp2pConnector, Libp2pError, Libp2pIdentity, Libp2pOutboundConnector, Libp2pStream, Libp2pStreamProvider,
    PlaceholderStreamProvider, SwarmStreamProvider,
};
