//! Libp2p transport adapter placeholder.
//!
//! This crate is feature-gated and intentionally lean; the actual libp2p
//! wiring lives here to keep the core kaspad build free from libp2p deps
//! unless explicitly requested.

pub mod config;
pub mod helper_api;
pub mod metadata;
pub mod reservations;
pub mod transport;

pub use config::{Config, Identity, Mode};
