//! Libp2p transport adapter placeholder.
//!
//! This crate is feature-gated and intentionally lean; the actual libp2p
//! wiring lives here to keep the core kaspad build free from libp2p deps
//! unless explicitly requested.

/// Marker type for future libp2p components.
#[derive(Debug, Default, Clone)]
pub struct Libp2pAdapter;
