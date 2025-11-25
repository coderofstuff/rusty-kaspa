pub mod pb {
    // this one includes messages.proto + p2p.proto + rcp.proto
    tonic::include_proto!("protowire");
}

// Allow internal modules/tests to refer to the crate by its external name.
extern crate self as kaspa_p2p_lib;

pub mod common;
pub mod convert;
pub mod echo;

mod core;
mod handshake;
pub mod transport;

pub use crate::core::adaptor::{Adaptor, ConnectionInitializer, DirectMetadataFactory};
pub use crate::core::connection_handler::ConnectionError;
pub use crate::core::connection_handler::{ConnectionHandler, MetadataFactory, OutboundConnector, TcpConnector};
pub use crate::core::hub::Hub;
pub use crate::core::payload_type::KaspadMessagePayloadType;
pub use crate::core::peer::{Peer, PeerKey, PeerProperties};
pub use crate::core::router::{BLANK_ROUTE_ID, IncomingRoute, Router, SharedIncomingRoute};
pub use crate::transport::{Capabilities, PathKind, TransportConnector, TransportMetadata};
pub use handshake::KaspadHandshake;
