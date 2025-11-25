//! Protobuf-like codecs for a raw byte stream over libp2p. Placeholder for now.

use futures::{AsyncReadExt, AsyncWriteExt};
use libp2p::request_response::{ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec, RequestResponseConfig};
use libp2p::swarm::behaviour::ConnectionClosed;
use libp2p::swarm::NetworkBehaviour;
use libp2p::PeerId;

/// Stream protocol marker.
#[derive(Clone)]
pub struct StreamProtocol;

/// Bytes request.
#[derive(Debug, Clone)]
pub struct StreamRequest(pub Vec<u8>);

/// Bytes response.
#[derive(Debug, Clone)]
pub struct StreamResponse(pub Vec<u8>);

/// Codec for raw bytes.
#[derive(Clone)]
pub struct StreamCodec;

#[async_trait::async_trait]
impl RequestResponseCodec for StreamCodec {
    type Protocol = StreamProtocol;
    type Request = StreamRequest;
    type Response = StreamResponse;

    async fn read_request(
        &mut self,
        _: &StreamProtocol,
        io: &mut libp2p::request_response::NegotiatedRequest,
    ) -> std::io::Result<Self::Request> {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        Ok(StreamRequest(buf))
    }

    async fn read_response(
        &mut self,
        _: &StreamProtocol,
        io: &mut libp2p::request_response::NegotiatedResponse,
    ) -> std::io::Result<Self::Response> {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;
        Ok(StreamResponse(buf))
    }

    async fn write_request(
        &mut self,
        _: &StreamProtocol,
        io: &mut libp2p::request_response::NegotiatedRequest,
        StreamRequest(data): StreamRequest,
    ) -> std::io::Result<()> {
        io.write_all(&data).await?;
        io.close().await
    }

    async fn write_response(
        &mut self,
        _: &StreamProtocol,
        io: &mut libp2p::request_response::NegotiatedResponse,
        StreamResponse(data): StreamResponse,
    ) -> std::io::Result<()> {
        io.write_all(&data).await?;
        io.close().await
    }
}

/// Behaviour wrapping a request-response codec for streams.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "StreamEvent")]
pub struct StreamBehaviour {
    inner: RequestResponse<StreamCodec>,
}

impl Default for StreamBehaviour {
    fn default() -> Self {
        let cfg = RequestResponseConfig::default();
        let protocols = std::iter::once((StreamProtocol, ProtocolSupport::Full));
        Self { inner: RequestResponse::new(StreamCodec, protocols, cfg) }
    }
}

/// Events emitted by StreamBehaviour.
#[allow(clippy::large_enum_variant)]
pub enum StreamEvent {
    Message { peer: PeerId, request: Option<RequestId>, response: Option<RequestId> },
    ResponseSent { peer: PeerId },
    OutboundFailure { peer: PeerId, id: RequestId },
    InboundFailure { peer: PeerId, id: RequestId },
    ConnectionClosed { peer: PeerId, error: Option<ConnectionClosed<'static, ()>> },
}

impl From<libp2p::request_response::Event<StreamRequest, StreamResponse>> for StreamEvent {
    fn from(event: libp2p::request_response::Event<StreamRequest, StreamResponse>) -> Self {
        match event {
            libp2p::request_response::Event::Message { peer, message } => match message {
                libp2p::request_response::Message::Request { request_id, .. } => {
                    StreamEvent::Message { peer, request: Some(request_id), response: None }
                }
                libp2p::request_response::Message::Response { request_id, .. } => {
                    StreamEvent::Message { peer, request: None, response: Some(request_id) }
                }
            },
            libp2p::request_response::Event::ResponseSent { peer, .. } => StreamEvent::ResponseSent { peer },
            libp2p::request_response::Event::OutboundFailure { peer, request_id, .. } => {
                StreamEvent::OutboundFailure { peer, id: request_id }
            }
            libp2p::request_response::Event::InboundFailure { peer, request_id, .. } => {
                StreamEvent::InboundFailure { peer, id: request_id }
            }
            libp2p::request_response::Event::ConnectionClosed { peer, error, .. } => {
                StreamEvent::ConnectionClosed { peer, error }
            }
        }
    }
}
