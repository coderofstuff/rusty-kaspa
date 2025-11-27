use crate::transport::{Libp2pError, Libp2pIdentity};
use crate::config::Config;
use futures_util::future;
use libp2p::core::transport::choice::OrTransport;
use libp2p::core::upgrade::{self, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::identify;
use libp2p::noise;
use libp2p::ping;
use libp2p::relay::{self, client as relay_client};
use libp2p::swarm::behaviour::ToSwarm;
use libp2p::swarm::handler::{OneShotHandler, StreamUpgradeError};
use libp2p::swarm::THandlerInEvent;
use libp2p::swarm::{ConnectionId, NetworkBehaviour, NotifyHandler, Stream, StreamProtocol, SubstreamProtocol};
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::yamux;
use libp2p::{identity, swarm, Swarm, Transport};
use log::info;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::iter;
use std::task::{Context, Poll};
use std::time::Duration;

type BoxedTransport = libp2p::core::transport::Boxed<(libp2p::PeerId, libp2p::core::muxing::StreamMuxerBox)>;

/// Minimal libp2p behaviour (Ping only) to validate swarm construction.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "PingEvent")]
pub struct BaseBehaviour {
    pub ping: ping::Behaviour,
}

#[allow(dead_code)]
pub enum PingEvent {
    Ping,
}

impl From<ping::Event> for PingEvent {
    fn from(_: ping::Event) -> Self {
        PingEvent::Ping
    }
}

/// Build a baseline libp2p swarm (TCP + Noise + Yamux + Ping).
pub fn build_base_swarm(identity: &Libp2pIdentity) -> Result<Swarm<BaseBehaviour>, Libp2pError> {
    let peer_id = identity.peer_id;
    let (transport, cfg) = build_tcp_transport(identity)?;

    info!("libp2p base swarm peer id: {}", peer_id);

    Ok(Swarm::new(transport, BaseBehaviour { ping: ping::Behaviour::default() }, peer_id, cfg))
}

/// Libp2p behaviour that includes ping and a raw stream channel for kaspad.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "Libp2pEvent")]
pub(crate) struct Libp2pBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub relay_client: relay_client::Behaviour,
    pub relay_server: relay::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::Behaviour,
    pub streams: StreamBehaviour,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Libp2pEvent {
    Ping(PingEvent),
    Identify(identify::Event),
    RelayClient(relay_client::Event),
    RelayServer(relay::Event),
    Dcutr(dcutr::Event),
    Autonat(autonat::Event),
    Stream(StreamEvent),
}

impl From<PingEvent> for Libp2pEvent {
    fn from(event: PingEvent) -> Self {
        Libp2pEvent::Ping(event)
    }
}

impl From<identify::Event> for Libp2pEvent {
    fn from(event: identify::Event) -> Self {
        Libp2pEvent::Identify(event)
    }
}

impl From<relay_client::Event> for Libp2pEvent {
    fn from(event: relay_client::Event) -> Self {
        Libp2pEvent::RelayClient(event)
    }
}

impl From<relay::Event> for Libp2pEvent {
    fn from(event: relay::Event) -> Self {
        Libp2pEvent::RelayServer(event)
    }
}

impl From<dcutr::Event> for Libp2pEvent {
    fn from(event: dcutr::Event) -> Self {
        Libp2pEvent::Dcutr(event)
    }
}

impl From<autonat::Event> for Libp2pEvent {
    fn from(event: autonat::Event) -> Self {
        Libp2pEvent::Autonat(event)
    }
}

impl From<ping::Event> for Libp2pEvent {
    fn from(event: ping::Event) -> Self {
        Libp2pEvent::Ping(PingEvent::from(event))
    }
}

impl From<StreamEvent> for Libp2pEvent {
    fn from(event: StreamEvent) -> Self {
        Libp2pEvent::Stream(event)
    }
}

/// Build a libp2p swarm with ping + raw stream support.
pub(crate) fn build_streaming_swarm(
    identity: &Libp2pIdentity,
    config: &Config,
    protocol: StreamProtocol,
) -> Result<Swarm<Libp2pBehaviour>, Libp2pError> {
    let peer_id = identity.peer_id;
    let (relay_transport, relay_client_behaviour) = relay_client::new(peer_id);
    let (transport, cfg) = build_transport(identity, relay_transport)?;

    info!("libp2p streaming swarm peer id: {} (dcutr enabled, identify will advertise {})", peer_id, dcutr::PROTOCOL_NAME);

    // Configure AutoNAT
    let mut autonat_cfg = autonat::Config::default();
    if config.autonat.enable_client {
        autonat_cfg.confidence_max = config.autonat.confidence_threshold;
        info!("AutoNAT client mode ENABLED for peer={}", peer_id);
    }
    if config.autonat.enable_server {
        autonat_cfg.only_global_ips = config.autonat.server_only_if_public;
        autonat_cfg.throttle_server_period = Duration::from_secs(60);
        autonat_cfg.throttle_clients_peer_max = config.autonat.max_server_requests_per_peer;
        info!("AutoNAT server mode ENABLED for peer={}", peer_id);
    }
    if !config.autonat.enable_client && !config.autonat.enable_server {
        info!("AutoNAT DISABLED for peer={}", peer_id);
    }
    let autonat = autonat::Behaviour::new(peer_id, autonat_cfg);

    let identify = {
        let identify_cfg =
            identify::Config::new(format!("/kaspad/libp2p/{}", env!("CARGO_PKG_VERSION")), identity.keypair.public())
                .with_push_listen_addr_updates(false); // Disabled to prevent relay flooding
        info!("Identify: push listen addr updates = false");
        let behaviour = identify::Behaviour::new(identify_cfg);
        // Developer sanity: ensure DCUtR is in the supported protocol set we hand to Identify.
        debug_assert!(!dcutr::PROTOCOL_NAME.as_ref().is_empty(), "dcutr protocol name must be non-empty");
        behaviour
    };

    let behaviour = Libp2pBehaviour {
        ping: ping::Behaviour::default(),
        identify,
        relay_client: relay_client_behaviour,
        relay_server: relay::Behaviour::new(peer_id, relay::Config::default()),
        dcutr: dcutr::Behaviour::new(peer_id),
        autonat,
        streams: StreamBehaviour::new(protocol),
    };
    Ok(Swarm::new(transport, behaviour, peer_id, cfg))
}

fn build_transport(
    identity: &Libp2pIdentity,
    relay_transport: relay_client::Transport,
) -> Result<(BoxedTransport, swarm::Config), Libp2pError> {
    let local_key: identity::Keypair = identity.keypair.clone();
    let noise_keys = noise::Config::new(&local_key).map_err(|e| Libp2pError::Identity(e.to_string()))?;

    // Configure yamux with large buffers to support high throughput (e.g. block sync) over hole-punched links
    let yamux_config = {
        let mut cfg = yamux::Config::default();
        cfg.set_receive_window_size(32 * 1024 * 1024); // 32 MiB
        cfg.set_max_buffer_size(32 * 1024 * 1024);
        cfg
    };

    let tcp = TcpTransport::new(libp2p::tcp::Config::default().nodelay(true));
    let transport = OrTransport::new(tcp, relay_transport)
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise_keys)
        .multiplex(yamux_config)
        .boxed();

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Ok((transport, cfg))
}

fn build_tcp_transport(identity: &Libp2pIdentity) -> Result<(BoxedTransport, swarm::Config), Libp2pError> {
    let local_key: identity::Keypair = identity.keypair.clone();
    let noise_keys = noise::Config::new(&local_key).map_err(|e| Libp2pError::Identity(e.to_string()))?;

    let yamux_config = {
        let mut cfg = yamux::Config::default();
        cfg.set_receive_window_size(32 * 1024 * 1024); // 32 MiB
        cfg.set_max_buffer_size(32 * 1024 * 1024);
        cfg
    };

    let tcp = TcpTransport::new(libp2p::tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise_keys)
        .multiplex(yamux_config)
        .boxed();

    let cfg = libp2p::swarm::Config::with_tokio_executor();
    Ok((tcp, cfg))
}

/// Unique identifier used to correlate outbound stream requests.
pub(crate) type StreamRequestId = ConnectionId;

/// Event emitted by the stream behaviour when a substream is negotiated.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    Inbound {
        peer_id: libp2p::PeerId,
        _connection_id: ConnectionId,
        endpoint: libp2p::core::ConnectedPoint,
        stream: Stream,
    },
    Outbound {
        peer_id: libp2p::PeerId,
        _connection_id: ConnectionId,
        request_id: StreamRequestId,
        endpoint: libp2p::core::ConnectedPoint,
        stream: Stream,
    },
}

#[derive(Debug)]
pub(crate) struct StreamBehaviour {
    protocol: StreamProtocol,
    pending_events: VecDeque<StreamEvent>,
    pending_requests: VecDeque<StreamBehaviourAction>,
    connections: HashMap<ConnectionId, libp2p::core::ConnectedPoint>,
}

#[derive(Debug)]
struct StreamBehaviourAction {
    peer_id: libp2p::PeerId,
    connection_id: ConnectionId,
    request_id: StreamRequestId,
}

impl StreamBehaviour {
    pub fn new(protocol: StreamProtocol) -> Self {
        Self { protocol, pending_events: VecDeque::new(), pending_requests: VecDeque::new(), connections: HashMap::new() }
    }

    pub fn request_stream(&mut self, peer_id: libp2p::PeerId, connection_id: ConnectionId, request_id: StreamRequestId) {
        self.pending_requests.push_back(StreamBehaviourAction { peer_id, connection_id, request_id });
    }

    fn new_handler(&self) -> StreamHandler {
        let inbound = StreamInboundUpgrade { protocol: self.protocol.clone() };
        OneShotHandler::new(SubstreamProtocol::new(inbound, ()), Default::default())
    }
}

impl NetworkBehaviour for StreamBehaviour {
    type ConnectionHandler = StreamHandler;
    type ToSwarm = StreamEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _peer: libp2p::PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<Self::ConnectionHandler, swarm::ConnectionDenied> {
        let handler = self.new_handler();
        self.connections.insert(
            connection_id,
            libp2p::core::ConnectedPoint::Listener { local_addr: local_addr.clone(), send_back_addr: remote_addr.clone() },
        );
        Ok(handler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _peer: libp2p::PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
        port_use: libp2p::core::transport::PortUse,
    ) -> Result<Self::ConnectionHandler, swarm::ConnectionDenied> {
        let handler = self.new_handler();
        let endpoint = libp2p::core::ConnectedPoint::Dialer { address: addr.clone(), role_override, port_use };
        self.connections.insert(connection_id, endpoint);
        Ok(handler)
    }

    fn on_swarm_event(&mut self, event: swarm::behaviour::FromSwarm) {
        match event {
            swarm::behaviour::FromSwarm::ConnectionEstablished(event) => {
                self.connections.insert(event.connection_id, event.endpoint.clone());
            }
            swarm::behaviour::FromSwarm::AddressChange(event) => {
                self.connections.insert(event.connection_id, event.new.clone());
            }
            swarm::behaviour::FromSwarm::ConnectionClosed(event) => {
                self.connections.remove(&event.connection_id);
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: ConnectionId,
        event: Result<StreamHandlerEvent, StreamUpgradeError<Infallible>>
    ) {
        let event = match event {
            Ok(ev) => ev,
            Err(e) => {
                log::warn!("libp2p stream upgrade failed: {e}");
                return;
            }
        };

        let Some(endpoint) = self.connections.get(&connection_id).cloned() else { return };

        match event {
            StreamHandlerEvent::Inbound(stream) => {
                self.pending_events.push_back(StreamEvent::Inbound { peer_id, _connection_id: connection_id, endpoint, stream });
            }
            StreamHandlerEvent::Outbound { request_id, stream } => {
                self.pending_events.push_back(StreamEvent::Outbound {
                    peer_id,
                    _connection_id: connection_id,
                    request_id,
                    endpoint,
                    stream,
                });
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }

        if let Some(req) = self.pending_requests.pop_front() {
            let upgrade = StreamOutboundUpgrade { protocol: self.protocol.clone(), request_id: req.request_id };
            return Poll::Ready(ToSwarm::NotifyHandler {
                peer_id: req.peer_id,
                handler: NotifyHandler::One(req.connection_id),
                event: upgrade,
            });
        }

        Poll::Pending
    }
}

pub(crate) type StreamHandler = OneShotHandler<StreamInboundUpgrade, StreamOutboundUpgrade, StreamHandlerEvent>;

#[derive(Debug, Clone)]
pub(crate) struct StreamInboundUpgrade {
    protocol: StreamProtocol,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamOutboundUpgrade {
    protocol: StreamProtocol,
    request_id: StreamRequestId,
}

#[derive(Debug)]
pub(crate) enum StreamHandlerEvent {
    Inbound(Stream),
    Outbound { request_id: StreamRequestId, stream: Stream },
}

impl UpgradeInfo for StreamInboundUpgrade {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<StreamProtocol>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(self.protocol.clone())
    }
}

impl UpgradeInfo for StreamOutboundUpgrade {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<StreamProtocol>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(self.protocol.clone())
    }
}

impl InboundUpgrade<Stream> for StreamInboundUpgrade {
    type Output = StreamHandlerEvent;
    type Error = Infallible;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        future::ready(Ok(StreamHandlerEvent::Inbound(stream)))
    }
}

impl OutboundUpgrade<Stream> for StreamOutboundUpgrade {
    type Output = StreamHandlerEvent;
    type Error = Infallible;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        future::ready(Ok(StreamHandlerEvent::Outbound { request_id: self.request_id, stream }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_swarm_succeeds() {
        let id = Libp2pIdentity::from_config(&crate::config::Config::default()).expect("identity");
        let swarm = build_base_swarm(&id).expect("swarm");
        assert_eq!(swarm.local_peer_id(), &id.peer_id);
    }
}
