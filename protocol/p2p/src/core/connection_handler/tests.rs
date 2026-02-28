use super::*;
use crate::core::{adaptor::Adaptor, hub::Hub};
use crate::handshake::KaspadHandshake;
use crate::pb::VersionMessage;
use crate::transport::Capabilities;
use crate::{DirectMetadataFactory, PathKind};
use kaspa_utils::networking::PeerId;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, duplex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

#[derive(Clone)]
struct TestInitializer;

#[tonic::async_trait]
impl ConnectionInitializer for TestInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        router.set_identity(PeerId::new(Uuid::new_v4()));
        router.start();
        Ok(())
    }
}

#[derive(Clone)]
struct TimeoutHandshakeInitializer {
    version_timeout: std::time::Duration,
    ready_timeout: std::time::Duration,
}

#[tonic::async_trait]
impl ConnectionInitializer for TimeoutHandshakeInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        let mut handshake = KaspadHandshake::new(&router, self.version_timeout, self.ready_timeout);
        router.start();
        let self_version_message = build_test_version_message();
        handshake.handshake(self_version_message).await?;
        handshake.exchange_ready_messages().await
    }
}

#[derive(Clone)]
struct SilentInitializer;

#[tonic::async_trait]
impl ConnectionInitializer for SilentInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        router.start();
        Ok(())
    }
}

#[derive(Clone)]
struct BasicHandshakeInitializer {
    version_timeout: std::time::Duration,
    ready_timeout: std::time::Duration,
}

#[tonic::async_trait]
impl ConnectionInitializer for BasicHandshakeInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        let mut handshake = KaspadHandshake::new(&router, self.version_timeout, self.ready_timeout);
        router.start();
        let self_version_message = build_test_version_message();
        handshake.handshake(self_version_message).await?;
        handshake.exchange_ready_messages().await
    }
}

struct TestServerIo {
    stream: tokio::io::DuplexStream,
    remote: SocketAddr,
}

impl TestServerIo {
    fn new(stream: tokio::io::DuplexStream, remote: SocketAddr) -> Self {
        Self { stream, remote }
    }
}

impl Connected for TestServerIo {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        tonic::transport::server::TcpConnectInfo { local_addr: None, remote_addr: Some(self.remote) }
    }
}

impl AsyncRead for TestServerIo {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TestServerIo {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_stream_establishes_router() {
    kaspa_core::log::try_init_logger("info");

    let initializer = Arc::new(TestInitializer);
    let counters = Arc::new(TowerConnectionCounters::default());
    let hub = Hub::new();
    let (hub_tx, hub_rx) = mpsc::channel(Adaptor::hub_channel_size());
    hub.clone().start_event_loop(hub_rx, initializer.clone());
    let handler =
        ConnectionHandler::new(hub_tx, initializer.clone(), counters, Arc::new(DirectMetadataFactory), Arc::new(TcpConnector));

    let (client_half, server_half) = duplex(8 * 1024);
    let remote_addr = SocketAddr::from(([127, 0, 0, 1], 4000));

    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    incoming_tx.send(TestServerIo::new(server_half, remote_addr)).await.expect("send server stream");
    drop(incoming_tx);
    let incoming_stream = ReceiverStream::new(incoming_rx).map(Ok::<_, std::io::Error>);
    handler.serve_with_incoming(incoming_stream);

    let metadata = TransportMetadata {
        libp2p_peer_id: Some("peer-stream-test".to_string()),
        peer_id: None,
        reported_ip: None,
        path: PathKind::Direct,
        capabilities: Capabilities::default(),
    };
    let expected_addr = metadata.synthetic_socket_addr().expect("synthetic address");

    let router = handler.connect_with_stream(client_half, metadata).await.expect("connect via stream");
    assert_eq!(router.net_address(), expected_addr);
    assert_eq!(router.metadata().libp2p_peer_id.as_deref(), Some("peer-stream-test"));

    router.close().await;
}

struct MetaServerIo {
    stream: tokio::io::DuplexStream,
    info: MetadataConnectInfo,
}

impl MetaServerIo {
    fn new(stream: tokio::io::DuplexStream, info: MetadataConnectInfo) -> Self {
        Self { stream, info }
    }
}

impl Connected for MetaServerIo {
    type ConnectInfo = MetadataConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for MetaServerIo {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for MetaServerIo {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[derive(Default)]
struct RecordingOutboundConnector {
    calls: Mutex<Vec<(String, TransportMetadata)>>,
}

impl OutboundConnector for RecordingOutboundConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: TransportMetadata,
        _handler: &'a ConnectionHandler,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Arc<Router>, ConnectionError>> + Send + 'a>> {
        self.calls.lock().unwrap().push((address, metadata));
        Box::pin(async { Err(ConnectionError::ProtocolError(ProtocolError::Other("recording connector"))) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_forwards_multiaddr_without_socket_parsing() {
    let initializer = Arc::new(SilentInitializer);
    let counters = Arc::new(TowerConnectionCounters::default());
    let hub = Hub::new();
    let (hub_tx, hub_rx) = mpsc::channel(Adaptor::hub_channel_size());
    hub.clone().start_event_loop(hub_rx, initializer.clone());

    let connector = Arc::new(RecordingOutboundConnector::default());
    let handler = ConnectionHandler::new(hub_tx, initializer, counters, Arc::new(DirectMetadataFactory), connector.clone());

    let relay_multiaddr = "/ip4/10.0.3.26/tcp/16112/p2p/12D3KooWA2jw4ycPqjtbffumkH97QUARFojtmpHT8Ki5aWXW6BtK/p2p-circuit/p2p/12D3KooWP2x9RVcnFnr6kPt1MQw7SHdJr9c6sB8BiFSrenSSzXfx";
    let err = handler.connect(relay_multiaddr.to_string()).await.expect_err("recording connector always errors");
    assert!(matches!(err, ConnectionError::ProtocolError(ProtocolError::Other("recording connector"))));

    let mut calls = connector.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let (address, metadata) = calls.pop().unwrap();
    assert_eq!(address, relay_multiaddr);
    assert_eq!(metadata.reported_ip, None);
    assert!(matches!(metadata.path, PathKind::Unknown));
    assert!(!metadata.capabilities.libp2p);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_incoming_accepts_metadata_connect_info() {
    kaspa_core::log::try_init_logger("info");

    let initializer = Arc::new(TestInitializer);
    let counters = Arc::new(TowerConnectionCounters::default());
    let hub = Hub::new();
    let (hub_tx, hub_rx) = mpsc::channel(Adaptor::hub_channel_size());
    hub.clone().start_event_loop(hub_rx, initializer.clone());
    let handler =
        ConnectionHandler::new(hub_tx, initializer.clone(), counters, Arc::new(DirectMetadataFactory), Arc::new(TcpConnector));

    let metadata = TransportMetadata {
        libp2p_peer_id: Some("peer-meta-test".to_string()),
        peer_id: None,
        reported_ip: None,
        path: PathKind::Relay { relay_id: Some("relay-x".into()) },
        capabilities: Capabilities { libp2p: true },
    };
    let expected_addr = metadata.synthetic_socket_addr().expect("synthetic address");

    let (client_half, server_half) = duplex(8 * 1024);

    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    let info = MetadataConnectInfo::new(None, metadata.clone());
    incoming_tx.send(MetaServerIo::new(server_half, info)).await.expect("send server stream");
    drop(incoming_tx);
    let incoming_stream = ReceiverStream::new(incoming_rx).map(Ok::<_, std::io::Error>);
    handler.serve_with_incoming(incoming_stream);

    let router = handler.connect_with_stream(client_half, metadata.clone()).await.expect("connect via stream");
    assert_eq!(router.net_address(), expected_addr);
    assert_eq!(router.metadata().libp2p_peer_id.as_deref(), Some("peer-meta-test"));

    router.close().await;
}

fn build_test_version_message() -> VersionMessage {
    VersionMessage {
        protocol_version: 5,
        services: 0,
        timestamp: kaspa_core::time::unix_now() as i64,
        address: None,
        id: Vec::from(Uuid::new_v4().as_bytes()),
        user_agent: String::new(),
        disable_relay_tx: false,
        subnetwork_id: None,
        network: "kaspa-mainnet".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_times_out_when_peer_silent() {
    kaspa_core::log::try_init_logger("info");

    let client_initializer = Arc::new(TimeoutHandshakeInitializer {
        version_timeout: std::time::Duration::from_millis(100),
        ready_timeout: std::time::Duration::from_millis(100),
    });
    let server_initializer = Arc::new(SilentInitializer);

    let counters = Arc::new(TowerConnectionCounters::default());

    let server_hub = Hub::new();
    let (server_tx, server_rx) = mpsc::channel(Adaptor::hub_channel_size());
    server_hub.clone().start_event_loop(server_rx, server_initializer.clone());
    let server_handler = ConnectionHandler::new(
        server_tx,
        server_initializer.clone(),
        counters.clone(),
        Arc::new(DirectMetadataFactory),
        Arc::new(TcpConnector),
    );

    let client_hub = Hub::new();
    let (client_tx, client_rx) = mpsc::channel(Adaptor::hub_channel_size());
    client_hub.clone().start_event_loop(client_rx, client_initializer.clone());
    let client_handler =
        ConnectionHandler::new(client_tx, client_initializer, counters, Arc::new(DirectMetadataFactory), Arc::new(TcpConnector));

    let (client_half, server_half) = duplex(8 * 1024);
    let metadata = TransportMetadata {
        libp2p_peer_id: Some("handshake-timeout-peer".to_string()),
        peer_id: None,
        reported_ip: None,
        path: PathKind::Direct,
        capabilities: Capabilities::default(),
    };
    let info = MetadataConnectInfo::new(None, metadata.clone());

    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    incoming_tx.send(MetaServerIo::new(server_half, info)).await.expect("send server stream");
    drop(incoming_tx);
    let incoming_stream = ReceiverStream::new(incoming_rx).map(Ok::<_, std::io::Error>);
    server_handler.serve_with_incoming(incoming_stream);

    let start = std::time::Instant::now();
    let res = client_handler.connect_with_stream(client_half, metadata).await;
    let elapsed = start.elapsed();
    let err = res.expect_err("handshake should fail fast");
    match err {
        ConnectionError::ProtocolError(ProtocolError::Timeout(_))
        | ConnectionError::ProtocolError(ProtocolError::ConnectionClosed) => {}
        other => panic!("expected timeout/close, got {other:?}"),
    }
    assert!(elapsed < std::time::Duration::from_secs(1), "handshake timeout should fire quickly (elapsed: {:?})", elapsed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interop_relay_metadata_handshake_succeeds() {
    kaspa_core::log::try_init_logger("info");

    let initializer = Arc::new(BasicHandshakeInitializer {
        version_timeout: std::time::Duration::from_secs(1),
        ready_timeout: std::time::Duration::from_secs(2),
    });

    let counters = Arc::new(TowerConnectionCounters::default());

    let server_hub = Hub::new();
    let (server_tx, server_rx) = mpsc::channel(Adaptor::hub_channel_size());
    server_hub.clone().start_event_loop(server_rx, initializer.clone());
    let server_handler = ConnectionHandler::new(
        server_tx,
        initializer.clone(),
        counters.clone(),
        Arc::new(DirectMetadataFactory),
        Arc::new(TcpConnector),
    );

    let client_hub = Hub::new();
    let (client_tx, client_rx) = mpsc::channel(Adaptor::hub_channel_size());
    client_hub.clone().start_event_loop(client_rx, initializer.clone());
    let client_handler =
        ConnectionHandler::new(client_tx, initializer.clone(), counters, Arc::new(DirectMetadataFactory), Arc::new(TcpConnector));

    let (client_half, server_half) = duplex(8 * 1024);

    // Server side: baseline metadata (no libp2p/relay awareness).
    let server_metadata = TransportMetadata {
        libp2p_peer_id: None,
        peer_id: None,
        reported_ip: Some(Ipv4Addr::new(203, 0, 113, 10).into()),
        path: PathKind::Direct,
        capabilities: Capabilities::default(),
    };
    let info = MetadataConnectInfo::new(Some(SocketAddr::from(([203, 0, 113, 10], 16000))), server_metadata);

    // Client side: libp2p/relay metadata to simulate a "new" node.
    let client_metadata = TransportMetadata {
        libp2p_peer_id: Some("interop-new-peer".to_string()),
        peer_id: None,
        reported_ip: None,
        path: PathKind::Relay { relay_id: Some("relay-xyz".into()) },
        capabilities: Capabilities { libp2p: true },
    };

    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    incoming_tx.send(MetaServerIo::new(server_half, info)).await.expect("send server stream");
    drop(incoming_tx);
    let incoming_stream = ReceiverStream::new(incoming_rx).map(Ok::<_, std::io::Error>);
    server_handler.serve_with_incoming(incoming_stream);

    let router = client_handler.connect_with_stream(client_half, client_metadata.clone()).await.expect("handshake should succeed");
    assert_eq!(router.metadata().path, client_metadata.path);
    assert!(router.metadata().capabilities.libp2p);

    router.close().await;
}
