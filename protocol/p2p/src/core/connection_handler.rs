use crate::common::ProtocolError;
use crate::core::hub::HubEvent;
use crate::pb::{
    KaspadMessage, p2p_client::P2pClient as ProtoP2pClient, p2p_server::P2p as ProtoP2p, p2p_server::P2pServer as ProtoP2pServer,
};
use crate::transport::TransportMetadata;
use crate::{ConnectionInitializer, Router};
use futures::{FutureExt, Stream};
use hyper::client::conn::http2::Builder as HyperH2Builder;
use hyper::client::conn::http2::SendRequest as HyperSendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use kaspa_core::{debug, info};
use kaspa_utils::networking::{IpAddress, NetAddress};
use kaspa_utils_tower::{
    counters::TowerConnectionCounters,
    middleware::{BodyExt, CountBytesBody, MapRequestBodyLayer, MapResponseBodyLayer, ServiceBuilder},
};
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{Sender as MpscSender, channel as mpsc_channel};
use tokio::sync::oneshot::{Sender as OneshotSender, channel as oneshot_channel};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Uri;
use tonic::transport::{Error as TonicError, Server as TonicServer};
use tonic::{Request, Response, Status as TonicStatus, Streaming};
use tonic::{body::BoxBody, transport::server::Connected};
use tower::Service;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("missing socket address")]
    NoAddress,

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    TonicError(#[from] TonicError),

    #[error("{0}")]
    TonicStatus(#[from] TonicStatus),

    #[error("{0}")]
    ProtocolError(#[from] ProtocolError),
}

/// Maximum P2P decoded gRPC message size to send and receive
const P2P_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
const LIBP2P_HTTP2_STREAM_WINDOW: u32 = 8 * 1024 * 1024; // 8 MiB
const LIBP2P_HTTP2_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024; // 16 MiB
const LIBP2P_HTTP2_MAX_FRAME_SIZE: u32 = 1024 * 1024; // 1 MiB
const LIBP2P_HTTP2_MAX_HEADER_LIST_SIZE: u32 = 64 * 1024; // 64 KiB

/// Handles Router creation for both server and client-side new connections
#[derive(Clone)]
pub struct ConnectionHandler {
    /// Cloned on each new connection so that routers can communicate with a central hub
    hub_sender: MpscSender<HubEvent>,
    initializer: Arc<dyn ConnectionInitializer>,
    counters: Arc<TowerConnectionCounters>,
    metadata_factory: Arc<dyn MetadataFactory + Send + Sync>,
    outbound_connector: Arc<dyn OutboundConnector>,
}

/// Factory to build transport metadata for new inbound/outbound connections.
pub trait MetadataFactory: Send + Sync {
    fn for_outbound(&self, reported_ip: IpAddress) -> TransportMetadata;
    fn for_inbound(&self, reported_ip: IpAddress) -> TransportMetadata;
}

/// A pluggable outbound connector. Defaults to TCP; libp2p will provide another.
pub trait OutboundConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: TransportMetadata,
        handler: &'a ConnectionHandler,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Arc<Router>, ConnectionError>> + Send + 'a>>;
}

/// Default TCP outbound connector.
pub struct TcpConnector;

impl OutboundConnector for TcpConnector {
    fn connect<'a>(
        &'a self,
        address: String,
        metadata: TransportMetadata,
        handler: &'a ConnectionHandler,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Arc<Router>, ConnectionError>> + Send + 'a>> {
        Box::pin(async move { handler.connect_tcp(address, metadata).await })
    }
}

/// Optional precomputed metadata to be attached to inbound connections.
#[derive(Clone, Debug)]
pub struct MetadataConnectInfo {
    pub socket_addr: Option<SocketAddr>,
    pub metadata: TransportMetadata,
}

impl MetadataConnectInfo {
    pub fn new(socket_addr: Option<SocketAddr>, metadata: TransportMetadata) -> Self {
        Self { socket_addr, metadata }
    }
}

impl ConnectionHandler {
    pub(crate) fn new(
        hub_sender: MpscSender<HubEvent>,
        initializer: Arc<dyn ConnectionInitializer>,
        counters: Arc<TowerConnectionCounters>,
        metadata_factory: Arc<dyn MetadataFactory + Send + Sync>,
        outbound_connector: Arc<dyn OutboundConnector>,
    ) -> Self {
        Self { hub_sender, initializer, counters, metadata_factory, outbound_connector }
    }

    /// Launches a P2P server listener loop
    pub(crate) fn serve(&self, serve_address: NetAddress) -> Result<OneshotSender<()>, ConnectionError> {
        let (termination_sender, termination_receiver) = oneshot_channel::<()>();
        let connection_handler = self.clone();
        info!("P2P Server starting on: {}", serve_address);

        let bytes_tx = self.counters.bytes_tx.clone();
        let bytes_rx = self.counters.bytes_rx.clone();

        tokio::spawn(async move {
            let serve_address_log = serve_address.clone();
            let proto_server = ProtoP2pServer::new(connection_handler)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(P2P_MAX_MESSAGE_SIZE);

            // TODO: check whether we should set tcp_keepalive
            let serve_result = TonicServer::builder()
                .layer(MapRequestBodyLayer::new(move |body| CountBytesBody::new(body, bytes_rx.clone()).boxed_unsync()))
                .layer(MapResponseBodyLayer::new(move |body| CountBytesBody::new(body, bytes_tx.clone())))
                .add_service(proto_server)
                .serve_with_shutdown(serve_address.into(), termination_receiver.map(drop))
                .await;

            match serve_result {
                Ok(_) => info!("P2P Server stopped: {}", serve_address_log),
                Err(err) => panic!("P2P, Server {serve_address_log} stopped with error: {err:?}"),
            }
        });
        Ok(termination_sender)
    }

    /// Connect to a new peer
    pub(crate) async fn connect(&self, peer_address: String) -> Result<Arc<Router>, ConnectionError> {
        let metadata = if peer_address.starts_with('/') {
            // libp2p multiaddrs are not valid socket addresses; let the outbound
            // connector resolve them without pre-parsing as TCP.
            TransportMetadata::default()
        } else {
            let Some(socket_address) = peer_address.to_socket_addrs()?.next() else {
                return Err(ConnectionError::NoAddress);
            };
            let reported_ip: IpAddress = socket_address.ip().into();
            self.metadata_factory.for_outbound(reported_ip)
        };

        // Delegate to the configured outbound connector (defaults to TCP)
        self.outbound_connector.connect(peer_address, metadata, self).await
    }

    /// Connect to a new peer with `retry_attempts` retries and `retry_interval` duration between each attempt
    pub(crate) async fn connect_with_retry(
        &self,
        address: String,
        retry_attempts: u8,
        retry_interval: Duration,
    ) -> Result<Arc<Router>, ConnectionError> {
        let mut counter = 0;
        loop {
            counter += 1;
            match self.connect(address.clone()).await {
                Ok(router) => {
                    debug!("P2P, Client connected, peer: {:?}", address);
                    return Ok(router);
                }
                Err(ConnectionError::ProtocolError(err)) => {
                    // On protocol errors we avoid retrying
                    debug!("P2P, connect retry #{} failed with error {:?}, peer: {:?}, aborting retries", counter, err, address);
                    return Err(ConnectionError::ProtocolError(err));
                }
                Err(err) => {
                    debug!("P2P, connect retry #{} failed with error {:?}, peer: {:?}", counter, err, address);
                    if counter < retry_attempts {
                        // Await `retry_interval` time before retrying
                        tokio::time::sleep(retry_interval).await;
                    } else {
                        debug!("P2P, Client connection retry #{} - all failed", retry_attempts);
                        return Err(err);
                    }
                }
            }
        }
    }

    pub(crate) async fn connect_tcp(&self, peer_address: String, metadata: TransportMetadata) -> Result<Arc<Router>, ConnectionError> {
        let Some(socket_address) = peer_address.to_socket_addrs()?.next() else {
            return Err(ConnectionError::NoAddress);
        };
        let peer_address = format!("http://{}", peer_address); // Add scheme prefix as required by Tonic

        let channel = tonic::transport::Endpoint::new(peer_address)?
            .timeout(Duration::from_millis(Self::communication_timeout()))
            .connect_timeout(Duration::from_millis(Self::connect_timeout()))
            .tcp_keepalive(Some(Duration::from_millis(Self::keep_alive())))
            .connect()
            .await?;

        let channel = ServiceBuilder::new()
            .layer(MapResponseBodyLayer::new(move |body| CountBytesBody::new(body, self.counters.bytes_rx.clone())))
            .layer(MapRequestBodyLayer::new(move |body| CountBytesBody::new(body, self.counters.bytes_tx.clone()).boxed_unsync()))
            .service(channel);

        let mut client = ProtoP2pClient::new(channel)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(P2P_MAX_MESSAGE_SIZE);

        let (outgoing_route, outgoing_receiver) = mpsc_channel(Self::outgoing_network_channel_size());
        let incoming_stream = client.message_stream(ReceiverStream::new(outgoing_receiver)).await?.into_inner();

        let router = Router::new(socket_address, true, metadata, self.hub_sender.clone(), incoming_stream, outgoing_route).await;

        // For outbound peers, we perform the initialization as part of the connect logic
        match self.initializer.initialize_connection(router.clone()).await {
            Ok(()) => {
                // Notify the central Hub about the new peer
                self.hub_sender.send(HubEvent::NewPeer(router.clone())).await.expect("hub receiver should never drop before senders");
            }

            Err(err) => {
                router.try_sending_reject_message(&err).await;
                // Ignoring the new router
                router.close().await;
                debug!("P2P, handshake failed for outbound peer {}: {}", router, err);
                return Err(ConnectionError::ProtocolError(err));
            }
        }

        Ok(router)
    }

    /// Connect to a peer using a pre-established async stream instead of dialing by address.
    ///
    /// This is intended for libp2p/DCUtR paths where the transport is provided by an
    /// outer layer. The stream is wrapped with an h2 client tuned for libp2p buffers,
    /// and a synthetic socket address is derived from the provided metadata.
    pub async fn connect_with_stream<S>(&self, stream: S, metadata: TransportMetadata) -> Result<Arc<Router>, ConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        info!("libp2p_bridge: connecting Kaspa peer from libp2p stream with metadata {:?}", metadata);
        let socket_address = metadata
            .synthetic_socket_addr()
            .or_else(|| metadata.reported_ip.map(|ip| SocketAddr::new(ip.into(), 0)))
            .ok_or(ConnectionError::NoAddress)?;

        let channel =
            build_libp2p_channel(stream, Duration::from_millis(Self::connect_timeout())).await.map_err(ConnectionError::IoError)?;

        let channel = ServiceBuilder::new()
            .layer(MapResponseBodyLayer::new(move |body| CountBytesBody::new(body, self.counters.bytes_rx.clone())))
            .layer(MapRequestBodyLayer::new(move |body| CountBytesBody::new(body, self.counters.bytes_tx.clone()).boxed_unsync()))
            .service(channel);

        let mut client = ProtoP2pClient::with_origin(channel, Uri::from_static("http://kaspa.libp2p"))
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(P2P_MAX_MESSAGE_SIZE);

        let (outgoing_route, outgoing_receiver) = mpsc_channel(Self::outgoing_network_channel_size());
        let incoming_stream = client.message_stream(ReceiverStream::new(outgoing_receiver)).await?.into_inner();

        let router = Router::new(socket_address, true, metadata, self.hub_sender.clone(), incoming_stream, outgoing_route).await;

        match self.initializer.initialize_connection(router.clone()).await {
            Ok(()) => {
                self.hub_sender.send(HubEvent::NewPeer(router.clone())).await.expect("hub receiver should never drop before senders");
            }

            Err(err) => {
                router.try_sending_reject_message(&err).await;
                router.close().await;
                debug!("P2P, handshake failed for outbound peer {}: {}", router, err);
                return Err(ConnectionError::ProtocolError(err));
            }
        }

        Ok(router)
    }

    // TODO: revisit the below constants
    fn outgoing_network_channel_size() -> usize {
        // TODO: this number is taken from go-kaspad and should be re-evaluated
        (1 << 17) + 256
    }

    fn communication_timeout() -> u64 {
        10_000
    }

    fn keep_alive() -> u64 {
        10_000
    }

    fn connect_timeout() -> u64 {
        1_000
    }

    /// Serve incoming connections from a custom stream source (e.g., libp2p substreams).
    pub fn serve_with_incoming<S, I>(&self, incoming: I)
    where
        S: AsyncRead + AsyncWrite + Connected + Send + Unpin + 'static,
        I: Stream<Item = Result<S, std::io::Error>> + Send + 'static,
    {
        let connection_handler = self.clone();
        let bytes_tx = self.counters.bytes_tx.clone();
        let bytes_rx = self.counters.bytes_rx.clone();

        tokio::spawn(async move {
            let proto_server = ProtoP2pServer::new(connection_handler)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(P2P_MAX_MESSAGE_SIZE);

            let serve_result = configure_libp2p_server(TonicServer::builder())
                .layer(MapRequestBodyLayer::new(move |body| CountBytesBody::new(body, bytes_rx.clone()).boxed_unsync()))
                .layer(MapResponseBodyLayer::new(move |body| CountBytesBody::new(body, bytes_tx.clone())))
                .add_service(proto_server)
                .serve_with_incoming(incoming)
                .await;

            match serve_result {
                Ok(_) => debug!("P2P Server stopped (incoming stream closed)"),
                Err(err) => panic!("P2P, server stopped with error: {err:?}"),
            }
        });
    }
}

#[tonic::async_trait]
impl ProtoP2p for ConnectionHandler {
    type MessageStreamStream = Pin<Box<dyn futures::Stream<Item = Result<KaspadMessage, TonicStatus>> + Send + 'static>>;

    /// Handle the new arriving **server** connections
    async fn message_stream(
        &self,
        request: Request<Streaming<KaspadMessage>>,
    ) -> Result<Response<Self::MessageStreamStream>, TonicStatus> {
        let (socket_address, metadata) = if let Some(info) = request.extensions().get::<MetadataConnectInfo>().cloned() {
            let socket_address = info
                .socket_addr
                .or_else(|| info.metadata.synthetic_socket_addr())
                .ok_or_else(|| TonicStatus::new(tonic::Code::InvalidArgument, "Incoming connection opening request has no address"))?;
            info!("libp2p_bridge: Kaspa peer registered from libp2p stream; addr={socket_address}, metadata={:?}", info.metadata);
            (socket_address, info.metadata)
        } else {
            let remote_address = request.remote_addr().ok_or_else(|| {
                TonicStatus::new(tonic::Code::InvalidArgument, "Incoming connection opening request has no remote address")
            })?;

            let reported_ip: IpAddress = remote_address.ip().into();
            (remote_address, self.metadata_factory.for_inbound(reported_ip))
        };

        // Build the in/out pipes
        let (outgoing_route, outgoing_receiver) = mpsc_channel(Self::outgoing_network_channel_size());
        let incoming_stream = request.into_inner();

        // Build the router object
        let router = Router::new(socket_address, false, metadata, self.hub_sender.clone(), incoming_stream, outgoing_route).await;

        // Notify the central Hub about the new peer
        self.hub_sender.send(HubEvent::NewPeer(router)).await.expect("hub receiver should never drop before senders");

        // Give tonic a receiver stream (messages sent to it will be forwarded to the network peer)
        Ok(Response::new(Box::pin(ReceiverStream::new(outgoing_receiver).map(Ok)) as Self::MessageStreamStream))
    }
}

fn configure_libp2p_server(builder: TonicServer) -> TonicServer {
    builder
        .max_frame_size(Some(LIBP2P_HTTP2_MAX_FRAME_SIZE))
        .initial_stream_window_size(Some(LIBP2P_HTTP2_STREAM_WINDOW))
        .initial_connection_window_size(Some(LIBP2P_HTTP2_CONNECTION_WINDOW))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .http2_max_header_list_size(Some(LIBP2P_HTTP2_MAX_HEADER_LIST_SIZE))
        .http2_adaptive_window(Some(false))
}

type Libp2pGrpcService = Libp2pSendRequest;

struct Libp2pSendRequest {
    inner: HyperSendRequest<BoxBody>,
}

impl From<HyperSendRequest<BoxBody>> for Libp2pSendRequest {
    fn from(inner: HyperSendRequest<BoxBody>) -> Self {
        Self { inner }
    }
}

impl Service<http::Request<BoxBody>> for Libp2pSendRequest {
    type Response = http::Response<BoxBody>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        debug!("libp2p gRPC send request: {}", req.uri());
        let fut = self.inner.send_request(req);
        Box::pin(async move {
            match fut.await {
                Ok(res) => Ok(res.map(tonic::body::boxed)),
                Err(err) => {
                    debug!("libp2p gRPC request failed: {err:?}");
                    Err(err)
                }
            }
        })
    }
}

async fn build_libp2p_channel<S>(stream: S, connect_timeout: Duration) -> Result<Libp2pGrpcService, std::io::Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let exec = TokioExecutor::new();
    let mut builder = HyperH2Builder::new(exec.clone());
    builder
        .timer(TokioTimer::new())
        .initial_stream_window_size(Some(LIBP2P_HTTP2_STREAM_WINDOW))
        .initial_connection_window_size(Some(LIBP2P_HTTP2_CONNECTION_WINDOW))
        .keep_alive_interval(Some(Duration::from_secs(30)))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .max_frame_size(Some(LIBP2P_HTTP2_MAX_FRAME_SIZE))
        .max_header_list_size(LIBP2P_HTTP2_MAX_HEADER_LIST_SIZE)
        .adaptive_window(false);

    let handshake = tokio::time::timeout(connect_timeout, builder.handshake(TokioIo::new(stream)))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "libp2p grpc handshake timed out"))?;

    let (send_request, connection) = handshake.map_err(std::io::Error::other)?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            debug!("libp2p h2 connection error: {:?}", err);
        }
    });

    Ok(Libp2pSendRequest::from(send_request))
}

#[cfg(test)]
mod tests {
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
}
