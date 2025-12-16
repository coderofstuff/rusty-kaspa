use crate::helper_api::{HelperApi, HelperError};
use crate::reservations::ReservationManager;
use crate::transport::{multiaddr_to_metadata, BoxedLibp2pStream, Libp2pStreamProvider, ReservationHandle, StreamDirection};
use crate::{config::Config, transport::Libp2pError};
use kaspa_p2p_lib::{ConnectionHandler, MetadataConnectInfo, PathKind};
use libp2p::Multiaddr;
use log::{debug, warn};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use triggered::Listener;

/// Placeholder libp2p service that will eventually own dial/listen/reservation logic.
#[derive(Clone)]
pub struct Libp2pService {
    config: Config,
    provider: Option<std::sync::Arc<dyn Libp2pStreamProvider>>,
    reservations: ReservationManager,
    shutdown: Option<Listener>,
}

const RESERVATION_REFRESH_INTERVAL: Duration = Duration::from_secs(20 * 60);
const RESERVATION_POLL_INTERVAL: Duration = Duration::from_secs(30);
const RESERVATION_BASE_BACKOFF: Duration = Duration::from_secs(5);
const RESERVATION_MAX_BACKOFF: Duration = Duration::from_secs(60);
const HELPER_MAX_LINE: usize = 8 * 1024;
const HELPER_READ_TIMEOUT: Duration = Duration::from_secs(5);

impl Libp2pService {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            provider: None,
            reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF),
            shutdown: None,
        }
    }

    pub fn with_provider(config: Config, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self {
            config,
            provider: Some(provider),
            reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF),
            shutdown: None,
        }
    }

    pub fn with_shutdown(mut self, shutdown: Listener) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    pub async fn shutdown(&self) {
        if let Some(provider) = &self.provider {
            provider.shutdown().await;
        }
    }

    pub async fn start(&self) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let provider = self.provider.as_ref().ok_or(Libp2pError::ProviderUnavailable)?.clone();

        if !self.config.reservations.is_empty() {
            let reservations = self.config.reservations.clone();
            let state = ReservationState::new(self.reservations.clone(), RESERVATION_REFRESH_INTERVAL);
            let provider_for_worker = provider.clone();
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move { reservation_worker(provider_for_worker, reservations, state, shutdown).await });
        }

        if let Some(addr) = self.config.helper_listen {
            let api = HelperApi::new(provider.clone());
            let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| Libp2pError::ListenFailed(e.to_string()))?;
            log::info!("libp2p helper API listening on {addr}");

            let mut shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                loop {
                    if let Some(shutdown) = shutdown.as_mut() {
                        tokio::select! {
                            _ = shutdown.clone() => {
                                debug!("libp2p helper listener shutting down");
                                break;
                            }
                            accept_res = listener.accept() => match accept_res {
                                Ok((stream, _)) => {
                                    let api = api.clone();
                                    tokio::spawn(async move {
                                        handle_helper_connection(stream, api).await;
                                    });
                                }
                                Err(err) => {
                                    warn!("libp2p helper accept error: {err}");
                                    sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                    } else {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let api = api.clone();
                                tokio::spawn(async move {
                                    handle_helper_connection(stream, api).await;
                                });
                            }
                            Err(err) => {
                                warn!("libp2p helper accept error: {err}");
                                sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Start an inbound listener using the provided stream provider and connection handler.
    /// This bridges libp2p streams into the tonic server via `serve_with_incoming`.
    pub async fn start_inbound(&self, handler: std::sync::Arc<ConnectionHandler>) -> Result<(), Libp2pError> {
        if !self.config.mode.is_enabled() {
            return Err(Libp2pError::Disabled);
        }

        let provider = self.provider.as_ref().ok_or(Libp2pError::ProviderUnavailable)?.clone();
        let (tx, rx) = mpsc::channel(8);
        let handler_for_outbound = handler.clone();
        let mut shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            loop {
                let listen_fut = provider.listen();
                let listen_result = if let Some(ref mut shutdown) = shutdown {
                    tokio::select! {
                        _ = shutdown.clone() => {
                            debug!("libp2p inbound bridge shutting down");
                            break;
                        }
                        res = listen_fut => res,
                    }
                } else {
                    listen_fut.await
                };

                match listen_result {
                    Ok((metadata, direction, close, stream)) => {
                        match direction {
                            StreamDirection::Outbound => {
                                // For locally initiated streams, act as the client and connect directly.
                                log::info!(
                                    "libp2p_bridge: outbound stream ready for Kaspa connect_with_stream with metadata {:?}",
                                    metadata
                                );
                                let mut close = Some(close);
                                if let Err(err) = handler_for_outbound.connect_with_stream(stream, metadata).await {
                                    log::warn!("libp2p_bridge: outbound connect_with_stream failed: {err}");
                                    if let Some(close) = close.take() {
                                        close();
                                    }
                                }
                            }
                            StreamDirection::Inbound => {
                                log::info!("libp2p_bridge: inbound stream ready for Kaspa with metadata {:?}", metadata);
                                let info = MetadataConnectInfo::new(None, metadata);
                                let connected = MetaConnectedStream::new(stream, info, Some(close));
                                if tx.send(Ok(connected)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!("libp2p inbound listen error: {err}");
                        let _ = tx.send(Err(io::Error::other(err.to_string()))).await;
                        break;
                    }
                }
            }
        });

        let incoming_stream = ReceiverStream::new(rx);
        handler.serve_with_incoming(incoming_stream);

        Ok(())
    }
}

async fn handle_helper_connection(mut stream: tokio::net::TcpStream, api: HelperApi) {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    match tokio::time::timeout(HELPER_READ_TIMEOUT, reader.read_line(&mut line)).await {
        Ok(Ok(bytes)) => {
            if bytes == 0 {
                return;
            }
        }
        Ok(Err(err)) => {
            warn!("libp2p helper read error: {err}");
            if err.kind() == io::ErrorKind::InvalidData {
                let resp = HelperApi::error_response(&HelperError::Invalid("invalid utf-8".into()));
                let _ = writer.write_all(resp.as_bytes()).await;
                let _ = writer.write_all(b"\n").await;
            }
            return;
        }
        Err(_) => {
            warn!("libp2p helper read timeout after {:?}", HELPER_READ_TIMEOUT);
            let _ = writer.write_all(br#"{"ok":false,"error":"timeout waiting for request"}"#).await;
            let _ = writer.write_all(b"\n").await;
            return;
        }
    }

    if line.len() > HELPER_MAX_LINE {
        warn!("libp2p helper request exceeded max length ({} bytes)", HELPER_MAX_LINE);
        let _ = writer.write_all(br#"{"ok":false,"error":"request too long"}"#).await;
        let _ = writer.write_all(b"\n").await;
        return;
    }

    let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
    let resp_str = match api.handle_json(trimmed).await {
        Ok(r) => r,
        Err(e) => {
            warn!("libp2p helper request error: {e}");
            HelperApi::error_response(&e)
        }
    };
    let _ = writer.write_all(resp_str.as_bytes()).await;
    let _ = writer.write_all(b"\n").await;
}

async fn reservation_worker(
    provider: Arc<dyn Libp2pStreamProvider>,
    reservations: Vec<String>,
    mut state: ReservationState,
    mut shutdown: Option<Listener>,
) {
    if reservations.is_empty() {
        return;
    }

    loop {
        let now = Instant::now();
        refresh_reservations(provider.as_ref(), &reservations, &mut state, now).await;

        if let Some(shutdown) = shutdown.as_mut() {
            tokio::select! {
                _ = shutdown.clone() => {
                    debug!("libp2p reservation worker shutting down");
                    state.release_all().await;
                    break;
                }
                _ = sleep(RESERVATION_POLL_INTERVAL) => {}
            }
        } else {
            sleep(RESERVATION_POLL_INTERVAL).await;
        }
    }
}

struct ReservationState {
    backoff: ReservationManager,
    active: HashMap<String, ActiveReservation>,
    refresh_interval: Duration,
}

impl ReservationState {
    fn new(backoff: ReservationManager, refresh_interval: Duration) -> Self {
        Self { backoff, active: HashMap::new(), refresh_interval }
    }

    async fn release_all(&mut self) {
        let active = std::mem::take(&mut self.active);
        for (_, reservation) in active {
            reservation.handle.release().await;
        }
    }
}

struct ActiveReservation {
    refresh_at: Instant,
    handle: ReservationHandle,
}

async fn refresh_reservations(
    provider: &dyn Libp2pStreamProvider,
    reservations: &[String],
    state: &mut ReservationState,
    now: Instant,
) {
    for raw in reservations {
        let parsed = match ParsedReservation::parse(raw) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!("invalid reservation target {raw}: {err}");
                state.backoff.record_failure(raw, now);
                continue;
            }
        };

        let key = parsed.key.clone();

        let refresh_due = match state.active.get(&key) {
            Some(active) => now >= active.refresh_at,
            None => true,
        };

        if !refresh_due {
            continue;
        }

        if !state.backoff.should_attempt(&key, now) {
            continue;
        }

        if let Some(active) = state.active.remove(&key) {
            active.handle.release().await;
        }

        match attempt_reservation(provider, &parsed).await {
            Ok(handle) => {
                state.backoff.record_success(&key);
                let refresh_at = now + state.refresh_interval;
                state.active.insert(key, ActiveReservation { refresh_at, handle });
            }
            Err(err) => {
                warn!("libp2p reservation attempt to {raw} failed: {err}");
                state.backoff.record_failure(&key, now);
            }
        }
    }
}

async fn attempt_reservation(
    provider: &dyn Libp2pStreamProvider,
    target: &ParsedReservation,
) -> Result<ReservationHandle, Libp2pError> {
    let handle = provider.reserve(target.multiaddr.clone()).await?;
    debug!("libp2p reservation attempt to {} via relay {:?}", target.raw, target.key);
    Ok(handle)
}

#[derive(Clone)]
struct ParsedReservation {
    raw: String,
    key: String,
    multiaddr: Multiaddr,
}

impl ParsedReservation {
    fn parse(raw: &str) -> Result<Self, Libp2pError> {
        let multiaddr: Multiaddr = Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))?;
        let (_net, path) = multiaddr_to_metadata(&multiaddr);
        let key = match path {
            PathKind::Relay { relay_id: Some(id) } => id,
            _ => raw.to_string(),
        };

        Ok(Self { raw: raw.to_string(), key, multiaddr })
    }
}

struct MetaConnectedStream {
    stream: BoxedLibp2pStream,
    info: MetadataConnectInfo,
    close: Option<Box<dyn FnOnce() + Send>>,
}

impl MetaConnectedStream {
    fn new(stream: BoxedLibp2pStream, info: MetadataConnectInfo, close: Option<Box<dyn FnOnce() + Send>>) -> Self {
        Self { stream, info, close }
    }
}

impl Drop for MetaConnectedStream {
    fn drop(&mut self) {
        if let Some(close) = self.close.take() {
            close();
        }
    }
}

impl tonic::transport::server::Connected for MetaConnectedStream {
    type ConnectInfo = MetadataConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for MetaConnectedStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for MetaConnectedStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TransportMetadata;
    use futures_util::future::BoxFuture;
    use kaspa_utils::networking::NetAddress;
    use libp2p::PeerId;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
    use tokio::time::Duration;

    #[tokio::test]
    async fn start_disabled_returns_disabled() {
        let svc = Libp2pService::new(Config::default());
        let res = svc.start().await;
        assert!(matches!(res, Err(Libp2pError::Disabled)));
    }

    #[tokio::test]
    async fn reservation_backoff_advances() {
        let mut mgr = ReservationManager::new(Duration::from_secs(1), Duration::from_secs(4));
        let now = Instant::now();
        assert!(mgr.should_attempt("relay1", now));
        mgr.record_failure("relay1", now);
        assert!(!mgr.should_attempt("relay1", now));
        assert!(mgr.should_attempt("relay1", now + Duration::from_secs(1)));
    }

    #[tokio::test]
    async fn reservation_refresh_respects_backoff_and_releases() {
        let drops = Arc::new(AtomicUsize::new(0));
        let releases = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(MockProvider::with_responses(
            VecDeque::from([Err(Libp2pError::ProviderUnavailable), Ok(()), Ok(())]),
            drops.clone(),
            releases.clone(),
        ));

        let mut state = ReservationState::new(
            ReservationManager::new(Duration::from_millis(50), Duration::from_millis(200)),
            Duration::from_millis(100),
        );
        let relay = PeerId::random();
        let reservations = vec![format!("/ip4/203.0.113.1/tcp/4001/p2p/{relay}")];

        let now = Instant::now();
        refresh_reservations(provider.as_ref(), &reservations, &mut state, now).await;
        assert_eq!(provider.attempts(), 1, "first attempt should occur immediately");

        refresh_reservations(provider.as_ref(), &reservations, &mut state, now + Duration::from_millis(20)).await;
        assert_eq!(provider.attempts(), 1, "backoff should skip early retry");

        refresh_reservations(provider.as_ref(), &reservations, &mut state, now + Duration::from_millis(60)).await;
        assert_eq!(provider.attempts(), 2, "second attempt after backoff");
        assert_eq!(state.active.len(), 1, "successful attempt should register active reservation");

        refresh_reservations(provider.as_ref(), &reservations, &mut state, now + Duration::from_millis(120)).await;
        assert_eq!(provider.attempts(), 2, "no refresh until interval passes");

        refresh_reservations(provider.as_ref(), &reservations, &mut state, now + Duration::from_millis(200)).await;
        assert_eq!(provider.attempts(), 3, "refresh after interval should re-attempt");
        assert_eq!(drops.load(Ordering::SeqCst), 0, "no streams are held for reservations");
        assert_eq!(releases.load(Ordering::SeqCst), 1, "previous reservation should be released before refresh");
    }

    async fn run_helper_once(payload: &str) -> String {
        let api = HelperApi::new(Arc::new(MockProvider::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind helper listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                handle_helper_connection(stream, api).await;
            }
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect helper");
        client.write_all(payload.as_bytes()).await.expect("write payload");
        client.write_all(b"\n").await.expect("newline");
        let mut resp = String::new();
        let _ = client.read_to_string(&mut resp).await;
        resp
    }

    #[tokio::test]
    async fn helper_rejects_long_lines() {
        let payload = "x".repeat(HELPER_MAX_LINE + 10);
        let resp = run_helper_once(&payload).await;
        assert!(resp.contains("request too long"));
    }

    #[tokio::test]
    async fn helper_survives_bad_input() {
        let api = HelperApi::new(Arc::new(MockProvider::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind helper listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept().await {
                    let api = api.clone();
                    tokio::spawn(async move {
                        handle_helper_connection(stream, api).await;
                    });
                }
            }
        });

        let mut client1 = tokio::net::TcpStream::connect(addr).await.expect("connect helper");
        client1.write_all(b"not-json\n").await.expect("write bad payload");
        let mut resp1 = String::new();
        let _ = client1.read_to_string(&mut resp1).await;

        let mut client2 = tokio::net::TcpStream::connect(addr).await.expect("connect helper");
        client2.write_all(br#"{"action":"status"}"#).await.expect("write good payload");
        client2.write_all(b"\n").await.expect("newline");
        let mut resp2 = String::new();
        let _ = client2.read_to_string(&mut resp2).await;

        assert!(resp1.contains(r#""ok":false"#), "bad request should return error");
        assert!(resp2.contains(r#""ok":true"#), "subsequent request should still succeed");
    }

    #[tokio::test]
    async fn helper_rejects_invalid_utf8() {
        let api = HelperApi::new(Arc::new(MockProvider::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind helper listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                handle_helper_connection(stream, api).await;
            }
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect helper");
        client.write_all(&[0xff, 0xfe, b'\n']).await.expect("write invalid utf8 payload");
        let mut resp = String::new();
        let _ = client.read_to_string(&mut resp).await;

        assert!(resp.contains(r#""ok":false"#), "invalid utf-8 should return error response");
    }

    #[tokio::test]
    async fn reservation_worker_releases_on_shutdown() {
        let drops = Arc::new(AtomicUsize::new(0));
        let releases = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(MockProvider::with_responses(VecDeque::from([Ok(())]), drops.clone(), releases.clone()));
        let relay = PeerId::random();
        let reservations = vec![format!("/ip4/203.0.113.1/tcp/4001/p2p/{relay}")];
        let state = ReservationState::new(
            ReservationManager::new(Duration::from_millis(10), Duration::from_millis(20)),
            Duration::from_millis(10),
        );
        let (trigger, listener) = triggered::trigger();
        let worker = tokio::spawn(reservation_worker(provider.clone(), reservations, state, Some(listener)));

        tokio::time::sleep(Duration::from_millis(5)).await;
        trigger.trigger();
        let _ = worker.await;

        assert_eq!(releases.load(Ordering::SeqCst), 1, "shutdown should release active reservations");
    }

    fn make_stream(drops: Arc<AtomicUsize>) -> BoxedLibp2pStream {
        let (client, _server) = duplex(64);
        Box::new(DropStream { inner: client, drops })
    }

    struct DropStream {
        inner: tokio::io::DuplexStream,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropStream {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl AsyncRead for DropStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DropStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
            Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    struct MockProvider {
        responses: StdMutex<VecDeque<Result<(), Libp2pError>>>,
        attempts: AtomicUsize,
        drops: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
    }

    impl MockProvider {
        fn with_responses(responses: VecDeque<Result<(), Libp2pError>>, drops: Arc<AtomicUsize>, releases: Arc<AtomicUsize>) -> Self {
            Self { responses: StdMutex::new(responses), attempts: AtomicUsize::new(0), drops, releases }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl Default for MockProvider {
        fn default() -> Self {
            Self {
                responses: StdMutex::new(VecDeque::new()),
                attempts: AtomicUsize::new(0),
                drops: Arc::new(AtomicUsize::new(0)),
                releases: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Libp2pStreamProvider for MockProvider {
        fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = self.responses.lock().expect("responses");
                let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
                resp.map(|_| (TransportMetadata::default(), make_stream(self.drops.clone())))
            })
        }

        fn dial_multiaddr<'a>(
            &'a self,
            _address: Multiaddr,
        ) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = self.responses.lock().expect("responses");
                let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
                resp.map(|_| (TransportMetadata::default(), make_stream(self.drops.clone())))
            })
        }

        fn listen<'a>(
            &'a self,
        ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>>
        {
            let drops = self.drops.clone();
            Box::pin(async move {
                let (client, _server) = duplex(64);
                let stream: BoxedLibp2pStream = Box::new(DropStream { inner: client, drops });
                let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
                Ok((TransportMetadata::default(), StreamDirection::Inbound, closer, stream))
            })
        }

        fn reserve<'a>(&'a self, _target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = self.responses.lock().expect("responses");
                let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
                let releases = self.releases.clone();
                resp.map(|_| {
                    ReservationHandle::new(async move {
                        releases.fetch_add(1, Ordering::SeqCst);
                    })
                })
            })
        }
    }
}
