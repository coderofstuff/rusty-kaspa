use crate::reservations::ReservationManager;
use crate::transport::{multiaddr_to_metadata, BoxedLibp2pStream, Libp2pStreamProvider};
use crate::{config::Config, transport::Libp2pError};
use kaspa_p2p_lib::{ConnectionHandler, MetadataConnectInfo, PathKind};
use libp2p::Multiaddr;
use log::{debug, warn};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;

/// Placeholder libp2p service that will eventually own dial/listen/reservation logic.
#[derive(Clone)]
pub struct Libp2pService {
    config: Config,
    provider: Option<std::sync::Arc<dyn Libp2pStreamProvider>>,
    reservations: ReservationManager,
}

const RESERVATION_REFRESH_INTERVAL: Duration = Duration::from_secs(20 * 60);
const RESERVATION_POLL_INTERVAL: Duration = Duration::from_secs(30);
const RESERVATION_BASE_BACKOFF: Duration = Duration::from_secs(5);
const RESERVATION_MAX_BACKOFF: Duration = Duration::from_secs(60);

impl Libp2pService {
    pub fn new(config: Config) -> Self {
        Self { config, provider: None, reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF) }
    }

    pub fn with_provider(config: Config, provider: Arc<dyn Libp2pStreamProvider>) -> Self {
        Self {
            config,
            provider: Some(provider),
            reservations: ReservationManager::new(RESERVATION_BASE_BACKOFF, RESERVATION_MAX_BACKOFF),
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
            tokio::spawn(async move { reservation_worker(provider, reservations, state).await });
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

        tokio::spawn(async move {
            loop {
                match provider.listen().await {
                    Ok((metadata, _close, stream)) => {
                        let info = MetadataConnectInfo::new(None, metadata);
                        let connected = MetaConnectedStream::new(stream, info);
                        if tx.send(Ok(connected)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(io::Error::new(io::ErrorKind::Other, err.to_string()))).await;
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

async fn reservation_worker(provider: Arc<dyn Libp2pStreamProvider>, reservations: Vec<String>, mut state: ReservationState) {
    if reservations.is_empty() {
        return;
    }

    loop {
        let now = Instant::now();
        refresh_reservations(provider.as_ref(), &reservations, &mut state, now).await;
        sleep(RESERVATION_POLL_INTERVAL).await;
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
}

struct ActiveReservation {
    refresh_at: Instant,
    handle: ReservationHandle,
}

struct ReservationHandle {}

impl ReservationHandle {
    fn release(self) {}
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
            active.handle.release();
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
    provider.reserve(target.multiaddr.clone()).await?;
    debug!("libp2p reservation attempt to {} via relay {:?}", target.raw, target.key);
    Ok(ReservationHandle {})
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
}

impl MetaConnectedStream {
    fn new(stream: BoxedLibp2pStream, info: MetadataConnectInfo) -> Self {
        Self { stream, info }
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
    use tokio::io::duplex;
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
        let provider = Arc::new(MockProvider::with_responses(
            VecDeque::from([Err(Libp2pError::ProviderUnavailable), Ok(()), Ok(())]),
            drops.clone(),
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
    }

    impl MockProvider {
        fn with_responses(responses: VecDeque<Result<(), Libp2pError>>, drops: Arc<AtomicUsize>) -> Self {
            Self { responses: StdMutex::new(responses), attempts: AtomicUsize::new(0), drops }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl Libp2pStreamProvider for MockProvider {
        fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = self.responses.lock().expect("responses");
                let resp = guard.pop_front().unwrap_or_else(|| Err(Libp2pError::ProviderUnavailable));
                resp.map(|_| (TransportMetadata::default(), make_stream(self.drops.clone())))
            })
        }

        fn listen<'a>(
            &'a self,
        ) -> BoxFuture<'a, Result<(TransportMetadata, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
            let drops = self.drops.clone();
            Box::pin(async move {
                let (client, _server) = duplex(64);
                let stream: BoxedLibp2pStream = Box::new(DropStream { inner: client, drops });
                let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
                Ok((TransportMetadata::default(), closer, stream))
            })
        }

        fn reserve<'a>(&'a self, _target: Multiaddr) -> BoxFuture<'a, Result<(), Libp2pError>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let mut guard = self.responses.lock().expect("responses");
                let resp = guard.pop_front().unwrap_or_else(|| Err(Libp2pError::ProviderUnavailable));
                resp
            })
        }
    }
}
