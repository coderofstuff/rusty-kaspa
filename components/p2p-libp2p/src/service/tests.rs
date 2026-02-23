use super::Libp2pService;
use super::helper::{HELPER_MAX_LINE, handle_helper_connection};
use super::inbound::{INBOUND_LISTEN_MAX_RETRYABLE_ERRORS, InboundListenErrorAction, inbound_listen_error_action};
use super::reservations::{ReservationState, refresh_reservations, reservation_worker};

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use futures_util::future::BoxFuture;
use libp2p::{Multiaddr, PeerId};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex};
use tokio::time::Duration;

use crate::config::Config;
use crate::helper_api::HelperApi;
use crate::metadata::TransportMetadata;
use crate::reservations::ReservationManager;
use crate::transport::{BoxedLibp2pStream, Libp2pError, Libp2pStreamProvider, ReservationHandle, StreamDirection};
use kaspa_utils::networking::NetAddress;

#[tokio::test]
async fn start_disabled_returns_disabled() {
    let svc = Libp2pService::new(Config::default());
    let res = svc.start().await;
    assert!(matches!(res, Err(Libp2pError::Disabled)));
}

#[test]
fn inbound_listen_error_action_aborts_on_terminal_errors() {
    let action = inbound_listen_error_action(&Libp2pError::ListenFailed("address in use".into()), 0);
    assert_eq!(action, InboundListenErrorAction::Abort);

    let action = inbound_listen_error_action(&Libp2pError::Disabled, 0);
    assert_eq!(action, InboundListenErrorAction::Abort);
}

#[test]
fn inbound_listen_error_action_retries_provider_unavailable_until_limit() {
    for retryable_errors in 0..INBOUND_LISTEN_MAX_RETRYABLE_ERRORS {
        let action = inbound_listen_error_action(&Libp2pError::ProviderUnavailable, retryable_errors);
        assert_eq!(action, InboundListenErrorAction::Retry { attempt: retryable_errors + 1 });
    }

    let action = inbound_listen_error_action(&Libp2pError::ProviderUnavailable, INBOUND_LISTEN_MAX_RETRYABLE_ERRORS);
    assert_eq!(action, InboundListenErrorAction::Abort);
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
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
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
