use super::*;
use std::sync::Arc;

pub(super) struct MockProvider {
    responses: std::sync::Mutex<std::collections::VecDeque<Result<(), Libp2pError>>>,
    attempts: std::sync::atomic::AtomicUsize,
    drops: Arc<std::sync::atomic::AtomicUsize>,
    probe_peer: std::sync::Mutex<Option<PeerId>>,
    last_probe: std::sync::Mutex<Option<Multiaddr>>,
}

pub(super) fn make_test_stream(drops: Arc<std::sync::atomic::AtomicUsize>) -> BoxedLibp2pStream {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::io::{ReadBuf, duplex};

    struct DropStream {
        inner: tokio::io::DuplexStream,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for DropStream {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl AsyncRead for DropStream {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DropStream {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    let (client, _server) = duplex(64);
    Box::new(DropStream { inner: client, drops })
}

impl MockProvider {
    pub(super) fn with_responses(
        responses: std::collections::VecDeque<Result<(), Libp2pError>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses),
            attempts: std::sync::atomic::AtomicUsize::new(0),
            drops,
            probe_peer: std::sync::Mutex::new(None),
            last_probe: std::sync::Mutex::new(None),
        }
    }

    pub(super) fn with_probe_peer(
        responses: std::collections::VecDeque<Result<(), Libp2pError>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        probe_peer: PeerId,
    ) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses),
            attempts: std::sync::atomic::AtomicUsize::new(0),
            drops,
            probe_peer: std::sync::Mutex::new(Some(probe_peer)),
            last_probe: std::sync::Mutex::new(None),
        }
    }

    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn last_probe(&self) -> Option<Multiaddr> {
        self.last_probe.lock().expect("probe addr").clone()
    }
}

impl Libp2pStreamProvider for MockProvider {
    fn dial<'a>(&'a self, _address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| (TransportMetadata::default(), make_test_stream(self.drops.clone())))
        })
    }

    fn dial_multiaddr<'a>(
        &'a self,
        _address: Multiaddr,
    ) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| (TransportMetadata::default(), make_test_stream(self.drops.clone())))
        })
    }

    fn listen<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let drops = self.drops.clone();
        Box::pin(async move {
            let stream = make_test_stream(drops);
            let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
            Ok((TransportMetadata::default(), StreamDirection::Inbound, closer, stream))
        })
    }

    fn reserve<'a>(&'a self, _target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
        Box::pin(async move {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = self.responses.lock().expect("responses");
            let resp = guard.pop_front().unwrap_or(Err(Libp2pError::ProviderUnavailable));
            resp.map(|_| ReservationHandle::noop())
        })
    }

    fn probe_relay<'a>(&'a self, address: Multiaddr) -> BoxFuture<'a, Result<PeerId, Libp2pError>> {
        Box::pin(async move {
            *self.last_probe.lock().expect("probe addr") = Some(address);
            (*self.probe_peer.lock().expect("probe peer")).ok_or_else(|| Libp2pError::DialFailed("probe peer not configured".into()))
        })
    }

    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        Box::pin(async { Vec::new() })
    }
}
