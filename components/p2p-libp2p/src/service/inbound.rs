use std::sync::Arc;

use log::{debug, error, warn};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tokio_stream::wrappers::ReceiverStream;

use kaspa_p2p_lib::{ConnectionHandler, MetadataConnectInfo};

use crate::transport::{Libp2pError, Libp2pStreamProvider, StreamDirection};

use super::meta_stream::MetaConnectedStream;

use triggered::Listener;

const INBOUND_LISTEN_RETRY_DELAY: Duration = Duration::from_millis(200);
pub(super) const INBOUND_LISTEN_MAX_RETRYABLE_ERRORS: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InboundListenErrorAction {
    Retry { attempt: usize },
    Abort,
}

pub(super) fn inbound_listen_error_action(err: &Libp2pError, retryable_errors: usize) -> InboundListenErrorAction {
    match err {
        Libp2pError::ProviderUnavailable => {
            if retryable_errors < INBOUND_LISTEN_MAX_RETRYABLE_ERRORS {
                InboundListenErrorAction::Retry { attempt: retryable_errors + 1 }
            } else {
                InboundListenErrorAction::Abort
            }
        }
        Libp2pError::Disabled
        | Libp2pError::ListenFailed(_)
        | Libp2pError::ReservationFailed(_)
        | Libp2pError::DialFailed(_)
        | Libp2pError::Identity(_)
        | Libp2pError::Multiaddr(_) => InboundListenErrorAction::Abort,
    }
}

pub(super) fn start_inbound_bridge(
    provider: Arc<dyn Libp2pStreamProvider>,
    handler: Arc<ConnectionHandler>,
    mut shutdown: Option<Listener>,
) {
    let (tx, rx) = mpsc::channel(8);
    let handler_for_outbound = handler.clone();

    tokio::spawn(async move {
        let mut retryable_errors = 0usize;
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
                    retryable_errors = 0;
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
                Err(err) => match inbound_listen_error_action(&err, retryable_errors) {
                    InboundListenErrorAction::Retry { attempt } => {
                        retryable_errors = attempt;
                        warn!("libp2p inbound listen error (retry {attempt}/{INBOUND_LISTEN_MAX_RETRYABLE_ERRORS}): {err}");
                        if let Some(shutdown) = shutdown.as_mut() {
                            tokio::select! {
                                _ = shutdown.clone() => {
                                    debug!("libp2p inbound bridge shutting down after listen error");
                                    break;
                                }
                                _ = sleep(INBOUND_LISTEN_RETRY_DELAY) => {}
                            }
                        } else {
                            sleep(INBOUND_LISTEN_RETRY_DELAY).await;
                        }
                    }
                    InboundListenErrorAction::Abort => {
                        error!("libp2p inbound listen terminated: {err}");
                        break;
                    }
                },
            }
        }
    });

    let incoming_stream = ReceiverStream::new(rx);
    handler.serve_with_incoming(incoming_stream);
}
