use std::net::SocketAddr;

use log::{debug, warn};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{Duration, sleep, timeout};

use crate::helper_api::{HelperApi, HelperError};
use crate::transport::Libp2pError;

use triggered::Listener;

const HELPER_ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(200);
pub(super) const HELPER_MAX_LINE: usize = 8 * 1024;
const HELPER_READ_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) async fn start_helper_listener(
    addr: SocketAddr,
    api: HelperApi,
    mut shutdown: Option<Listener>,
) -> Result<(), Libp2pError> {
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| Libp2pError::ListenFailed(e.to_string()))?;
    log::info!("libp2p helper API listening on {addr}");

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
                            sleep(HELPER_ACCEPT_RETRY_DELAY).await;
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
                        sleep(HELPER_ACCEPT_RETRY_DELAY).await;
                    }
                }
            }
        }
    });

    Ok(())
}

pub(super) async fn handle_helper_connection(mut stream: tokio::net::TcpStream, api: HelperApi) {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    match timeout(HELPER_READ_TIMEOUT, reader.read_line(&mut line)).await {
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
