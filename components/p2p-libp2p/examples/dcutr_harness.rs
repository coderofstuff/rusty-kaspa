#![cfg(feature = "libp2p")]

//! Minimal helper/DCUtR harness for manual interop.
//!
//! Usage:
//! `cargo run -p kaspa-p2p-libp2p --example dcutr_harness --features libp2p -- <ip:port>`
//! or set `LIBP2P_TARGET_ADDR=<ip:port>` to attempt a dial. If no target is
//! provided, the harness just prints the peer ID and listens.

use kaspa_p2p_libp2p::config::{Config, ConfigBuilder, Mode};
use kaspa_p2p_libp2p::{Libp2pIdentity, SwarmStreamProvider};
use kaspa_utils::networking::NetAddress;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    kaspa_core::log::try_init_logger("info");

    let target = env::args().nth(1).or_else(|| env::var("LIBP2P_TARGET_ADDR").ok());

    let cfg = ConfigBuilder::new().mode(Mode::Full).build();
    let id = Libp2pIdentity::from_config(&cfg).expect("identity");
    let provider = SwarmStreamProvider::new(cfg.clone(), id.clone())?;

    println!("libp2p harness peer id: {}", id.peer_id_string());
    println!("waiting for inbound streams; set LIBP2P_TARGET_ADDR or pass <ip:port> to dial");

    if let Some(target) = target {
        match target.parse::<NetAddress>() {
            Ok(addr) => {
                match provider.dial(addr).await {
                    Ok((_md, _stream)) => println!("dial succeeded to {target}"),
                    Err(err) => eprintln!("dial failed to {target}: {err}"),
                }
            }
            Err(err) => eprintln!("invalid target address {target}: {err}"),
        }
    }

    // Keep the process alive to allow inbound testing.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

#[cfg(not(feature = "libp2p"))]
fn main() {
    eprintln!("libp2p feature disabled; build with --features libp2p to run this harness");
}
