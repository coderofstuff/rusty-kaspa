# TCP Hole Punch / Libp2p Sanity Checks

Date: 2025-11-27 (post-consensus alignment re-verify)

## Build / Test Matrix

- `cargo build` ✅
- `cargo check --no-default-features` ✅
- `cargo build --features libp2p --bin kaspad` ✅ (yamux deprecation warnings only)
- `cargo test -p kaspa-p2p-libp2p --lib --tests` ✅ (DCUtR integration tests remain `#[ignore]` upstream)
- `cargo test -p kaspad --lib --features libp2p` ✅

## Runtime Smoke (single-host)

- Default binary (no libp2p feature):
  - `./target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-default --loglevel=info --nodnsseed --disable-upnp --listen=127.0.0.1:18611 --rpclisten=127.0.0.1:18610`
  - Result: P2P on 127.0.0.1:18611, RPC on 127.0.0.1:18610, clean SIGTERM shutdown.
- Libp2p feature build, mode `off`:
  - `./target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-libp2p-off --loglevel=info --nodnsseed --disable-upnp --listen=127.0.0.1:19611 --rpclisten=127.0.0.1:19610 --libp2p-mode=off`
  - Result: Same as default; no libp2p activity; clean shutdown.
- Libp2p feature build, mode `full` (dedicated port):
  - `./target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-libp2p-full --loglevel=debug --nodnsseed --disable-upnp --listen=127.0.0.1:20611 --rpclisten=127.0.0.1:20610 --libp2p-mode=full --libp2p-listen-port=20612 --libp2p-identity-path=/tmp/kaspad-sanity-libp2p-full/id.key`
  - Result: libp2p swarm initialises (peer id logged); AutoNAT client+server enabled; listening on `/ip4/127.0.0.1/tcp/20612`; clean shutdown.

## DCUtR / Lab status

- Libp2p DCUtR harness remains `#[ignore]` because upstream relay-client panics when the transport channel drops; not run automatically.
- Proxmox NAT lab harness not executed in this environment after this consensus verification; rerun on the lab hosts to reconfirm hole punching.
