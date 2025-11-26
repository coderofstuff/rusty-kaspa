# TCP Hole Punch / Libp2p Sanity Checks

Date: 2025-11-26

## Build / Test Matrix

- Default (no `libp2p` feature):
  - `cargo build` ✅
  - `cargo check --no-default-features` ✅
  - Targeted tests (existing): `cargo test -p kaspa-p2p-libp2p --lib` not applicable; baseline libs compile cleanly.
- Libp2p enabled:
  - `cargo build --features libp2p --bin kaspad` ✅
  - `cargo test -p kaspa-p2p-libp2p --lib` ✅
  - `cargo test -p kaspad --lib --features libp2p` ✅

Notes:
- Warnings limited to unused type aliases in `kaspa-wrpc-wasm` (pre-existing) and resolved import warning in `kaspad/src/daemon.rs`.

## Runtime Smoke (single-node, local)

- Default binary (libp2p feature off, runtime default):
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-default --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=0 --nogrpc`
  - Result: Starts normally; P2P on 0.0.0.0:16511; no libp2p/helper activity. Clean shutdown on SIGTERM.
- Libp2p feature on, mode `off`:
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-libp2p-off --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=0 --nogrpc --libp2p-mode=off`
  - Result: Same behaviour as default run; P2P on 0.0.0.0:16511; no libp2p/helper started. Clean shutdown on SIGTERM.
- Libp2p feature on, mode `full` (helper configured):
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-libp2p-full --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=32 --rpclisten=127.0.0.1:27510 --listen=0.0.0.0:27511 --libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080`
  - Result: Starts cleanly; P2P on 0.0.0.0:27511; libp2p swarm initialises (peer id logged). Outbound still stubbed (“NotImplemented” as expected). Helper port not yet bound (helper control still pending implementation).

## getLibp2pStatus (full mode)

- Command: `KASPAD_GRPC_SERVER=grpc://127.0.0.1:27510 cargo run --example libp2p_status -p kaspa-grpc-client --quiet`
- Response: `GetLibp2pStatusResponse { mode: Full, peer_id: Some("12D3KooWSuRL3jST2tQpxnG5hjwsfRkGx9vFKryusrrq4hMUmXJK"), identity: Ephemeral }`

## Listener snapshots (lsof -i -P -n -a -p <pid>)

- Default (no libp2p feature build):
  - Ports: `127.0.0.1:28510 (RPC)`, `0.0.0.0:28511 (P2P)`
- Libp2p feature on, `--libp2p-mode=off`:
  - Ports: `127.0.0.1:29510 (RPC)`, `0.0.0.0:29511 (P2P)`
- Libp2p feature on, `--libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080`:
  - Ports: `127.0.0.1:27510 (RPC)`, `0.0.0.0:27511 (P2P)`; helper control port not bound yet (control plane still TODO).

## Notes / Fix

- Root cause: `SwarmStreamProvider::new` was constructed before a Tokio runtime existed (daemon sync path), causing “no reactor running” panics in full mode.
- Fix: libp2p init is now deferred to an `AsyncService` (`Libp2pInitService`) registered with the existing daemon `AsyncRuntime`, constructing the swarm via `SwarmStreamProvider::with_handle` inside the runtime. Outbound connector uses a shared `OnceCell` to pick up the provider once initialised.
