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
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-libp2p-full --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=0 --nogrpc --libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080`
  - Result: **Panics immediately**: `there is no reactor running, must be called from the context of a Tokio 1.x runtime` (from `SwarmStreamProvider::new`), so libp2p full mode does not start yet.

## Follow-ups

- Fix libp2p full-mode startup: ensure `SwarmStreamProvider::new` is created inside a Tokio runtime (current panic blocks full-mode smoke).
- Consider a quick retry after that fix; gating/off behaviour already matches default. No PLAN updates needed beyond tracking the startup fix.
