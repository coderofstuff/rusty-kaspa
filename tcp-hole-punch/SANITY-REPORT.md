# TCP Hole Punch / Libp2p Sanity Checks

Date: 2025-11-27

## Build / Test Matrix

- Default (no `libp2p` feature):
  - `cargo build` ✅
  - `cargo check --no-default-features` ✅
- Libp2p enabled:
  - `cargo build --features libp2p --bin kaspad` ✅
  - `cargo test -p kaspa-p2p-libp2p --lib` ✅
  - `cargo test -p kaspad --lib --features libp2p` ✅

Notes:
- Warnings limited to pre-existing unused type aliases in `kaspa-wrpc-wasm`.

## Runtime Smoke (single-node, local)

- Default binary (libp2p feature off, runtime default):
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-default --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=0 --nogrpc`
  - Result: Starts normally; P2P on 0.0.0.0:16511; no libp2p/helper activity. Clean shutdown on SIGTERM.
- Libp2p feature on, mode `off`:
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-sanity-libp2p-off --loglevel=info --nodnsseed --disable-upnp --rpcmaxclients=0 --nogrpc --libp2p-mode=off`
  - Result: Same behaviour as default run; P2P on 0.0.0.0:16511; no libp2p/helper started. Clean shutdown on SIGTERM.
- Libp2p feature on, mode `full` (helper configured):
  - Command: `target/debug/kaspad --simnet --appdir /tmp/kaspad-libp2p-full --loglevel=debug --nodnsseed --disable-upnp --listen=0.0.0.0:16511 --rpclisten=127.0.0.1:27510 --libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080 --libp2p-identity-path=/tmp/kaspad-libp2p-full/libp2p.id --libp2p-reservations=/ip4/127.0.0.1/tcp/16511/p2p/12D3KooWTestPeer --libp2p-external-multiaddrs=/ip4/127.0.0.1/tcp/16511 --libp2p-advertise-addresses=127.0.0.1:16511`
  - Result: Starts cleanly; P2P on 0.0.0.0:16511; libp2p swarm initialises (peer id logged); reservations/external multiaddrs parsed and applied to the swarm. Helper control still a placeholder (no listener bound).

## getLibp2pStatus (full mode)

- Command: `KASPAD_GRPC_SERVER=grpc://127.0.0.1:27510 cargo run --example libp2p_status -p kaspa-grpc-client --quiet`
- Response: `GetLibp2pStatusResponse { mode: Full, peer_id: Some("12D3KooWMUhtqULNyvLW14JgdaizRFgA34um5d2ozy26ZAiq8woZ"), identity: Persisted { path: "/tmp/kaspad-libp2p-full/libp2p.id" } }`

## Listener snapshots (lsof -i -P -n -a -p <pid>)

- Default (no libp2p feature build):
  - Ports: `TCP *:16511 (P2P)`, RPC disabled for the run above.
- Libp2p feature on, `--libp2p-mode=off`:
  - Ports: `TCP *:16511 (P2P)`, RPC disabled for the run above.
- Libp2p feature on, `--libp2p-mode=full --libp2p-helper-listen=127.0.0.1:38080`:
  - Ports: `127.0.0.1:27510 (RPC)`, `0.0.0.0:16511 (P2P)`; helper control port not bound (control plane still TODO).

## Notes / Fix

- libp2p transport no longer logs “transport unimplemented”; the swarm now dials/listens/reserves with real metadata plumbing.
- Helper control remains a stub; no listener is bound even when `--libp2p-helper-listen` is set (documented as TBD).
