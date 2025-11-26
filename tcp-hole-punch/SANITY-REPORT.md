# TCP Hole Punch / Libp2p Sanity Checks

Date: 2025-11-27

## Build / Test Matrix

- Default (no `libp2p` feature):
  - `cargo build` ✅
  - `cargo check --no-default-features` ✅
- Libp2p enabled:
  - `cargo build --features libp2p --bin kaspad` ✅
  - `cargo test -p kaspa-p2p-libp2p --lib --tests -- --nocapture` ✅ (one DCUtR integration test is `#[ignore]` due to upstream relay-client drop panic; leave for manual runs)
  - `cargo test -p kaspad --lib --features libp2p` ✅

Notes:
- Warnings limited to pre-existing unused type aliases in `kaspa-wrpc-wasm` and deprecated `SwarmBuilder` in the ignored DCUtR harness.

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

## Local libp2p→Kaspa bridging sanity (two nodes, no NAT)

- Node C (relay/public role, dedicated libp2p port):
  - `RUST_LOG=info,kaspa_p2p_libp2p=debug,libp2p_swarm=info,libp2p_dcutr=debug KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/127.0.0.1/tcp/40012 target/debug/kaspad --simnet --appdir=/tmp/kaspad-libp2p-c --loglevel=debug --nodnsseed --disable-upnp --listen=0.0.0.0:40011 --rpclisten=127.0.0.1:40010 --libp2p-mode=full --libp2p-listen-port=40012 --libp2p-identity-path=/tmp/kaspad-libp2p-c/id.key`
  - Result: libp2p listening on `/ip4/127.0.0.1/tcp/40012` (plus local interfaces); PeerId `12D3KooWG2D1pixy2ezhFf5wSQ5sCFjxkLxLKWZtL3SKuY6Uc22P`.
- Node A (private, reserves on C):
  - `RUST_LOG=info,kaspa_p2p_libp2p=debug,libp2p_swarm=info,libp2p_dcutr=debug KASPAD_LIBP2P_RESERVATIONS=/ip4/127.0.0.1/tcp/40012/p2p/12D3KooWG2D1pixy2ezhFf5wSQ5sCFjxkLxLKWZtL3SKuY6Uc22P KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/127.0.0.1/tcp/41012 target/debug/kaspad --simnet --appdir=/tmp/kaspad-libp2p-a --loglevel=debug --nodnsseed --disable-upnp --listen=0.0.0.0:41011 --rpclisten=127.0.0.1:41010 --libp2p-mode=full --libp2p-listen-port=41012 --libp2p-identity-path=/tmp/kaspad-libp2p-a/id.key`
  - Result: reservation accepted by C; libp2p bridge hands stream to Kaspa (`libp2p_bridge: outbound stream ready for Kaspa…`), `connect_with_stream` invoked, and `libp2p_bridge: Kaspa peer registered from libp2p stream; addr=[fdff::33e7:6090:3164:c673]:45412…` observed.
- Kaspa layer:
  - Node A connection manager shows `outgoing: 1/8`; handshake/flows registered on synthetic address.
  - Node C logs `P2P Connected to incoming peer … (inbound: 1)`.
- Key listeners: P2P on 40011/41011; libp2p on 40012/41012 (dedicated, not multiplexed with P2P).

## Local DCUtR harness sanity

- Command: `cargo test -p kaspa-p2p-libp2p --test dcutr -- --ignored --nocapture`
- Result: panics inside `libp2p-relay::priv_client` (“Behaviour polled after transport channel closed”) when the upstream relay client drops its transport channel; this is an upstream libp2p issue.
- Conclusion: DCUtR harness currently blocked by upstream relay client panic; test remains `#[ignore]` and should be run manually only for debugging once upstream fixes it.

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
- DCUtR logging promoted to info on success/fail; a dedicated DCUtR harness test exists but is `#[ignore]` for now due to upstream relay-client drop panic—run manually when needed.
- Added DCUtR dial-back via relay to trigger hole punch negotiations when we only have inbound relayed connections; see `tcp-hole-punch/DCUTR-GAP-NOTE.md`. Build/test matrix above rerun after the change.
