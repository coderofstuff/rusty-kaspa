# Lab Report 2 – Proxmox NAT libp2p run (clean branch with implemented transport)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.

## Commands / harness
- Refresh on each node (A/B/C): `pkill -x kaspad || true`; moved aside `~/rusty-kaspa`, `~/kaspa`; fresh clone `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git ~/rusty-kaspa`; build `source ~/.cargo/env && cargo build --release --features libp2p --bin kaspad`.
- Started Node C once manually to obtain PeerId, then ran harness with explicit paths to avoid local tilde expansion:
  - Manual Node C start (matches harness): `nohup env RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info /home/ubuntu/rusty-kaspa/target/release/kaspad --simnet --appdir=/home/ubuntu/kaspa/node-c --loglevel=debug --nodnsseed --disable-upnp --listen=0.0.0.0:16111 --rpclisten=0.0.0.0:17110 --libp2p-mode=full --libp2p-helper-listen=0.0.0.0:38080 --libp2p-identity-path=/home/ubuntu/kaspa/node-c/libp2p.id &`
  - PeerId from Node C: `12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM`.
  - Harness stop/start (after stopping the manual run): \
    `C_PEER_ID=12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM REPO_PATH=/home/ubuntu/rusty-kaspa BIN_PATH=/home/ubuntu/rusty-kaspa/target/release/kaspad APPDIR_C=/home/ubuntu/kaspa/node-c APPDIR_A=/home/ubuntu/kaspa/node-a APPDIR_B=/home/ubuntu/kaspa/node-b bash tcp-hole-punch/lab/hole_punch_lab.sh start`
  - Reservations passed via env for A/B: `/ip4/10.0.3.50/tcp/16111/p2p/12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM`; external multiaddrs: `/ip4/10.0.3.61/tcp/16111` (A), `/ip4/10.0.3.62/tcp/16111` (B).

## Status snapshots
- Node C `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
- Node A `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWLWJ8eLrd9uRnN7SrRK4o9MXCXvCjoFvzqBRbSvEgBBEt`, identity persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
- Node B `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWEpUATgsvjotuK9sqmJcuBzrG7Ljosobcp9wrdj43AECq`, identity persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.
- Listeners (ss -ltnp) on all nodes: only `16111` (p2p) and `17110` (gRPC); no visible helper/control port bound on `38080`.

## Logs / behaviour
- Node A log excerpts (reservations + failure):
  - `libp2p reservation attempt to /ip4/10.0.3.50/tcp/16111/p2p/12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM via relay "/ip4/10.0.3.50/tcp/16111/p2p/12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM"`
  - `Failed to dial /ip4/10.0.3.50/tcp/16111/p2p/… using libp2p_relay::priv_client::transport::Transport`
  - `Failed to upgrade outbound stream to /noise`
  - `P2P Server stopped (incoming stream closed)` (still stays up; connection loop continues with 0/8 peers).
- Node B shows the same pattern as Node A (reservation attempt to Node C, dial failure via libp2p_relay transport, noise upgrade failure, P2P server stopped message).
- Node C logs:
  - `libp2p streaming swarm peer id: 12D3KooWJUEXFY9tsyJ5Nk4zm9gAcXy7fUHfHWC7maegJMhm2NtM`
  - Repeated `Failed to listen on /ip4/0.0.0.0/tcp/16111 using libp2p_relay::priv_client::transport::Transport`
  - `P2P Server stopped (incoming stream closed)`
  - No reservation accept/relay events observed.
- No “transport unimplemented” messages anywhere (transport path is active and attempting libp2p operations).

## Outcome
- The libp2p stack now attempts reservations/dials (unlike the previous run), but both private nodes fail to complete the reservation/dial to Node C: libp2p_relay transport listen/dial errors and noise upgrade failures occur immediately, leaving 0/8 peers and no established circuits or DCUtR.
- No crashes or runaway reconnect loops; processes remain up, but no libp2p connectivity is achieved. Helper/control port is not visible as a listener.
- Next investigation targets: why relay transport listen/dial fails on 16111, whether helper/control needs to be bound/exposed, and why the outbound stream noise upgrade fails during reservation attempts. Once resolved, rerun this NAT scenario to confirm A/B reachability via C.
