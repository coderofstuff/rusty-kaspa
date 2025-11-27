# Lab Report 6 – Proxmox NAT libp2p/DCUtR run (simnet, dcute-holes)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.
- Ports: P2P 16111, libp2p 16112 (P2P+1), RPC 17110.

## Build steps (all nodes A/B/C)
- Stopped any kaspad (`pkill -x kaspad || true`), removed old `~/rusty-kaspa` and `~/kaspa`.
- Fresh clone: `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git ~/rusty-kaspa`.
- Build: `cd ~/rusty-kaspa && source ~/.cargo/env && cargo build --release --features libp2p --bin kaspad`.

## Run commands / harness (simnet)
- Start Node C (simnet) then A/B via harness:
  ```
  # get C peer id via gRPC after start_node_c
  C_PEER_ID=12D3KooWEFUhhZKaDmAepDLHqoNEs8dMcrD4GGcJ4pnbbT1m8hLG \
  P2P_PORT=16111 LIBP2P_PORT=16112 RPC_PORT=17110 NETWORK_FLAG="--simnet" \
  REPO_PATH=/home/ubuntu/rusty-kaspa BIN_PATH=/home/ubuntu/rusty-kaspa/target/release/kaspad \
  APPDIR_C=/home/ubuntu/kaspa/node-c APPDIR_A=/home/ubuntu/kaspa/node-a APPDIR_B=/home/ubuntu/kaspa/node-b \
  RUST_LOG=info,kaspa_p2p_libp2p=debug,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=debug \
  SSH_JUMP=root@10.0.3.11 \
  bash tcp-hole-punch/lab/hole_punch_lab.sh start
  ```
- Mode: simnet (`--simnet` via NETWORK_FLAG).
- Appdirs: `/home/ubuntu/kaspa/node-{a,b,c}`; repo/bin under `/home/ubuntu/rusty-kaspa`.

## Status snapshots (getLibp2pStatus)
- Node C: `mode: Full`, `peer_id: 12D3KooWEFUhhZKaDmAepDLHqoNEs8dMcrD4GGcJ4pnbbT1m8hLG`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
- Node A: `mode: Full`, `peer_id: 12D3KooWSP3T5sMYKY1vqaKfk6FyYgZTt4NfkkCY4YWM3g2vbALe`, persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
- Node B: `mode: Full`, `peer_id: 12D3KooWDNUvLKpJTZUZYd4PGFHspGfz59UU6t6Z78PLXp6EVuRc`, persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.

## Listener snapshots (ss -ltnp | grep kaspad)
- Node C: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node A: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node B: 16111 (P2P), 16112 (libp2p), 17110 (RPC).

## Key log excerpts
- Reservations (Node C):
  - `ReservationReqAccepted { src_peer_id: 12D3KooWSP3T5sMYKY1vqaKfk6FyYgZTt4NfkkCY4YWM3g2vbALe }`
  - `ReservationReqAccepted { src_peer_id: 12D3KooWDNUvLKpJTZUZYd4PGFHspGfz59UU6t6Z78PLXp6EVuRc }`
- Identify (no DCUtR protocol advertised):
  - From A/B to C: protocols include `/kaspad/transport/1.0.0`, relay hop/stop, ping, id; **no** `/libp2p/dcutr`.
  - From C to A/B: same set (no dcutr).
- DCUtR dial-back:
  - C: `libp2p dcutr: skipping dial-back to <A/B peer> : peer does not support dcutr`.
  - A/B: `libp2p dcutr: skipping dial-back to 12D3KooWEFUhhZKaDmAepDLHqoNEs8dMcrD4GGcJ4pnbbT1m8hLG: peer does not support dcutr`.
- Bridging + Kaspa peers:
  - C: inbound streams from A/B, `libp2p_bridge ... handing to Kaspa`, then `Kaspa peer registered from libp2p stream` (addresses `[fdff::7a37:d1f1:175a:b583]:38746` and `[fdff::8334:7af:28a7:d9f6]:43175`). `connect_with_stream` also logs `connection error`/`Unknown` once per peer.
  - A: `P2P Connected to outgoing peer [fdff::955:7fec:e6ce:d419]:59086 (outbound: 1)`.
  - B: same outgoing peer and full Kaspa version/ready handshake with C; periodic ping latency reports.
- Connection manager counts:
  - A: `outgoing: 1/8` steady.
  - B: `outgoing: 1/8` steady.
  - C: connection manager still reports `outgoing: 0/8` (inbound peers only).
- No A↔B direct/relayed peer entries found; no DCUtR success logs.

## Outcome
- Successes: libp2p listeners on dedicated port; reservations accepted on C; identify/handshake works; Kaspa peers established A↔C and B↔C (version/ready flows, pings), so relayed connectivity path functions.
- Gaps:
  - DCUtR never attempted because peers do not advertise `/libp2p/dcutr`; logs show “peer does not support dcutr” on all nodes.
  - No A↔B connectivity observed; only A/B↔C.
  - C’s `connect_with_stream` still logs `connection error` on both inbound bridges, though peers register and stay connected.
- Not run: optional mainnet rerun (kept simnet focus for DCUtR verification).
