# Lab Report 3 – Proxmox NAT libp2p run (dedicated libp2p port)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.
- Ports: P2P 16111, libp2p 16112 (P2P+1), RPC 17110.

## Build & run commands
- On each node (A/B/C): `pkill -x kaspad || true`; archive old `~/rusty-kaspa`/`~/kaspa`; fresh clone `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git ~/rusty-kaspa`; build `source ~/.cargo/env && cd ~/rusty-kaspa && cargo build --release --features libp2p --bin kaspad`.
- Harness invocation (from repo root):
  ```
  C_PEER_ID=12D3KooWMN2TcyDTw34yY3ZpDCvsvNR5Ybf5C5mdDY3g5MTNqK7h \
  P2P_PORT=16111 \
  LIBP2P_PORT=16112 \
  REPO_PATH=/home/ubuntu/rusty-kaspa \
  BIN_PATH=/home/ubuntu/rusty-kaspa/target/release/kaspad \
  APPDIR_C=/home/ubuntu/kaspa/node-c \
  APPDIR_A=/home/ubuntu/kaspa/node-a \
  APPDIR_B=/home/ubuntu/kaspa/node-b \
  bash tcp-hole-punch/lab/hole_punch_lab.sh start
  ```
- Harness changes: libp2p listen port set via `--libp2p-listen-port` and `KASPAD_LIBP2P_LISTEN_PORT`; reservations use `/tcp/16112`; external multiaddrs use `/tcp/16112`.

## Status snapshots
- Node C `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWMN2TcyDTw34yY3ZpDCvsvNR5Ybf5C5mdDY3g5MTNqK7h`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
- Node A `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWLF3tzRTraDHAabUvf4cUz3XvY7iXxomSP7Fd9GBWtTN9`, identity persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
- Node B `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWEVE7Y4GsUmC1bGPW5WQhZKFG3D2xkCgYVSohrtSkvwDx`, identity persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.
- Listeners (ss -ltnp):
  - Node C: 16111 (P2P), 17110 (RPC), 16112 (libp2p).
  - Node A: 16111 (P2P), 17110 (RPC), 16112 (libp2p).
  - Node B: 16111 (P2P), 17110 (RPC), 16112 (libp2p).

## Logs / behaviour
- Node C:
  - `libp2p init: listen addresses [0.0.0.0:16112]`
  - `libp2p listening on /ip4/127.0.0.1/tcp/16112`, `/ip4/10.0.3.50/tcp/16112`
  - Incoming connections from A (`/ip4/10.0.3.61/tcp/44230`) and B (`/ip4/10.0.3.62/tcp/58162`) negotiate Noise/Yamux, identify, and accept relay reservations: `ReservationReqAccepted { src_peer_id: ... }`.
- Node A:
  - `libp2p init: listen addresses [0.0.0.0:16112]`; listening on `/ip4/127.0.0.1/tcp/16112`, `/ip4/192.168.1.10/tcp/16112`.
  - Reservation dial: `libp2p reservation attempt to /ip4/10.0.3.50/tcp/16112/p2p/<C_PEER_ID> ...` followed by successful Noise/Yamux negotiation and identify with Node C (`listen_addrs: [/ip4/10.0.3.50/tcp/16112,...]`).
- Node B:
  - `libp2p init: listen addresses [0.0.0.0:16112]`; listening on `/ip4/127.0.0.1/tcp/16112`, `/ip4/192.168.2.10/tcp/16112`.
  - Reservation dial to Node C on 16112 with Noise/Yamux negotiation and identify exchange; reservation accepted on C.
- No “transport unimplemented” or noise upgrade failures; relay reservations succeed and identify exchanges complete. Connection manager still reports 0/8 P2P peers (libp2p relayed peers are not reflected there).

## Outcome
- Dedicated libp2p port is active on all nodes; reservations from A and B to C over 16112 succeed, with relay reservations accepted on C and identify/ping negotiated.
- Stable processes and listeners; no dial/listen errors on libp2p ports.
- Remaining gap: P2P connection manager still shows 0/8 peers, so libp2p peers are not yet reflected in the legacy P2P peer counts/flows. Relay connectivity appears established at the libp2p layer; further integration may be needed for full Kaspa peer routing over the libp2p streams.
