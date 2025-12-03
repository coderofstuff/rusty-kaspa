# Lab Report 4 – Proxmox NAT libp2p/DCUtR run (dedicated libp2p port)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.
- Ports: P2P 16111, libp2p 16112 (P2P+1), RPC 17110.

## Build steps (all nodes A/B/C)
- Clean slate: `pkill -x kaspad || true`; archived old `~/rusty-kaspa` and `~/kaspa` dirs.
- Fresh clone: `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git ~/rusty-kaspa`.
- Build: `source ~/.cargo/env && cd ~/rusty-kaspa && cargo build --release --features libp2p --bin kaspad`.

## Run commands / harness
- Harness: `tcp-hole-punch/lab/hole_punch_lab.sh` (updated for libp2p port).
- Invocation (from repo root):
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
- Script sets `KASPAD_LIBP2P_LISTEN_PORT`, `--libp2p-listen-port`, reservations to `/ip4/10.0.3.50/tcp/16112/p2p/<C_PEER_ID>`, and external multiaddrs `/ip4/10.0.3.61|62/tcp/16112`.

## Status snapshots
- Node C `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWMN2TcyDTw34yY3ZpDCvsvNR5Ybf5C5mdDY3g5MTNqK7h`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
- Node A `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWLF3tzRTraDHAabUvf4cUz3XvY7iXxomSP7Fd9GBWtTN9`, persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
- Node B `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWEVE7Y4GsUmC1bGPW5WQhZKFG3D2xkCgYVSohrtSkvwDx`, persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.

## Listener snapshots (ss -ltnp)
- Node C: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node A: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node B: 16111 (P2P), 16112 (libp2p), 17110 (RPC).

## Logs / behaviour
- Node C (kaspad.log):
  - `libp2p init: listen addresses [0.0.0.0:16112]`
  - `libp2p listening on /ip4/127.0.0.1/tcp/16112`, `/ip4/10.0.3.50/tcp/16112`
  - Reservations accepted: `ReservationReqAccepted` for A (`12D3KooWLF3tz...`) and B (`12D3KooWEVE7...`).
- Node A:
  - `libp2p init: listen addresses [0.0.0.0:16112]`; listening on `/ip4/127.0.0.1/tcp/16112`, `/ip4/192.168.1.10/tcp/16112`.
  - Reservation dial: `libp2p reservation attempt to /ip4/10.0.3.50/tcp/16112/p2p/12D3KooWMN2Tcy... via relay ...` followed by Noise/Yamux and identify with Node C.
- Node B:
  - `libp2p init: listen addresses [0.0.0.0:16112]`; listening on `/ip4/127.0.0.1/tcp/16112`, `/ip4/192.168.2.10/tcp/16112`.
  - Reservation dial to Node C on 16112; Noise/Yamux and identify with Node C.
- Connection manager counts (A/B/C): repeated lines `Connection manager: outgoing: 0/8 , connecting: 0, iterator: 0` (no Kaspa peers registered).
- No DCUtR-specific log lines observed (no “dcutr” hits); no “Connected to incoming/outgoing peer” entries.

## Outcome
- Successes: Dedicated libp2p port is active; A and B reserve on C, negotiate Noise/Yamux, and identify; C accepts both reservations.
- Gaps: Kaspa connection manager remains 0/8 on all nodes; A/B do not appear as Kaspa peers (no inbound/outbound peer logs), and no DCUtR events between A and B were observed. The libp2p layer establishes reservations, but the Kaspa peer layer is not showing connected peers in this NAT scenario.
- No crashes or panics encountered during this run.
