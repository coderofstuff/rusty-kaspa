# Lab Report 7 – Proxmox NAT libp2p/DCUtR run (simnet, libp2p 0.56 + dcutr advertise)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.
- Ports: P2P 16111, libp2p 16112 (P2P+1), RPC 17110.

## Build steps (all nodes A/B/C)
- Stopped kaspad; removed old `~/rusty-kaspa` and `~/kaspa`.
- Fresh clone: `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git ~/rusty-kaspa`.
- Build: `cd ~/rusty-kaspa && source ~/.cargo/env && cargo build --release --features libp2p --bin kaspad` (libp2p 0.56 pulled in).

## Run commands / harness (simnet)
- Harness env:
  ```
  C_PEER_ID=12D3KooWQBrAjZHCJ9zRgxeVAsMzNiZMV7oGRSEWjEYv5b5qYHYT
  P2P_PORT=16111 LIBP2P_PORT=16112 RPC_PORT=17110 NETWORK_FLAG="--simnet"
  REPO_PATH=/home/ubuntu/rusty-kaspa BIN_PATH=/home/ubuntu/rusty-kaspa/target/release/kaspad
  APPDIR_C=/home/ubuntu/kaspa/node-c APPDIR_A=/home/ubuntu/kaspa/node-a APPDIR_B=/home/ubuntu/kaspa/node-b
  RUST_LOG=info,kaspa_p2p_libp2p=debug,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=debug
  SSH_JUMP=root@10.0.3.11
  bash tcp-hole-punch/lab/hole_punch_lab.sh start
  ```
- Mode: simnet (`--simnet` via NETWORK_FLAG).

## Status snapshots (getLibp2pStatus)
- Node C: `mode: Full`, `peer_id: 12D3KooWQBrAjZHCJ9zRgxeVAsMzNiZMV7oGRSEWjEYv5b5qYHYT`, identity persisted.
- Node A: `mode: Full`, `peer_id: 12D3KooWA3TfEwQLeFKsJMZWcK16PjVNs7rhtMBEQyW54CkWCVCZ`, persisted.
- Node B: `mode: Full`, `peer_id: 12D3KooWSZgX92Muea2vbRkjLtHrKVcVCnC553rV4uq5gujDqZxq`, persisted.

## Listener snapshots (ss -ltnp | grep kaspad)
- Node C: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node A: 16111, 16112, 17110.
- Node B: 16111, 16112, 17110.

## Key log excerpts
- Protocol advertisement (Identify shows DCUtR now):
  - Node C: `identify received from 12D3KooWA3Tf...: protocols=[..., /libp2p/dcutr, ...] listen_addrs=[/ip4/192.168.1.10/tcp/16112, /ip4/10.0.3.61/tcp/16112, /ip4/127.0.0.1/tcp/16112]`
  - Node C: same for B `protocols=[..., /libp2p/dcutr, ...] listen_addrs=[/ip4/10.0.3.62/tcp/16112, ...]`.
- Reservations:
  - A/B: `libp2p reservation attempt ... accepted by 12D3KooWQBr... , renewal=false`.
  - C: `ReservationReqAccepted` for both A and B.
- DCUtR behaviour:
  - C: `libp2p dcutr: skipping dial-back to <A/B>: not connected via relay`.
  - A/B: `libp2p dcutr: skipping dial-back to 12D3KooWQBr...: not connected via relay`.
  - No hole-punch success logs.
- Bridging / peers:
  - C: inbound streams from A/B, no `connect_with_stream failed` lines; `Kaspa peer registered from libp2p stream` for both.
  - A: `P2P Connected to outgoing peer [fdff::5e9:a545:c4eb:8fee]:50411 (outbound: 1)`.
  - B: same outgoing peer line to the same address (C).
- Connection manager counts:
  - A: `outgoing: 1/8` stable.
  - B: `outgoing: 1/8` stable.
  - C: `outgoing: 0/8` (only inbound peers).

## Outcome
- Improvements observed: `/libp2p/dcutr` now advertised in Identify; no “peer does not support dcutr” messages. Stream bridge race appears resolved—no connect_with_stream errors on C, and peers register cleanly.
- Gap: DCUtR not attempted because connections are classified as Direct (skip reason “not connected via relay”), so no hole punch occurs and A/B never connect to each other. Only A→C and B→C Kaspa peers are active; C remains inbound-only.
- Next focus: ensure A/B connections to each other are relayed so dial-back can trigger, or force DCUtR attempt even when direct classification occurs in this NAT topology. No mainnet run in this pass.***
