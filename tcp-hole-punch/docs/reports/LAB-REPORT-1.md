# Lab Report 1 – Proxmox NAT libp2p run

## Topology
- Router A (OpenWrt full-cone NAT) on 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) on 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) on 10.0.3.50, no router in front.

## Prep and build
- Stopped prior kaspad/kaspawallet on all three nodes.
- Archived previous checkouts/data (`rusty-kaspa-old-*`, `kaspa-old-*`), fresh clone on each node:
  `git clone --branch tcp-hole-punch-clean --origin origin --single-branch https://github.com/elldeeone/rusty-kaspa.git rusty-kaspa`
- Build (all nodes): `source ~/.cargo/env && cd rusty-kaspa && cargo build --release --features libp2p --bin kaspad`

## Run commands (libp2p full mode)
- Node C (relay/anchor, persistent ID):
  ```
  nohup env RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info \
    /home/ubuntu/rusty-kaspa/target/release/kaspad \
    --simnet \
    --appdir=/home/ubuntu/kaspa/node-c \
    --loglevel=debug \
    --nodnsseed \
    --disable-upnp \
    --listen=0.0.0.0:16111 \
    --rpclisten=0.0.0.0:17110 \
    --libp2p-mode=full \
    --libp2p-helper-listen=0.0.0.0:38080 \
    --libp2p-identity-path=/home/ubuntu/kaspa/node-c/libp2p.id \
    >>/home/ubuntu/kaspa/node-c/kaspad.log 2>&1 &
  ```
- Node A (behind router A):
  ```
  KASPAD_LIBP2P_RESERVATIONS=/ip4/10.0.3.50/tcp/16111/p2p/12D3KooWSrgPKCU84mUiLNL5CN2yTGPurBs2nhfnPtbfbZTDAhFW \
  KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/10.0.3.61/tcp/16111 \
  RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info \
  nohup /home/ubuntu/rusty-kaspa/target/release/kaspad \
    --simnet \
    --appdir=/home/ubuntu/kaspa/node-a \
    --loglevel=debug \
    --nodnsseed \
    --disable-upnp \
    --listen=0.0.0.0:16111 \
    --rpclisten=0.0.0.0:17110 \
    --libp2p-mode=full \
    --libp2p-helper-listen=127.0.0.1:38080 \
    --libp2p-identity-path=/home/ubuntu/kaspa/node-a/libp2p.id \
    >>/home/ubuntu/kaspa/node-a/kaspad.log 2>&1 &
  ```
- Node B (behind router B):
  ```
  KASPAD_LIBP2P_RESERVATIONS=/ip4/10.0.3.50/tcp/16111/p2p/12D3KooWSrgPKCU84mUiLNL5CN2yTGPurBs2nhfnPtbfbZTDAhFW \
  KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/10.0.3.62/tcp/16111 \
  RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info \
  nohup /home/ubuntu/rusty-kaspa/target/release/kaspad \
    --simnet \
    --appdir=/home/ubuntu/kaspa/node-b \
    --loglevel=debug \
    --nodnsseed \
    --disable-upnp \
    --listen=0.0.0.0:16111 \
    --rpclisten=0.0.0.0:17110 \
    --libp2p-mode=full \
    --libp2p-helper-listen=127.0.0.1:38080 \
    --libp2p-identity-path=/home/ubuntu/kaspa/node-b/libp2p.id \
    >>/home/ubuntu/kaspa/node-b/kaspad.log 2>&1 &
  ```
- Logs live in `/home/ubuntu/kaspa/node-{a,b,c}/kaspad.log`. PIDs are recorded in the same dirs.

## Status snapshots
- Node C `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWSrgPKCU84mUiLNL5CN2yTGPurBs2nhfnPtbfbZTDAhFW`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
- Node A `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWEXYjrmHX1qnW97tPeRdZYxyD4ZA3eDGhnFSMkq6RkP8k`, identity persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
- Node B `getLibp2pStatus`: `mode: Full`, `peer_id: 12D3KooWHmsP3wovWDe3fKMwEAU1WsiSEKywRScDXJ9jVRCNTTmY`, identity persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.

## Observations/issues
- All nodes log: `libp2p mode enabled; transport is currently unimplemented and outbound connections will return NotImplemented`.
- `ss -ltnp` on each host shows only P2P `16111` and RPC `17110`; no libp2p listener/socket beyond the standard kaspad ports.
- Env vars for reservations/external multiaddrs are present in process environments, but no reservation attempts or peer connections appear in logs; connection loop stays at `outgoing: 0/8`.
- Result: Node A/B never establish libp2p connectivity to Node C (relayed or direct). Success criteria not met due to the stubbed libp2p transport.

## Next steps
- Wire the libp2p adapter into the daemon lifecycle (start `Libp2pService`, honor `KASPAD_LIBP2P_RESERVATIONS`/external addresses, and expose a deterministic listen multiaddr).
- Remove the “transport unimplemented” path or provide a TCP fallback when libp2p is enabled but inactive.
- Once transport is active, rerun the Proxmox NAT scenario with the same commands/script (`tcp-hole-punch/lab/hole_punch_lab.sh`) to verify A/B reachability via C.
