# Lab Report 5 – Proxmox NAT libp2p/DCUtR run (mainnet, 49f24760)

## Topology
- Router A (OpenWrt full-cone NAT) at 10.0.3.61; Node A 192.168.1.10/24 gw 192.168.1.1 (vmbr10).
- Router B (OpenWrt full-cone NAT) at 10.0.3.62; Node B 192.168.2.10/24 gw 192.168.2.1 (vmbr20).
- Node C (public/relay anchor) at 10.0.3.50, no NAT in front.
- Ports: P2P 16111, libp2p 16112 (P2P+1), RPC 17110.

## Build steps (all nodes A/B/C)
- Stopped kaspad, removed old appdirs under `/home/ubuntu/kaspa/node-{a,b,c}`.
- Fresh sync to `origin/tcp-hole-punch-clean` (HEAD 49f24760) with `git fetch && git reset --hard origin/tcp-hole-punch-clean`.
- Rebuilt clean: `cargo clean && cargo build --release --features libp2p --bin kaspad`.

## Run commands / harness
- Harness `tcp-hole-punch/lab/hole_punch_lab.sh` now honours `NETWORK_FLAG` (default `--simnet`, set `NETWORK_FLAG=""` for mainnet). For this run we used mainnet (no `--simnet`).
- Node C start (mainnet):
  ```
  RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info \
  KASPAD_LIBP2P_LISTEN_PORT=16112 \
  nohup /home/ubuntu/rusty-kaspa/target/release/kaspad \
    --appdir=/home/ubuntu/kaspa/node-c \
    --loglevel=debug \
    --nodnsseed \
    --disable-upnp \
    --listen=0.0.0.0:16111 \
    --rpclisten=0.0.0.0:17110 \
    --libp2p-mode=full \
    --libp2p-listen-port=16112 \
    --libp2p-helper-listen=0.0.0.0:38080 \
    --libp2p-identity-path=/home/ubuntu/kaspa/node-c/libp2p.id \
    >>/home/ubuntu/kaspa/node-c/kaspad.log 2>&1 &
  ```
  PeerId: `12D3KooWESrJQ6CmknnH1Z62qpDotWdXwam18oaT1QHgR71mz9Qh`.
- Node A start (behind Router A):
  ```
  KASPAD_LIBP2P_RESERVATIONS=/ip4/10.0.3.50/tcp/16112/p2p/12D3KooWESrJQ6CmknnH1Z62qpDotWdXwam18oaT1QHgR71mz9Qh \
  KASPAD_LIBP2P_EXTERNAL_MULTIADDRS=/ip4/10.0.3.61/tcp/16112 \
  KASPAD_LIBP2P_LISTEN_PORT=16112 \
  RUST_LOG=info,libp2p_swarm=info,libp2p_dcutr=debug,libp2p_identify=info \
  nohup /home/ubuntu/rusty-kaspa/target/release/kaspad \
    --appdir=/home/ubuntu/kaspa/node-a \
    --loglevel=debug \
    --nodnsseed \
    --disable-upnp \
    --listen=0.0.0.0:16111 \
    --rpclisten=0.0.0.0:17110 \
    --libp2p-mode=full \
    --libp2p-listen-port=16112 \
    --libp2p-helper-listen=127.0.0.1:38080 \
    --libp2p-identity-path=/home/ubuntu/kaspa/node-a/libp2p.id \
    >>/home/ubuntu/kaspa/node-a/kaspad.log 2>&1 &
  ```
  PeerId: `12D3KooWHAqC2vhnUWbmDrKmBApvxrinsYE81hiF2p6Fi8onqNmP`.
- Node B start (behind Router B) mirrored Node A with external multiaddr `/ip4/10.0.3.62/tcp/16112`, appdir `/home/ubuntu/kaspa/node-b`, helper `127.0.0.1:38080`. PeerId: `12D3KooWJ4jPFLbNPAmvVd2rcnkLzRJEXLG2Tw1Jjgtc8QsD47GG`.

## Status snapshots
- `getLibp2pStatus` (gRPC example, mainnet):
  - Node C: `mode: Full`, `peer_id: 12D3KooWESrJQ6CmknnH1Z62qpDotWdXwam18oaT1QHgR71mz9Qh`, identity persisted at `/home/ubuntu/kaspa/node-c/libp2p.id`.
  - Node A: `mode: Full`, `peer_id: 12D3KooWHAqC2vhnUWbmDrKmBApvxrinsYE81hiF2p6Fi8onqNmP`, persisted at `/home/ubuntu/kaspa/node-a/libp2p.id`.
  - Node B: `mode: Full`, `peer_id: 12D3KooWJ4jPFLbNPAmvVd2rcnkLzRJEXLG2Tw1Jjgtc8QsD47GG`, persisted at `/home/ubuntu/kaspa/node-b/libp2p.id`.

## Listener snapshots (sudo ss -ltnp | grep kaspad)
- Node C: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node A: 16111 (P2P), 16112 (libp2p), 17110 (RPC).
- Node B: 16111 (P2P), 16112 (libp2p), 17110 (RPC).

## Logs / behaviour
- Node C:
  - libp2p listening on `/ip4/127.0.0.1/tcp/16112` and `/ip4/10.0.3.50/tcp/16112`.
  - Accepted reservations from A (`ReservationReqAccepted` for `12D3KooWHAqC2v...`) and B (`12D3KooWJ4jPFLbNP...`).
  - Inbound streams from A and B over Direct path show `libp2p_bridge: inbound stream ... handing to Kaspa`, but `connect_with_stream` fails with `hyper::Error(... broken pipe)` → `connection error`; connection manager stays `outgoing: 0/8`.
- Node A:
  - Reservation dial to `/ip4/10.0.3.50/tcp/16112/p2p/<C_PEER>` succeeds; Noise/Yamux/identify complete.
  - Bridge attempt to Kaspa fails: `libp2p gRPC send request ... hyper::Error(Io, "connection closed because of a broken pipe")` followed by `libp2p_bridge: outbound connect_with_stream failed: connection error`.
  - Connection manager lines remain `outgoing: 0/8 , connecting: 0`.
- Node B:
  - Same pattern as Node A: reservation to C succeeds, identify shows C addresses; bridge attempt fails with broken-pipe gRPC error; connection manager stays empty.
- No DCUtR events logged on any node; no Kaspa peers formed between A↔C or B↔C, and A/B never see each other.

## Outcome
- Successes: mainnet run confirms dedicated libp2p port is used; A/B can reach C’s libp2p listener and reservations are accepted; Noise/Yamux/identify succeed with real metadata.
- Gap: bridging libp2p streams into the Kaspa peer layer fails on all nodes with `connect_with_stream`/gRPC broken pipe errors, leaving connection manager at `0/8` and no Kaspa peers (no DCUtR activity). Behaviour mirrors prior simnet runs, indicating the issue is not simnet-specific.
