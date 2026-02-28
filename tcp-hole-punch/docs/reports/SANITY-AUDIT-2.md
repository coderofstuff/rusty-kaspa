# SANITY AUDIT – tcp-hole-punch-clean

## Summary
tcp-hole-punch-clean keeps the libp2p/DCUtR surface isolated: consensus/IBD/mining/mempool are byte-for-byte with upstream, and libp2p is fully feature-gated/off by default. The libp2p stack mirrors the working “dirty” branch (AutoNAT, DCUtR dial-back, observed address seeding, relay budgeting, helper API) but lives in `components/p2p-libp2p` with clean wiring into the existing P2P router. Only non-libp2p runtime delta spotted is a DB version-3 upgrade path in `kaspad/src/daemon.rs` (ghostdag cleanup) that is unrelated to hole-punching and should be double-checked for scope.

## Diff vs upstream/master
- Runtime diffs are confined to the agreed surface: `.github/workflows/ci.yaml` feature checks, Cargo manifest/lock additions, libp2p crate and wiring (`components/p2p-libp2p/**`, `kaspad/src/{args,daemon,libp2p}.rs`, `protocol/p2p/**`, `protocol/flows/**`, address/connection metadata, RPC additions, docs/tests under `tcp-hole-punch/**`).
- `kaspad/src/daemon.rs` carries an extra DB upgrade path (version 3 -> 4 ghostdag cleanup) not tied to libp2p; worth confirming it is intentional for this PR.
- No runtime diffs in `consensus/`, `consensus/core/`, `protocol/flows/src/ibd/`, `mining/`, `mempool/`, `consensus/core/src/hashing/`, `consensus/core/src/merkle.rs`, `utils/src/hex.rs`.

## Wire surface (NetAddress)
- Added `services` bitmask and optional `relay_port` to NetAddress; P2P proto (`protocol/p2p/proto/p2p.proto`), conversions, and handshake (`flow_context.rs`) propagate these when libp2p is enabled (service flag = `NET_ADDRESS_SERVICE_LIBP2P_RELAY`, relay_port set to helper bind port).
- Equality/Hash remain IP:port only; Borsh/Proto defaults to `0`/`None`, so legacy peers ignore the new fields safely. AddressManager merges metadata when seeing the same IP:port again.
- RPC and WRPC reuse the same NetAddress type; no enum repurposing or version bumps.

## AutoNAT & allow-private
- Config default: client+server enabled; server gated to global IPs (`server_only_if_public=true`). Yamux keeps 32 MiB windows.
- Flag/env `--libp2p-autonat-allow-private` / `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true` flips `server_only_if_public=false` for lab/private relays.
- Helper API binds only when `--libp2p-helper-listen` is set. Docs in `tcp-hole-punch/LIBP2P-OPERATOR.md` match the code (public-only by default, explicit opt-in for private labs).

## DCUtR vs dirty branch (origin/tcp-hole-punch)
| Area | Dirty branch | Clean branch | Reason / Comment |
| --- | --- | --- | --- |
| Architecture | Custom `hole_punch_bridge` wiring, mixed with broader code drift | New `components/p2p-libp2p` crate plugged into existing `ConnectionHandler`/Router, consensus untouched | Isolation and maintainability |
| AutoNAT | Client+server enabled | Same posture; default public-only, allow-private flag for labs | Keep lab success while mainnet-safe defaults |
| Identify push | Enabled by default | Push disabled | Avoid relay address flooding |
| DCUtR advertisement & candidates | DCUtR protocol advertised via behaviour; observed addrs fed to DCUtR cache | `DcutrBootstrapBehaviour` forces `/libp2p/dcutr` advertise; Identify observed/additional listen addrs added as external candidates | Ensures peers see DCUtR support and have dialback candidates |
| Dial-back on relay | Forced dial-back when connected via relay and peer supports DCUtR | Same logic, tracks relay ID and only dialer initiates bridge to avoid duplicate streams | Preserve working behaviour, remove races |
| Yamux tuning | 32 MiB windows | 32 MiB windows | Kept for throughput |
| Inbound relay limits | Private/relay limits tracked in status | Inbound soft caps per relay/unknown bucket via `connectionmanager` | Budget relay inbound load explicitly |

## Behaviour parity checkpoints
- Swarm setup: Identify advertises DCUtR, AutoNAT configured per defaults above, observed/listen addresses fed into external candidates.
- Transport metadata: libp2p streams carry `PathKind` (direct/relay) and synthetic addresses for accounting; handshake services include libp2p relay bit and optional helper relay_port.
- Dial-back: when connected via relay to a DCUtR-capable peer with no outbound connection, `maybe_request_dialback` dials via the active relay; bridge opens from the dialer side only.

## Build/tests & local runs
- Commands run: `cargo build`; `cargo check --no-default-features`; `cargo build --features libp2p --bin kaspad`; `cargo test -p kaspa-p2p-libp2p --lib --tests`; `cargo test -p kaspad --lib --features libp2p` (all pass; only Yamux deprecation warnings).
- Local smoke:
  - Default binary (libp2p not compiled): TCP listeners only; no libp2p logs (UPnP attempted until disabled).
  - libp2p compiled, `--libp2p-mode=off`: TCP-only startup, no libp2p logs.
  - libp2p full: dedicated port bound (`/ip4/127.0.0.1/tcp/20780`), helper bound only when flag set, AutoNAT client+server enabled (public-only), DCUtR advertised/seeding logged; clean shutdown.

## Risks / TODOs
- Non-libp2p DB upgrade block in `kaspad/src/daemon.rs` could be out-of-scope for this PR; confirm intent.
- Yamux window setters are deprecated upstream; future libp2p upgrades may need replacement APIs.
- DCUtR harness tests remain `#[ignore]` (upstream relay drop panic); lab rerun recommended on Proxmox once PR is frozen.
