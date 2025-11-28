# TCP Hole Punch Clean Branch Review

## Scope
- Allowed surface touched: `components/p2p-libp2p/**`, `kaspad/src/{libp2p.rs,daemon.rs,args.rs}`, `components/{connectionmanager,addressmanager}`, `utils/src/networking.rs`, libp2p RPC/proto, and `tcp-hole-punch/**` docs/lab harnesses. Changes there look aligned with the libp2p/DCUtR goal.
- Out-of-bounds diffs vs `upstream/master` (all unrelated to libp2p; recommend reverting to master):

| Area / File | In scope? | Keep or Revert | Reason |
| --- | --- | --- | --- |
| consensus/core/src/header.rs | No | Revert | Adds CompressedParents RLE encoding, new APIs/tests changing header layout. |
| consensus/core/src/hashing/header.rs | No | Revert | Hashing now uses compressed parents iterators. |
| consensus/core/src/config/genesis.rs | No | Revert | Genesis header now uses CompressedParents default. |
| consensus/core/src/errors/mod.rs | No | Revert | Adds new header error module. |
| consensus/core/src/errors/header.rs | No | Revert | New CompressedParentsError type. |
| consensus/client/src/header.rs | No | Revert | JS bridge converts parents to CompressedParents. |
| consensus/client/src/error.rs | No | Revert | New CompressedParents error variant. |
| consensus/src/consensus/factory.rs | No | Revert | DB version bump to 5 (compressed headers). |
| consensus/src/model/stores/headers.rs | No | Revert | New compressed header store/prefix and fallback deserialization. |
| consensus/src/pipeline/body_processor/body_validation_in_isolation.rs | No | Revert | Tests updated for CompressedParents. |
| consensus/src/pipeline/header_processor/post_pow_validation.rs | No | Revert | Parent checks rewritten for compressed iterator. |
| consensus/src/processes/parents_builder.rs | No | Revert | Parents builder now returns CompressedParents and alters parents_at_level logic. |
| consensus/src/processes/pruning_proof/build.rs | No | Revert | Uses expanded_iter on compressed parents. |
| consensus/src/test_helpers.rs | No | Revert | Test helper headers use CompressedParents. |
| mining/src/testutils/consensus_mock.rs | No | Revert | Mock header construction uses CompressedParents. |
| database/src/access.rs | No | Revert | Adds has/read_with_fallback helpers for new header store. |
| database/src/registry.rs | No | Revert | Adds CompressedHeaders prefix; marks legacy headers. |
| utils/src/iter.rs | No | Revert | Adds RLE helpers only used by compressed-parents change. |

No justified exceptions found; none of these are required for libp2p/DCUtR.

## Behaviour
- Build/test matrix (all ✅):
  - `cargo build` (warnings: unused `IGetLibp2pStatus*` aliases in `rpc/wrpc/wasm/src/client.rs`).
  - `cargo check --no-default-features` (same warnings).
  - `cargo build --features libp2p --bin kaspad` (yamux deprecation warnings for buffer/window setters).
  - `cargo test -p kaspa-p2p-libp2p --lib --tests` (yamux warnings; unused mock/test helpers; ignored DCUtR tests noted).
  - `cargo test -p kaspad --lib --features libp2p` (yamux warnings only).
- Runtime smoke:
  - Default (no libp2p feature): `--simnet --nodnsseed --disable-upnp --nogrpc` → only TCP P2P listen on `0.0.0.0:16511`, no libp2p logs.
  - Libp2p build, mode off: same CLI + `--libp2p-mode=off` → behaviour identical to default; no libp2p/helper logs.
  - Libp2p full: `--libp2p-mode=full --libp2p-listen-port=18080 --libp2p-helper-listen=127.0.0.1:18100` → libp2p swarm starts, peer ID logged, AutoNAT client+server enabled, DCUtR advertised, relay client/server active, helper binds as requested; no Tokio runtime panic observed.

## DCUtR / AutoNAT / Bridging
- Stack matches intent: TCP → Noise → Yamux, relay client/server, DCUtR always enabled, Identify push listen updates disabled, stream bridge over `/kaspad/transport/1.0.0`.
- Identify feeds observed/listen addrs into swarm external addresses (excluding relay addrs); DCUtR pre-seeded with advertise/external addrs; `/libp2p/dcutr` advertised via DcutrBootstrapBehaviour.
- Dial-back logic present: listener-side relayed connections trigger dial-back via active relay when peer supports DCUtR; avoids double-bridge by only dialer side initiating unless resolving pending relay dial.
- Transport metadata stable: synthetic addresses keyed by PeerId with relay path tracked separately; ConnectionManager respects relay/unknown caps.
- AutoNAT: default `server_only_if_public=true` (global only). Flag/env `--libp2p-autonat-allow-private` / `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE` flips to allow private IPs. Operator doc matches; lab harness sets it true for Proxmox.

## Merge Blockers
- Consensus/DB drift: CompressedParents/RLE, header store prefix, DB version bump, and dependent API changes across consensus/mining/utils/database are all out of scope and violate the clean-branch contract. Revert to `upstream/master` versions.
- Docs mismatch: PLAN-CLEANUP and SANITY-REPORT claim consensus rollback to master, but code still diverges (compressed parents). Needs correction after revert.

## Nice-to-haves / Follow-ups
- Update LIBP2P-OPERATOR.md to reflect that the helper API now binds when `--libp2p-helper-listen` is set (currently labelled “stubbed”).
- Address build warnings: yamux config setters are deprecated; unused WRPC libp2p status aliases; unused mocks in libp2p transport tests.
- Consider ensuring runtime smoke scripts include explicit `--listen`/`--rpclisten` to avoid port reuse during repeated runs.***
