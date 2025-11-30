# DCUtR Regression – Bridge Mode Fix Notes

## 0. Setup & Orientation
- Branch: `tcp-hole-punch-bridge-regression-fix` (from `tcp-hole-punch-bridge-mode` @ `1d2ab959`).
- Sanity builds/tests at start:
  - `cargo check` ✅
  - `cargo test -q -p kaspa-p2p-libp2p` ✅
  - `cargo test -q -p kaspad` ✅

## 1. High-level diff (24cf8d6b vs 656dfc5e)
| File | Summary |
| --- | --- |
| components/connectionmanager/src/lib.rs | Added role config/global private-cap, inbound handling, helper tests. |
| components/p2p-libp2p/src/config.rs | Added `Mode::Bridge`, `Role`, private inbound cap, builder plumbing. |
| components/p2p-libp2p/src/transport.rs | Refactored `Libp2pOutboundConnector::connect` with mode match, bridge cooldown/TCP fallback. |
| kaspad/src/args.rs | CLI parsing for bridge mode and libp2p role. |
| kaspad/src/libp2p.rs | Role/plumbing, bridge mode wiring, default private cap, status mapping. |
| kaspad/src/daemon.rs | Outbound connector match per mode, role-aware advertisement, role config injection. |

### 1.2 Can affect `--libp2p-mode=full`?
| File / Area | Can affect full? | Notes |
| --- | --- | --- |
| config.rs: Mode/Role | Yes (Mode::Bridge addition, Role defaults) | Mode plumbing touches all modes; must ensure Full unchanged. |
| transport.rs: connect() match | **Yes (suspected)** | Refactor introduced mode match + cooldown/fallback; needs to preserve Full semantics. |
| daemon.rs: connector wiring | Possibly | Match on mode; Full should still choose libp2p connector. |
| args.rs/libp2p.rs | Possibly | Mode/role resolution; ensure Full remains Full and not mapped to Bridge/Off. |
| connectionmanager | Unlikely | Inbound caps/role only; outbound/DCUtR path unaffected but verify. |

## 2. Intended semantics (target)
| Mode | Outbound connector behaviour | Swarm/libp2p runtime | Notes |
| --- | --- | --- |
| off | Pure TCP (`TcpConnector`) | Disabled | Matches upstream/master. |
| full/helper | Libp2p only for outbound dials (no TCP fallback); DCUtR available | Enabled | Must match `24cf8d6b` behaviour. |
| bridge | TCP for normal outbound; libp2p runtime still running for helper/DCUtR | Enabled | Hybrid/mainnet-safe mode. |

- Full mode invariant: behaviour identical to commit `24cf8d6b`; bridge/role additions must not change DCUtR in Full.

### 2.2 Roles (expected)
- Public: advertise `NET_ADDRESS_SERVICE_LIBP2P_RELAY` + `relay_port`; keep existing libp2p inbound split.
- Private/Auto: do **not** advertise relay bit/port; enforce libp2p private inbound cap; TCP inbound unchanged.
- NAT lab expectation: relay node resolves to Public; private nodes resolve to Private → should not alter libp2p DCUtR paths.

## 3. Regression reasoning snapshots
- `Mode::effective()/is_enabled()`: Helper→Full; Bridge→Bridge; is_enabled true for Full/Helper/Bridge only. No Full→Bridge mapping.
- `Libp2pOutboundConnector::connect`: refactored to a mode `match`; Bridge branch adds libp2p attempt + per-address cooldown then TCP fallback. Full/Helper share branch with provider resolution. Need to ensure Full never uses bridge cooldown/fallback and mirrors pre-bridge logic.
- Config/daemon wiring: `--libp2p-mode=full` should produce `Mode::Full` and select `Libp2pOutboundConnector`; Bridge selects TCP outbound. Verify role handling doesn’t suppress DCUtR paths.

## 4. Proposed fix (pre-coding plan)
- Keep `Mode::effective()/is_enabled()` simple; no Full→Bridge mapping.
- In `Libp2pOutboundConnector::connect`, explicitly separate Full/Helper from Bridge:
  - Full/Helper: use the pre-bridge libp2p-only dial path (no cooldown, no TCP fallback), identical metadata handling.
  - Bridge: retain bridge-specific cooldown + TCP fallback.
  - Off: TCP fallback with libp2p capability cleared.
- Add unit tests to lock in Mode semantics and connector behaviour (Full uses provider, Bridge uses fallback, Off uses TCP).
- Ensure role/inbound logic remains isolated from outbound/DCUtR (Full mode unaffected).

## 6. Tests to run after fix
- `cargo test -q -p kaspa-p2p-libp2p`
- `cargo test -q -p kaspad`
  - Results: both ✅ (warnings only from upstream yamux deprecations; ignored DCUtR tests remain #[ignore]).

## 7. Root cause (to fill after fix)
- Bridge refactor moved `Libp2pOutboundConnector::connect` behind a mode `match` and reused the new bridge cooldown/fallback plumbing without an explicit Full-only path. Full-mode dials could end up taking the bridge decision path after a failed libp2p attempt, skipping the libp2p-only retry behaviour DCUtR depends on.
- Fix: restore a dedicated libp2p-only branch for `Full|Helper` (pre-bridge behaviour) while keeping cooldown/TCP fallback isolated to `Bridge`; add unit tests to pin these behaviours.

## 8. Handoff (for lab engineer; fill after fix)
- Branch/commit to test: `tcp-hole-punch-bridge-regression-fix` @ HEAD (after fix).
- What changed: Outbound connector now keeps Full/Helper on the pre-bridge libp2p-only path; bridge fallback/cooldown is isolated. Added connector mode tests.
- What to exercise: `--libp2p-mode=full` DCUtR (overlay lab) should match baseline `24cf8d6b`; `--libp2p-mode=bridge` remains hybrid/TCP-safe. Roles unchanged (public advertises relay bit/port; private does not).
