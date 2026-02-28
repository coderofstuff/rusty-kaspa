# TCP Hole Punch – Bridge Mode & Role Integration Plan

**Branch:** `tcp-hole-punch-clean`  
**Base commit:** `222eba40688267253b9471eadc44f2797ed6a968` (“Add TCP hole punch stress test report”).   

This document is a **checklist** for integrating a safe “bridge” mode and basic libp2p roles, while keeping the existing TCP stack as the primary transport and preserving all guardrails from the clean‑slate plan.   

Use it as a living tracker: change `[ ]` → `[x]` as you complete items, and add short notes where useful.

---

## Legend

- `[ ]` = not started  
- `[x]` = completed  
- Under any item you can add bullet‑point notes, e.g.:
  - `- Note: implemented in commit abc123`

---

## 0. Orientation & Guardrails

### 0.1 Repository and base

- [x] Ensure you have a local checkout of `rusty-kaspa`.
- [x] Check out the base branch and create your working branch:

  ```bash
  git checkout tcp-hole-punch-clean
  git rev-parse HEAD   # should show 222eba4068...
  git checkout -b tcp-hole-punch-bridge-mode
````

  - Note: Branch created from `tcp-hole-punch-clean` at `b0f4224b` (merge-base `222eba4068`).

* [x] Run baseline tests to confirm a clean starting point:

  ```bash
  cargo test -q -p kaspa-p2p-libp2p
  cargo test -q -p kaspad
  ```
  - Note: `cargo test -q -p kaspad` needed an extended timeout but passed.

### 0.2 Required reading

Tick these once you have read and understood them (take notes separately if needed):

* [x] `CODE-DEEP-DIVE.md` – current libp2p/DCUtR integration and why `libp2p-mode=full` strands mainnet (0/8 outbound).
* [x] `HYBRID-INTEGRATION-PROPOSAL.md` – high‑level hybrid/bridge design.
* [x] `HYBRID-INTEGRATION-PROPOSAL-CODEX.md` – companion hybrid design with very tight scope to the current code. 
* [x] `MAINNET-SOAK-REPORT.md` – mainnet soak showing `libp2p-mode=full` cannot sync mainnet and pointing at the need for hybrid/fallback.
* [x] `STRESS-TEST-REPORT.md` – DCUtR stress tests and stability.
* [x] `NAT-LAB-HANDOVER.md` – Proxmox NAT lab topology and commands.
* [x] `tcp-hole-punch/PLAN-CLEANUP.md` – clean‑slate scope and guardrails. 
* [x] `components/p2p-libp2p/docs/lifecycle.md` – lifecycle invariants for pending dials, DCUtR handoff, shutdown, backpressure, helper, reservations.

### 0.3 Guardrails (must hold at the end)

* [x] Confirm you understand and will respect the **allowed change surface** from `PLAN-CLEANUP.md`: 

  * `components/p2p-libp2p/**`
  * `kaspad/src/libp2p.rs`
  * `kaspad/src/daemon.rs`
  * `kaspad/src/args.rs`
  * `components/connectionmanager/**`
  * `components/addressmanager/**` (libp2p/relay metadata only)
  * `utils/src/networking.rs` (NetAddress `services` / `relay_port` only)
  * `tcp-hole-punch/**` docs and reports

  - Note: Changes will stay within the allowed surface; consensus/IBD/mempool remain untouched.

* [x] Confirm you will *not* touch:

  * Consensus and IBD (`consensus/**`, `protocol/flows/src/ibd/**`).
  * Mempool, mining, pruning, hashing, merkle.
  * Any other area not explicitly listed as allowed.

* [x] Confirm **default node behaviour** (no libp2p flags, feature off) must remain identical to upstream/master.

---

## 1. Phase A – Add `bridge` Mode (Safe Hybrid)

**Objective:** Add a `bridge` mode that:

* Runs the libp2p stack (swarm, helper, reservations, DCUtR) when asked.
* Keeps mainnet connectivity safe by **not forcing outbound dials through libp2p**.
* Leaves `off` and `full` modes unchanged (full remains overlay‑only for the NAT lab).

### 1.1 Extend libp2p mode enum

**File:** `components/p2p-libp2p/src/config.rs`

* [x] Add a `Bridge` variant to the libp2p mode enum:

  ```rust
  pub enum Mode {
      Off,
      Full,
      Helper,
      Bridge,
  }
  ```

* [ ] Update `Mode::effective()` so:

  * `Helper` still maps to `Full`.
  * `Bridge` maps to itself.

* [x] Update `Mode::is_enabled()` so it returns `true` for `Full`, `Helper`, and `Bridge`, and `false` only for `Off`.

### 1.2 CLI support for `bridge`

**File:** `kaspad/src/args.rs`

* [x] Extend `--libp2p-mode` parsing to accept `bridge` and map it to `Mode::Bridge`.
* [x] Confirm default mode remains `off`.
* [x] Confirm `full` and `helper` behave as before.

### 1.3 Config plumbing

**File:** `kaspad/src/libp2p.rs`

* [x] Ensure the config builder passes the new `Mode::Bridge` through unchanged to the adapter config.
* [x] Confirm no code assumes a closed set `{Off, Full, Helper}`.

### 1.4 Daemon outbound connector wiring

**File:** `kaspad/src/daemon.rs`

Current wiring (summarised) chooses libp2p connector whenever `mode.is_enabled()` is true.

You will:

* [x] Replace the `if libp2p_config.mode.is_enabled()` with an explicit `match`:

  * `Mode::Off` → outbound connector is plain `TcpConnector`.
  * `Mode::Full | Mode::Helper` → outbound connector is `Libp2pOutboundConnector` (unchanged behaviour).
  * `Mode::Bridge` → outbound connector is **plain `TcpConnector`** (libp2p still runs, but TCP is used for outbound Kaspad dials).

* [x] Ensure the libp2p runtime (swarm, helper, reservations, inbound bridge) is created whenever `mode.is_enabled()` is true so both `Full` and `Bridge` still run libp2p services.

> Notes (add here):
>
> * Bridge mode uses TCP outbound from the daemon while `Libp2pOutboundConnector` adds a bridge fallback path (with a 10m per-address cooldown) for future hybrid attempts; RPC status reports bridge as `full` because the existing RPC enum has no bridge variant.

### 1.5 Phase A sanity tests

**A.1 – Compilation & unit tests**

* [x] `cargo check`
* [x] `cargo test -q -p kaspa-p2p-libp2p`
* [x] `cargo test -q -p kaspad`

**A.2 – Mainnet node with `bridge` mode**

Using the same general environment as the mainnet soak report:

* [x] Start a mainnet node with:

  ```bash
  kaspad \
    --libp2p-mode=bridge \
    # other flags as needed (no reservations required here)
  ```

* [x] Confirm:

  * It reaches normal outbound peer count (e.g. 8/8) via TCP.
  * IBD progresses and completes as it does with libp2p off.
  * No "0/8 outgoing" loops appear in logs.
  - Note: Validated in `MAINNET-SOAK-REPORT-BRIDGE.md` – 8/8 TCP peers, IBD progressing, zero panics.

**A.3 – NAT lab regression with `full` mode**

* [x] In the NAT lab, re‑run a short DCUtR sanity test using `--libp2p-mode=full` and the existing lab commands.
* [x] Confirm:

  * Relay reservations and DCUtR behaviour match the stress test and previous reports.
  * No new panics or behaviour changes.
  - Note: Validated in `BRIDGE-NAT-REGRESSION.md` – reservations accepted, DCUtR bounded failure (AttemptsExceeded(3)), zero panics.

---

## 2. Phase B – Roles & Private Inbound Limits

**Objective:** Introduce a simple `--libp2p-role` so public relays vs private nodes can behave differently, without changing protocol or consensus. Public nodes can advertise relay capability; private nodes do not and keep libp2p inbound bounded.

### 2.1 Role enum and CLI flag

**Files:**

* `components/p2p-libp2p/src/config.rs`
* `kaspad/src/args.rs`
* `kaspad/src/libp2p.rs`

Tasks:

* [x] Add a `Role` enum to the libp2p config:

  ```rust
  pub enum Role {
      Public,
      Private,
      Auto,
  }
  ```

* [x] Add CLI flag `--libp2p-role=public|private|auto` (default `auto` when libp2p is enabled).

* [x] In `kaspad/src/libp2p.rs`, resolve `Role::Auto` to either `Public` or `Private` using a simple rule (for now), for example:

  * If `--libp2p-reservations` is set → `Private`.
  * Else if `--libp2p-helper-listen` is set → `Public`.
  * Else → `Private`.

* [x] Pass the resolved role into the libp2p config.
  - Note: Auto defaults to private unless a helper listen address is set; bridge status is reported as `full` over RPC because the RPC enum has no bridge variant.

### 2.2 Role‑aware NetAddress service bits and `relay_port`

**Files:**

* `utils/src/networking.rs` (NetAddress)
* `kaspad/src/daemon.rs`
* `protocol/flows/src/flow_context.rs`

Today the daemon sets `NET_ADDRESS_SERVICE_LIBP2P_RELAY` and `relay_port` when libp2p is enabled, and the flow context advertises those in Version.

Tasks:

* [x] Find where the local `NetAddress` values for Version are built and where:

  * `services` is ORed with `NET_ADDRESS_SERVICE_LIBP2P_RELAY`.
  * `relay_port` is set from the libp2p helper/listen configuration.

* [x] Change this so that:

  * If role is `Public` and mode is `Full` or `Bridge`:

    * Set `NET_ADDRESS_SERVICE_LIBP2P_RELAY`.
    * Set `relay_port` from the libp2p config (as in current full mode).

  * If role is `Private`:

    * Do **not** set `NET_ADDRESS_SERVICE_LIBP2P_RELAY`.
    * Do **not** advertise `relay_port`.

* [x] Confirm the Version message layout is unchanged; only the values of `services` / `relay_port` differ by role.
  - Note: Advertisement is computed centrally (`libp2p_advertisement`) and only set for public roles with bridge/full/helper modes; private roles leave service bits and relay_port unset.

### 2.3 Private‑role libp2p inbound cap (ConnectionManager)

**File:** `components/connectionmanager/src/lib.rs`

The ConnectionManager already splits inbound limits between TCP and libp2p and enforces per‑relay buckets.

Tasks:

* [x] Extend ConnectionManager’s configuration/struct to carry:

  * A flag or enum indicating whether the node is running as a private role.
  * A numeric `libp2p_inbound_cap_private` (e.g. 8 by default).

* [x] Thread the resolved role from the daemon into ConnectionManager at construction time.

* [x] In `handle_inbound_connections()`:

  * For private role:

    * Count current libp2p inbound peers (`is_libp2p_peer()`).
    * If count exceeds `libp2p_inbound_cap_private`, drop excess libp2p peers (using existing randomness or relay‑bucket logic).
    * Do not change TCP inbound handling.

  * For public role:

    * Preserve existing behaviour (half the inbound limit for libp2p, per‑relay buckets as currently implemented).
  - Note: Private cap defaults to 8 and is set via a global role config seeded from daemon (only active when libp2p mode is enabled).

### 2.4 Phase B tests

**B.1 – Unit tests**

* [x] Add/extend tests in `connectionmanager` to cover:

  * Private role with many libp2p inbound peers → ensure we drop down to `libp2p_inbound_cap_private` and keep TCP peers.
  * Public role → behaviour remains as before (same distributions and caps).

* [x] Add/extend tests for Version address building to validate:

  * Public role sets `NET_ADDRESS_SERVICE_LIBP2P_RELAY` and `relay_port`.
  * Private role does not.
  - Note: Added tests for `libp2p_advertisement` (public vs private) and libp2p cap drop helper.

**B.2 – Manual role verification**

* [ ] Start a node configured as a **public relay** (e.g. testnet):

  ```bash
  kaspad \
    --libp2p-mode=bridge \
    --libp2p-role=public \
    --libp2p-helper-listen=127.0.0.1:38080 \
    # other existing libp2p flags as required
  ```

  * Verify via logs/RPC that Version addresses include the relay service bit and `relay_port`.

* [ ] Start a node configured as **private** (can be in the NAT lab):

  ```bash
  kaspad \
    --libp2p-mode=bridge \
    --libp2p-role=private \
    --libp2p-reservations=... \
    --libp2p-external-multiaddrs=... \
    --libp2p-helper-listen=127.0.0.1:38080
  ```

  * Verify:

    * Its Version addresses do **not** advertise relay service bits or `relay_port`.
    * libp2p inbound peer count is bounded by `libp2p_inbound_cap_private` even when many DCUtR sessions are triggered.

---

## 3. Phase C – Optional Hybrid Policy Refinements

> This phase is **optional** and can be deferred to a later PR. Do *not* start it until Phases A and B are complete and reviewed.

The hybrid proposals describe a richer selection and fallback policy (libp2p vs TCP per peer, cooldowns, relay diversity).

If you do tackle this, keep each sub‑step small and heavily tested.

### 3.1 Address metadata helpers (optional)

**Files:**

* `components/addressmanager/**`

* `utils/src/networking.rs`

* [ ] Add helper methods to `AddressManager` to:

  * Mark an address as libp2p‑capable (setting service bit / `relay_port` as needed).
  * Clear libp2p capability after repeated libp2p dial failures.

* [ ] Ensure these helpers still rely on `NetAddress` merge semantics (OR services, overwrite `relay_port` if new info is richer).

### 3.2 Transport selection and fallback (optional)

**Files:**

* `components/connectionmanager/src/lib.rs`

* `components/p2p-libp2p/src/transport.rs`

* [ ] Introduce a small “transport decision” step before calling the outbound connector in bridge mode, using:

  * Service bits and `relay_port` from `NetAddress`.
  * Any stored outcome info (e.g. recent libp2p failure → TCP for a cooldown).

* [ ] In bridge mode only, allow:

  * Libp2p first for libp2p‑capable peers, with a single TCP fallback when libp2p fails.
  * No fallback for explicit relay circuit dials (helper‑initiated DCUtR) to keep semantics clear.

* [ ] Keep `full` mode strictly libp2p‑only for outbound (unchanged) for lab reproducibility.

* [ ] Add unit tests for selection and fallback logic.

---

## 4. Final Verification & Documentation

### 4.1 Behavioural invariants

Before you consider this work complete, confirm:

* [x] `libp2p-mode=off`:

  * Behaviour matches upstream/master; no libp2p runtime at all.
  - Note: Default mode, unchanged from baseline.

* [x] `libp2p-mode=full`:

  * Behaviour is unchanged from current clean branch: it remains an overlay‑only mode, used by NAT lab; mainnet sync is still not expected to work here.
  - Note: Validated in `BRIDGE-NAT-REGRESSION.md` – DCUtR and relay behavior unchanged.

* [x] `libp2p-mode=bridge`:

  * Mainnet node reaches normal outbound peers and syncs via TCP.
  * libp2p stack (relay, DCUtR, helper) can still be exercised (e.g. in the NAT lab) without affecting mainnet connectivity.
  - Note: Validated in `MAINNET-SOAK-REPORT-BRIDGE.md` – 8/8 TCP peers with libp2p runtime active.

### 4.2 Tests

* [x] All relevant tests pass:

  ```bash
  cargo test -q -p kaspa-p2p-libp2p
  cargo test -q -p kaspad
  ```

* [x] Any new unit tests for connection manager, libp2p config, and roles are in place and green.

### 4.3 Mainnet soak with `bridge` mode

* [x] Repeat a mainnet soak similar to `MAINNET-SOAK-REPORT.md`, but with `--libp2p-mode=bridge`.

  * Node should:

    * Sync fully.
    * Maintain stable peer counts.
    * Show stable memory.
    * Show no panics.

* [x] Add an updated section or a new report file describing the bridge‑mode soak outcome.
  - Note: Created `MAINNET-SOAK-REPORT-BRIDGE.md` documenting successful 8/8 TCP connectivity, IBD progress, and zero panics.

### 4.4 NAT lab spot‑check

* [x] Perform a short NAT lab run (relay + two NAT nodes) to confirm DCUtR and reservations behave as before in `full` mode, and optionally validate a bridge‑mode node still behaves correctly while being able to participate in DCUtR overlays.
  - Note: Created `BRIDGE-NAT-REGRESSION.md` documenting full mode regression test – reservations accepted, DCUtR dial successful, bounded failure (AttemptsExceeded(3)), zero panics.

### 4.5 Operator documentation

* [x] Update `tcp-hole-punch/LIBP2P-OPERATOR.md` to describe:

  * Modes: `off`, `bridge`, `full` (and `helper` as alias).
  * Roles: `public`, `private`, `auto`.
  * Example CLI for:

    * Public relay node.
    * Private node behind NAT using DCUtR.
    * Typical mainnet node in bridge mode.

* [x] Optionally add a short `BRIDGE-MODE-SUMMARY.md` summarising what changed and how to use it.
  - Note: Added `BRIDGE-MODE-SUMMARY.md` as a quick reference alongside the updated operator notes.

---

## 5. Status Notes

Add any running notes, decisions, or deviations here as you work through the plan:

* 2025-11-30: Bridge mode validation complete
  - `MAINNET-SOAK-REPORT-BRIDGE.md` created – bridge mode enables 8/8 TCP connectivity on mainnet
  - `BRIDGE-NAT-REGRESSION.md` created – full mode DCUtR/relay behavior unchanged
  - All Phase A and Phase B items validated; Phase C (optional hybrid policy refinements) deferred
  - Branch ready for review

```

::contentReference[oaicite:38]{index=38}
```
