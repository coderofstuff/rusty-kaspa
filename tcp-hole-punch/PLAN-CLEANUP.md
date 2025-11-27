# TCP Hole Punch Clean-Slate Integration Plan

This document tracks the cleanup/reintegration work to make libp2p/DCUtR optional, safe, and minimal. It is written so a new engineer can pick it up and continue.

## Principles

- `upstream/master` is the **golden reference**. If a change is not clearly required for libp2p/DCUtR, we keep master’s behaviour and code as-is.
- libp2p is optional (feature-gated) and off by default.
- Default node behaviour (non-libp2p) remains unchanged.
- Identity is ephemeral by default; persistent IDs are explicit opt-in.
- Single transport injection seam; avoid touching consensus/flows unless required.
- Libp2p path must not change consensus-level behaviour.
- Any consensus/IBD change must be explicit, minimal, and separately reviewed – default stance is **no changes** here.
- No drive-by refactors: we do not “clean up” or tweak unrelated code because we are in the file.
- Add tests alongside each change.

A change is considered **necessary** for this project only if:

1. Without it, the libp2p / TCP hole-punch feature **does not compile or run**; or
2. It is required to **expose** the feature (CLI/env flags, RPC surface, CI wiring); or
3. It is a **test or documentation** that does not alter runtime behaviour.

Everything else should match `upstream/master`.

## Scope & Guardrails

For this project, the allowed change surface is deliberately small.

**Allowed areas for code changes (runtime):**

- New libp2p adapter + wiring:
  - `components/p2p-libp2p/**` (new crate)
  - `kaspad/src/libp2p.rs` (or equivalent wiring module)
  - `kaspad/src/daemon.rs` (libp2p mode/initialisation)
  - `kaspad/src/args.rs` (CLI/env libp2p flags)
- Metadata and limits needed for libp2p:
  - `components/connectionmanager/**` (only for libp2p-related limits/metadata)
  - `components/addressmanager/**` (only for relay/libp2p metadata)
  - `utils/src/networking.rs` (NetAddress services/relay_port only)
- Libp2p-specific RPC and helper surfaces:
  - `rpc/service/src/service.rs` and related RPC/proto definitions for libp2p status, if needed.
- Libp2p helper/bridge/relay crates under `tcp-hole-punch/**` (or equivalent).

**Everything below is out of bounds for this work and must stay identical to `upstream/master` (runtime code):**

- `consensus/core/**`
- `consensus/src/**`
- `protocol/flows/src/ibd/**`
- `mining/**`
- `mempool/**`
- `consensus/core/src/hashing/**`
- `consensus/core/src/merkle.rs`
- `utils/src/hex.rs`
- Any pruning stores/pipelines/errors

If you believe a change in one of these directories is absolutely required, you must:

1. Write down why libp2p cannot work without this exact change.
2. Explain why reverting to `upstream/master` is not an option.
3. Get that justification explicitly reviewed before writing code.

**Tests and docs** may be added anywhere they logically belong, as long as they do not alter runtime code.

## Consensus rollback

- Consensus/header code (including CompressedParents/RLE, hashing, stores, RPC/convertors) is reverted to exactly match `upstream/master`.
- Libp2p/DCUtR does not depend on consensus or IBD internals; any future divergence here must be explicitly justified and reviewed.

## Status audit (fresh eyes)

- Done: feature gating/default-off stance; identity loader (ephemeral + persisted); transport seam in `ConnectionHandler` (`connect_with_stream`/`serve_with_incoming`); NetAddress relay_port merge; inbound caps/buckets in connection manager; libp2p stream provider now real (relay/identify/DCUtR, dial/listen/reserve); daemon/libp2p runtime wired with outbound connector handing streams into the P2P hub; CLI surface exposes reservations/external/advertise/relay caps; dedicated libp2p listen port (default `p2p_port+1`, no port sharing with TCP); libp2p→Kaspa bridge live (streams handed into ConnectionHandler with synthetic metadata).
- Incomplete/by design: helper control plane still a placeholder (no listener bound yet); keep documented as TBD. DCUtR harness exists but is `#[ignore]` due to upstream libp2p relay-client panic when the transport channel drops; manual runs currently trip that upstream issue.
- Consensus/header drift was reverted to exactly match `upstream/master` (CompressedParents/RLE, hashing, stores, RPC/convertors). Libp2p/DCUtR does not depend on consensus internals.
- [x] DCUtR gap: legacy branch succeeded in the NAT lab because it forced a dial-back over the relay when only an inbound relayed connection existed. Clean branch never dialed back, so DCUtR never negotiated. Dial-back via active relay (peer-aware) added to the swarm driver; see `tcp-hole-punch/DCUTR-GAP-NOTE.md`.
   - [x] **Fix Applied (2025-11-27):**
     - Restored logic to feed `Identify` observed addresses into swarm external addresses.
     - Upgraded `libp2p` to `0.56.0` (latest stable) to benefit from upstream fixes.
     - Implemented `DcutrHackBehaviour` to manually advertise `/libp2p/dcutr` protocol, resolving a regression where `dcutr` behaviour was not registering its protocol with `Identify`.
     - Fixed double-stream initiation race condition on Node C by ensuring only the connection dialer initiates the bridge stream.
     - **Restored Helper API Dialing:** Re-implemented the helper API to support manual dialing of circuit addresses, which is essential for triggering the hole punch in the absence of automatic discovery (MDNS/Kademlia).
     - **AutoNAT / Private IP Flag (2025-11-27):** Added `--libp2p-autonat-allow-private` flag to enable AutoNAT server support for private IPs in lab environments, defaulting to safe global-only behaviour for production.
     - **DCUtR Lab Verification:** Successfully verified DCUtR hole punching in the Proxmox NAT lab with the AutoNAT fix and Identify push updates disabled.
- [x] Out-of-scope diffs: none (checked against `upstream/master` for guarded directories).
- Stub sweep: transport hot paths no longer emit `NotImplemented`; remaining TODOs are legacy (unrelated to libp2p) or helper-control stubs. Dcutr success/failure now logged at info.

## Execution Order (checklist)

0. **Branching & Safety Net**

   - [x] Fetch latest `upstream/master` and create a new clean-slate branch from it.
   - [x] Tag or otherwise mark the current `tcp-hole-punch` state as “messy-but-working” for reference.
   - [x] Keep the new work isolated from the old branch; default build remains non-libp2p.
   - [x] Default stance: diff against `upstream/master`, restore original behaviour where we diverged unnecessarily, then layer only the minimal required changes.
   - [x] Do **not** copy consensus/pruning/mining/mempool/IBD/merkle/hash changes from the old branch into the new one. Use the old branch for inspiration only.

1. **Safety / IBD Guards (verify-only)**

   Goal: ensure we have **not** accidentally changed IBD/RPC behaviour relative to `upstream/master`.

   - [x] Diff IBD and RPC gating code against `upstream/master` and confirm there are **no runtime changes** in this branch. If any appear, revert to master.
   - [x] Confirm pruning/catch-up logic matches `upstream/master` (no new logic, no removals).
   - [x] Do **not** add new IBD abstractions (no new state machine, no new enums) as part of this project.
   - [ ] Optionally add tests that codify `upstream/master` IBD/RPC semantics (e.g., which RPCs are allowed during IBD), without changing production logic.  
     _Decision: skipped for now (no runtime changes in this area; low risk)._

2. **Gating & Identity**

   - [x] Add `libp2p` feature flag (off by default); guard all libp2p/bridge/helper code with `#[cfg(feature = "libp2p")]`.
   - [x] Remove libp2p crates from default workspace members so the default build has no libp2p dependency.
t as alias to `full` until a real need. Helper control port requires explicit flag (no default bind).
   - [x] Default identity ephemeral (in-memory). Provide a single canonical flag (e.g., `--libp2p-identity-path`; alias optional) as the only way to persist.
   - [x] Expose current PeerId in `getLibpStatus` plus whether it is ephemeral or persisted (and path if persisted); document privacy trade-offs.

3. **Adapter / Injection Boundary**

   - [x] Create `components/p2p-libp2p` (feature-gated) owning: libp2p spawn/dial/listen/reserve, helper JSON API, H2 tuning, multiaddr parsing, synthetic addressing, reservation refresh/backoff.
   - [x] Add libp2p service skeleton to host dial/listen/reservation start-up (currently stubbed/unimplemented). Libp2p swarm/provider is initialised inside the daemon `AsyncRuntime` (see `Libp2pInitService`) to avoid pre-runtime panics.
   - [x] Structure adapter crate into submodules (e.g., `helper_api`, `reservations`, `transport`, `metadata`) to avoid a god module.
   - [x] Make `ConnectionHandler` transport-agnostic: accept `AsyncRead/AsyncWrite` + metadata. Metadata separates identity (PeerId/IP), path (relay/origin), capabilities—document this boundary.
   - [x] Synthetic address stable per PeerId (relay-agnostic); store relay metadata separately. Synthetic address is identity accounting only (not a reachable address).
   - [x] Add stream-based P2P entrypoints with libp2p-tuned h2 settings (`connect_with_stream`/`serve_with_incoming`) to support adapter dial/listen.
   - [x] Wire libp2p outbound connector to a stream provider hook (placeholder for now) so adapter can hand libp2p streams into the transport seam.
   - [x] Allow inbound connections to provide precomputed transport metadata (for libp2p streams) via connect info, with synthetic addressing fallback.
   - [x] Stub libp2p service/provider listen bridge using tonic `Connected` metadata; full libp2p dial/listen to be implemented on this seam.
   - [x] Libp2p identity loader (ephemeral or persisted key) and runtime hook for outbound connector/status reporting; placeholder provider now carries peer_id/libp2p capability.
   - [x] Implement swarm-based `Libp2pStreamProvider` (dial/listen) and wire into libp2p service/runtime; populate real metadata.

4. **Protocol / Metadata**

   - [x] No protocol version bump. Signal libp2p/DCUtR via service/capability flag + `relay_port`; ensure v8 peers ignore safely.
   - [x] NetAddress: keep IP:port equality/hash; on re-add merge services (bitwise OR) and upgrade `relay_port` if provided. Migration (if any) has a version marker, rewrites idempotently, and logs once.
   - [x] Proper multiaddr parsing for relay IDs; malformed/unknown go to an `unknown` bucket (still limited). Guarantee: no connection bypasses accounting.

5. **Behaviour & Limits**

   - [x] Split inbound budgets: TCP inbound vs libp2p inbound; private-node heuristics only affect libp2p pool.
   - [x] Per-relay limits: bucket by parsed relay ID; unknown bucket exists; predictable drop policy (documented LRU or similar). Consider a global hard cap as a maximum safeguard.
   - [x] Reservation refresh releases old reservations and uses backoff on failure to avoid relay spam.
   - [x] Handshake timeout configurable; raise default for relay/DCUtR (e.g., 5–10s) with an overall deadline to avoid zombie connections.

6. **Config Surface**

   - [x] Central libp2p config builder: ports, helper addr, reservations, external addrs, advertised IPs, role. CLI/env parsing here; daemon consumes.
   - [x] Restore env-var support for key flags (ports, identity path, helper, mode, reservations, inbound caps).
   - [x] Decide `override-params`: restore or explicit deprecation with clear error message.
   - [x] Layered approach: CLI/env → plain struct → libp2p config; consistent env naming (e.g., `KASPAD_LIBP2P_*`); document precedence (CLI > env > file > default).

7. **Testing (add during each step)**

   - [x] Unit/property: NetAddress metadata merge semantics.
   - [x] Helper JSON API: valid/malformed requests; reservation refresh/backoff; error paths.
   - [x] Per-relay limits incl. malformed multiaddrs; `unknown` bucket enforced.
   - [x] Synthetic address stability across relays.
   - [x] Handshake timeout behaviour (slow path terminates within configured window).
   - [ ] IBD/RPC gating tests that **match `upstream/master` behaviour** (early IBD rejects, post-IBD accepts for template/submit/UTXO, etc.) – logic unchanged, just codified.  
     _Decision: skipped for now (no runtime changes in this area; low risk)._
   - [x] Mixed v8/v9 interop with relay metadata.
   - [x] DCUtR/helper harness behind a feature (kept for review/repro).
   - [x] Multiaddr robustness: malformed inputs do not panic and end up accounted in `unknown` bucket.

8. **Docs / UX**

   - [x] Feature matrix: modes (off/helper/full), what runs, ports, identity behaviour, inbound caps, how to disable entirely (compile-time + runtime).
   - [x] Privacy note on identity persistence vs ephemeral.
   - [x] Operational expectations for public vs private nodes; how to opt into stable IDs and helper control.
   - [x] Examples: plain TCP public node (libp2p off); private relay with ephemeral ID; public relay with persistent ID.
   - [x] Explicit “disable libp2p” instructions: build (`--no-default-features`/no libp2p members) and run (`mode=off`, no helper control).

9. **CI / Workspace Hygiene**

   - [x] Add CI job for `--no-default-features` to ensure non-libp2p build stays healthy.
   - [x] Ensure libp2p crates are opt-in via features/default-members.
- [x] Add CI job with libp2p/all-features enabled so tests run in that mode. Optional: check default build’s dependency tree excludes libp2p.

## Wire surface (libp2p-specific)

- NetAddress carries `services` (bitflags) and `relay_port` (optional u16) to mark peers that can act as libp2p relays and the port to dial. Tags are optional/backwards-compatible on the wire (proto fields default to zero/None) and ignored by unaware peers.
- Libp2p/DCUtR uses this metadata to advertise relay capability and budget inbound relay traffic; default TCP nodes behave identically, and consensus/IBD remain untouched.

## Notes for New Engineers

- `upstream/master` is the gold standard. If in doubt, do nothing and keep master’s behaviour.
- Start from step 0 and add tests as you go (don’t defer all tests to the end).
- Any change outside the libp2p adapter + injection seam must be justified explicitly and tied to a checklist item above.
- Keep default (libp2p-off) behaviour unchanged; verify with `--no-default-features` build and basic node startup.
- When in doubt about identity persistence, choose ephemeral unless the operator explicitly opted in via CLI.
- If a change affects consensus or IBD semantics, call it out in the PR description, link to the justification, and tag reviewers accordingly – but the default is **do not change these at all** for this project.
- No “while I’m here” clean-ups. If you want to refactor something unrelated, that is a separate proposal and a separate branch, not part of TCP hole punch.
