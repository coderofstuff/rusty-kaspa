Goals & Non-Goals
- Enable “bridge” mode where nodes keep normal TCP connectivity while optionally leveraging libp2p/DCUtR for NAT traversal and relay assistance. Mainnet nodes must not lose the ability to sync with TCP-only peers.
- Preserve default behaviour for operators who do not enable libp2p; no consensus/IBD/mempool/mining changes (guardrails from `tcp-hole-punch/PLAN-CLEANUP.md`).
- Do not attempt to force all connectivity through libp2p or change protocol versions; avoid introducing new consensus messages or altering flow semantics.

Config & Modes
- Modes: `off` (status quo), `bridge` (hybrid TCP+libp2p), `full` (libp2p-only overlay for lab). Keep `helper` as alias of `full` for backwards compatibility.
- Optional role flag: `--libp2p-role=public|private|auto` (default auto). Public nodes advertise relay capability and run helper; private nodes do not claim relay service bits and keep libp2p inbound budgets low. `auto` derives public if `--libp2p-reservations` or `--libp2p-helper-listen` is set, else private.
- Keep existing flags; add a small helper flag to cap libp2p outbound attempts per relay (e.g., `--libp2p-relay-outbound-cap`) if needed for bridge mode backpressure.

Dial Policy
- Address classification: use `services` bit `NET_ADDRESS_SERVICE_LIBP2P_RELAY` and `relay_port` from NetAddress plus any stored libp2p peer_id/multiaddr metadata to tag peers as libp2p-capable. Plain TCP addresses (no service bit) are treated as TCP-only.
- Bridge mode selection:
  1) Plain TCP peers (no libp2p capability): dial TCP only.
  2) Peers advertising libp2p relay/service or known via reservation/helper/DCUtR: try libp2p first; on DialFailed/AttemptsExceeded, fall back to TCP once per backoff window so mainnet sync continues.
  3) Explicit circuit/relay targets from helper: libp2p only (no TCP fallback, because intent is hole-punch).
- Retry/backoff: cache the last transport decision per NetAddress; after a libp2p failure, prefer TCP for a cooldown (e.g., 10–15 minutes) to avoid oscillation and protect relays.

Relay Usage & Limits
- Keep per-relay inbound buckets (`ConnectionManager::relay_overflow`) and apply them regardless of mode. For bridge mode, extend outbound policy to avoid concentrating on a single relay: track outstanding libp2p dials per relay id and cap them (configurable).
- Randomize relay selection for reservations when multiple are configured; refresh reservations with backoff already present in `ReservationManager`.

Private Node Inbound Limits
- Maintain the split inbound budget but make it dynamic in bridge mode: start with a small libp2p slice (e.g., min(2, inbound_limit/4)) for private nodes, growing only when successful DCUtR sessions arrive. Public relay nodes keep the existing 50/50 split.
- Respect `--libp2p-role=private` by not advertising relay service bits and by tightening libp2p inbound caps; keep TCP inbound unaffected.

Implementation Plan (high-level, guardrails respected)
- CLI/config (`kaspad/src/args.rs`, `kaspad/src/libp2p.rs`, `kaspad/src/daemon.rs`):
  - Add bridge mode enum value; thread through AdapterMode.
  - Add `--libp2p-role` parsing and defaulting logic; expose role to FlowContext so service bits/relay_port are only advertised when role=public.
  - Preserve `full` for lab; keep helper alias.
- Address metadata (`utils/src/networking.rs`, `components/addressmanager/**`):
  - Consider an additional service bit for “libp2p-capable” distinct from “relay” if needed, or reuse relay bit plus `relay_port` to signal capability.
  - Store libp2p peer_id/external multiaddrs learned via Identify/DCUtR in AddressManager (new optional fields) so ConnectionManager can tell whether libp2p is viable.
  - Ensure `merge_metadata` also merges the new capability fields.
- Connection selection (`components/connectionmanager/src/lib.rs`):
  - Inject a transport decision step before calling `Adaptor::connect_peer`: choose TCP vs libp2p based on the classification rules above and recent outcomes/backoff.
  - Track per-relay outbound counters when using libp2p paths; reuse existing per-relay structures for inbound symmetry.
  - When in `full` mode, preserve current libp2p-only behaviour for lab reproducibility.
- Libp2p adapter surfaces (`components/p2p-libp2p/src/transport.rs`):
  - Teach `Libp2pOutboundConnector` to attempt TCP fallback when mode=bridge and the chosen transport is “prefer libp2p with fallback”; keep strict libp2p in full mode.
  - Record dial failures with enough context (relay id, AttemptsExceeded vs NoAddresses) for ConnectionManager backoff.
- FlowContext/handshake (`protocol/flows/src/flow_context.rs`):
  - Advertise libp2p relay service bits/relay_port only when role=public.
  - Optionally add a capability flag in the Version message indicating libp2p support without claiming relay to help peers classify transports.
- Preserve NAT lab workflows: `--libp2p-mode=full` continues to behave as today for overlay/DCUtR testing; bridge mode is additive and should not perturb lab scripts.

Testing Strategy
- Unit: transport-selection logic in ConnectionManager (TCP vs libp2p, cooldown after failure, per-relay outbound caps); AddressManager metadata merge for new fields; Libp2pOutboundConnector fallback behaviour in bridge mode.
- Integration (local): simulate mixed peers (some with service bit/relay_port, some plain TCP) to verify hybrid selection and fallback. Ensure inbound limits split correctly for private/public role.
- NAT lab: rerun DCUtR stress scenarios in full mode to confirm no regression; add a bridge-mode run where A/B can still sync over TCP if DCUtR fails.
- Mainnet-like soak: start a bridge-mode node against mainnet seeds and verify it reaches 8/8 outbound via TCP while still accepting libp2p/DCUtR sessions from lab-style peers.
