# Libp2p/DCUtR Clean Integration – PR Brief

## What the feature does
- Adds an optional libp2p transport alongside the existing TCP stack (off by default). When enabled, the node can listen/dial via libp2p, reserve relay slots, and perform DCUtR hole punching to reach private peers.
- Libp2p hands fully-formed streams into the existing P2P connection handler; consensus, IBD, mining, and mempool remain unchanged.
- Operators opt in via `--features libp2p` at build time and `--libp2p-mode` at runtime; helper control remains stubbed/off unless explicitly bound.

## Intentional change surface vs `upstream/master`
- `components/p2p-libp2p/**`: new crate owning swarm setup (identify/relay/DCUtR/AutoNAT), stream provider, helper API, reservations/backoff, transport wiring, and tests/harnesses.
- `kaspad/src/libp2p.rs`, `kaspad/src/daemon.rs`, `kaspad/src/args.rs`: libp2p config parsing (CLI/env), runtime bootstrap, and injection into the daemon while keeping feature gating/off-by-default behaviour.
- Address/connection metadata for relays: `utils/src/networking.rs` (NetAddress `services` + `relay_port`, metadata merge), `components/addressmanager/src/lib.rs` (persist/merge metadata), `components/connectionmanager/**` (inbound caps/buckets keyed by relay).
- Wire/RPC conversions: `protocol/p2p/proto/p2p.proto` (optional `services`/`relayPort`), `protocol/p2p/src/convert/net_address.rs`, `protocol/flows/src/flow_context.rs` (propagate relay port). Older peers ignore the new fields safely.
- Docs/harness: `tcp-hole-punch/**` (operator notes, plan, sanity/lab reports, DCUtR harness) and libp2p-specific tests.
- Everything else (consensus/IBD/hashing/pruning/etc.) remains byte-for-byte compatible with upstream; CompressedParents/RLE, hashing, and header stores are untouched.

## Compatibility guarantees
- Default build excludes libp2p entirely; `cargo check --no-default-features` mirrors upstream.
- TCP-only nodes (libp2p off) behave identically to upstream: same ports, handshake, IBD/consensus semantics, and RPC behaviour.
- Wire compatibility: new NetAddress metadata defaults to zero/None; hashing/equality still IP:port; proto additions are optional fields so legacy peers ignore them. No protocol version bump.
- Consensus and IBD codepaths are unchanged; libp2p/DCUtR does not depend on consensus internals.
- AutoNAT posture: client+server enabled in libp2p modes, server responds only to public IPs by default; labs can opt into private-IP support via `--libp2p-autonat-allow-private` / `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true`.

## Testing performed
- Build/check: `cargo build`; `cargo check --no-default-features`; `cargo build --features libp2p --bin kaspad`.
- Tests: `cargo test -p kaspa-p2p-libp2p --lib --tests` (DCUtR harness remains `#[ignore]` upstream); `cargo test -p kaspad --lib --features libp2p`.
- Runtime smoke (local): default binary; libp2p feature with `--libp2p-mode=off`; libp2p full mode on dedicated port (peer id + AutoNAT logged, clean shutdowns).

## Follow-ups / notes
- Proxmox NAT lab harness still needs to be re-run on lab hosts after this consensus alignment; not executed in this environment.
- DCUtR harness stays ignored until upstream `libp2p-relay` fixes the panic on dropped transport channels.
