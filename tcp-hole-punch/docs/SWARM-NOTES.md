# Libp2p Swarm Provider Notes

`Libp2pStreamProvider` is now backed by a concrete implementation: `SwarmStreamProvider`.

## Current behavior
- Builds a libp2p swarm (TCP + Noise + Yamux + behaviours) from `Libp2pIdentity`.
- Runs `SwarmDriver` on a background Tokio task.
- Implements:
  - `dial(NetAddress)` and `dial_multiaddr(Multiaddr)`
  - `listen()` (bridged via internal incoming channel)
  - `reserve(Multiaddr)` with release handle
  - `probe_relay(Multiaddr)`
  - `shutdown()`, `peers_snapshot()`, role/relay-hint watch updates, metrics snapshot
- Preserves TCP-only behavior when mode is `off`.

## Runtime notes
- `listen()` requests `EnsureListening` and then waits for bridged streams from the driver.
- Inbound bridge retry policy is bounded: transient provider-unavailable errors are retried; terminal listen errors abort the bridge loop and are surfaced in logs.
- Shutdown triggers the swarm task and awaits join to avoid leaking background work.

## libp2p API notes
- `Swarm::new` uses `libp2p::swarm::Config::with_tokio_executor()`.
- Relay/DCUtR path is wired through identify + relay client/server + DCUtR behaviour and stream protocol events.
