# Libp2p Swarm Provider Notes (placeholder)

This repo currently uses a placeholder `Libp2pStreamProvider` that returns `NotImplemented`. The real provider should:

1) Build a libp2p swarm (TCP + Noise + Yamux) using the loaded `Libp2pIdentity`.
2) Implement `dial(NetAddress)`:
   - Convert the address to a multiaddr.
   - Dial, open a dedicated substream for gRPC bytes.
   - Return `(TransportMetadata { libp2p=true, libp2p_peer_id=peer }, BoxedLibp2pStream)`.
3) Implement `listen()`:
   - Accept incoming connections/substreams.
   - Expose a closer guard and metadata (peer id, path/relay when known).
4) Drive the swarm on a Tokio task; expose shutdown.
5) Preserve default TCP behaviour when the `libp2p` feature/mode is off.

Notes for current libp2p 0.52 API (matches our Cargo.toml):
- `Swarm::new` expects a `libp2p::swarm::Config` (use `Config::with_tokio_executor()`).
- The builder flow is `let transport = libp2p::tcp::tokio::Transport::new(...).upgrade(...).authenticate(noise).multiplex(yamux).boxed();`
  then `let behaviour = BaseBehaviour::default();`
  then `let cfg = libp2p::swarm::Config::with_tokio_executor();`
  finally `let swarm = Swarm::new(transport, behaviour, peer_id, cfg);`

Event loop guidance:
- Avoid holding a std::sync::MutexGuard across `.await`; wrap swarm in `tokio::sync::Mutex` if you need to drive it.
- The provider should own the swarm and run a background task to poll `swarm.select_next_some().await`.
- Provide a shutdown trigger so we don’t leak the task on daemon exit.

These notes are for the eventual implementation; current runtime remains unchanged.
