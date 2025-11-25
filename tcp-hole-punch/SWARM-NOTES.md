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

These notes are for the eventual implementation; current runtime remains unchanged.
