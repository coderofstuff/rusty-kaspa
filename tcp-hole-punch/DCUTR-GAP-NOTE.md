# DCUtR gap analysis (tcp-hole-punch vs tcp-hole-punch-clean)

Context: DCUtR hole punch succeeded on the legacy `tcp-hole-punch` branch in the Proxmox full-cone NAT lab, but on `tcp-hole-punch-clean` we only see relay reservations; no DCUtR logs and no Kaspa peers.

## Side-by-side behaviour

| Area | tcp-hole-punch (legacy) | tcp-hole-punch-clean (current) | Likely effect |
| --- | --- | --- | --- |
| Swarm behaviours | Identify + relay + ping + DCUtR + AutoNAT + StaticAddrBehaviour that seeds/pushes observed listen addrs into external candidates. | Identify + relay + ping + DCUtR + stream bridge only; no AutoNAT/static seeding beyond CLI-provided external/advertise addrs. | Fewer direct-address candidates for DCUtR when operators don’t manually configure external addrs. |
| DCUtR trigger on relayed inbound | On identify or relay inbound circuit, if peer supports `/libp2p/dcutr` and we only have a relayed **inbound** connection, force an outbound dial back via the relay (`/p2p/<relay>/p2p-circuit/p2p/<peer>`, `PeerCondition::Always`). | No dial-back; we remain purely listener on the relayed circuit. | Remote never sees us as a dialer advertising DCUtR; no upgrade attempt, no punch logs. |
| Relay context tracking | Captures relay peer id from circuit listen addr, keeps it as active relay for dial-back construction. | Reservation path keeps no relay context; circuit listen addr is not reused for dial-back. | Even if we wanted to dial back, we don’t have the relay base addr/peer id handy. |
| Bridging | `libp2p_stream` behaviour opens stream after dial and hands to adaptor. | StreamBehaviour opens stream on connection establishment; bridging works (verified A→C). | Bridging is fine; the missing piece is creating the outbound dial that would trigger DCUtR. |

## Hypothesis

On the clean branch we never act as a dialer on a relayed circuit after receiving an inbound libp2p connection. DCUtR only advertises/negotiates when both sides have an outbound leg; staying listener-only means the upgrade is never attempted, so no punch logs and no Kaspa peers appear.

## Minimal fix applied

- Track relay circuit listen addrs and per-peer DCUtR support/relay-connected state in the libp2p swarm driver.
- When an inbound relayed connection from a DCUtR-capable peer is detected (identify + relay markers, no existing outbound), initiate a forced dial-back via the active relay using `PeerCondition::Always` so DCUtR can negotiate and punch.

See `components/p2p-libp2p/src/transport.rs` for the wiring.
