# DCUtR gap analysis (tcp-hole-punch vs tcp-hole-punch-clean)

Context: DCUtR hole punch succeeded on the legacy `tcp-hole-punch` branch in the Proxmox full-cone NAT lab, but on `tcp-hole-punch-clean` we only see relay reservations; no DCUtR logs and no Kaspa peers.

## Side-by-side behaviour

| Area | tcp-hole-punch (legacy) | tcp-hole-punch-clean (current) | Likely effect |
| --- | --- | --- | --- |
| Swarm behaviours | Identify + relay + ping + DCUtR + AutoNAT + StaticAddrBehaviour that seeds/pushes observed listen addrs into external candidates. | Identify + relay + ping + DCUtR + stream bridge only; no AutoNAT/static seeding beyond CLI-provided external/advertise addrs. | Fewer direct-address candidates for DCUtR when operators don’t manually configure external addrs. |
| DCUtR trigger on relayed inbound | On identify or relay inbound circuit, if peer supports `/libp2p/dcutr` and we only have a relayed **inbound** connection, force an outbound dial back via the relay (`/p2p/<relay>/p2p-circuit/p2p/<peer>`, `PeerCondition::Always`). | Logic exists but relies on accurate state tracking. | Remote never sees us as a dialer advertising DCUtR; no upgrade attempt, no punch logs. |
| Relay context tracking | Captures relay peer id from circuit listen addr, keeps it as active relay for dial-back construction. | Similar logic exists in `SwarmDriver`. | |
| External Address Candidates | `Identify::Received` explicitly added `observed_addr` (our public IP seen by relay) to `StaticAddrBehaviour`, feeding DCUtR candidates. | **MISSING.** `Identify::Received` only updated dcutr support flags; did not add `observed_addr` to swarm. | DCUtR has NO candidates to send in the Connect message if CLI args didn't perfectly match the public IP. Hole punch fails silently or aborts. |

## Hypothesis

1.  **Primary Gap:** On the clean branch, we were not adding the `observed_addr` (reported by the relay via Identify) to our swarm's external addresses. DCUtR relies on these addresses to send to the peer for the hole punch. Without them (and if CLI external addrs were missing or incorrect), DCUtR aborts.
2.  **Secondary:** Logging was insufficient to see why dial-back was skipped.

## Fix applied

1.  **Add Observed Addresses:** In `components/p2p-libp2p/src/transport.rs`, updated `Identify::Received` handler to add `info.observed_addr` to the swarm (filtering out relay addresses). This restores the behavior of the old `StaticAddrBehaviour`.
2.  **Enhanced Logging:** Added detailed debug logs in `maybe_request_dialback` and `track_established` to trace:
    -   Why a dial-back is skipped (no state, no dcutr support, not relayed, already dialer).
    -   Whether the connection is tracked as Relayed or Direct.
3.  **Helper Logic:** Added `addr_uses_relay` helper to support the filter.

This ensures that even if the operator's config for `external_multiaddrs` is slightly off (or missing), the relay's observation of our public IP is used for the hole punch.
