# DCUtR gap analysis (tcp-hole-punch vs tcp-hole-punch-clean)

Context: DCUtR hole punch succeeded on the legacy `tcp-hole-punch` branch in the Proxmox full-cone NAT lab, but on `tcp-hole-punch-clean` we initially saw only relay reservations; no DCUtR logs and no Kaspa peers. After fixing observed address seeding, we saw DCUtR logs but with "peer does not support dcutr".

## Side-by-side behaviour

| Area | tcp-hole-punch (legacy) | tcp-hole-punch-clean (current) | Likely effect |
| --- | --- | --- | --- |
| Swarm behaviours | Identify + relay + ping + DCUtR + AutoNAT + StaticAddrBehaviour that seeds/pushes observed listen addrs into external candidates. | Identify + relay + ping + DCUtR + stream bridge only; no AutoNAT/static seeding beyond CLI-provided external/advertise addrs. | Fewer direct-address candidates for DCUtR when operators don’t manually configure external addrs. |
| DCUtR trigger on relayed inbound | On identify or relay inbound circuit, if peer supports `/libp2p/dcutr` and we only have a relayed **inbound** connection, force an outbound dial back via the relay (`/p2p/<relay>/p2p-circuit/p2p/<peer>`, `PeerCondition::Always`). | Logic exists but relies on accurate state tracking. | Remote never sees us as a dialer advertising DCUtR; no upgrade attempt, no punch logs. |
| Relay context tracking | Captures relay peer id from circuit listen addr, keeps it as active relay for dial-back construction. | Similar logic exists in `SwarmDriver`. | |
| External Address Candidates | `Identify::Received` explicitly added `observed_addr` (our public IP seen by relay) to `StaticAddrBehaviour`, feeding DCUtR candidates. | **MISSING.** `Identify::Received` only updated dcutr support flags; did not add `observed_addr` to swarm. | DCUtR has NO candidates to send in the Connect message if CLI args didn't perfectly match the public IP. Hole punch fails silently or aborts. |
| DCUtR Protocol Advertisement | `libp2p` 0.52 `dcutr` behaviour advertised protocol automatically? (Unclear). | **MISSING.** In `libp2p` 0.56 (and 0.52), `dcutr` behaviour seems to NOT register `/libp2p/dcutr` with `Identify` unless actively negotiating or configured specifically. | Peer sees us, but `Identify` says we don't support DCUtR. Peer logs "skipping dial-back ... peer does not support dcutr". |
| Bridge Stream Initiation | Initiates stream on connection establishment regardless of role. | **Race Condition.** Listener (Relay C) initiates stream to Dialer (A) AND Dialer (A) initiates stream to Listener (C). | Double connection attempts on C. C logs "connect_with_stream failed" for one of them. |

## Hypothesis

1.  **Primary Gap 1:** On the clean branch, we were not adding the `observed_addr` (reported by the relay via Identify) to our swarm's external addresses. DCUtR relies on these addresses.
2.  **Primary Gap 2:** `dcutr` behaviour in `rust-libp2p` (0.52/0.56) does not automatically advertise the `/libp2p/dcutr` protocol via `Identify` when just instantiated. This prevents the remote peer from knowing we support DCUtR, so it skips the dial-back.
3.  **Secondary:** Double stream initiation causes error logs on the relay node C.

## Fix applied (2025-11-27)

1.  **Add Observed Addresses:** In `components/p2p-libp2p/src/transport.rs`, updated `Identify::Received` handler to add `info.observed_addr` to the swarm (filtering out relay addresses).
2.  **Upgrade libp2p:** Upgraded `libp2p` to `0.56.0` (latest stable) to benefit from upstream fixes and modern APIs.
3.  **Force DCUtR Advertisement:** Implemented `DcutrBootstrapBehaviour` which is a dummy `NetworkBehaviour` that manually registers a handler supporting `/libp2p/dcutr`. This forces `Identify` to advertise the protocol support.
4.  **Fix Stream Race:** Updated `SwarmDriver::handle_event` for `ConnectionEstablished` to ONLY request a bridge stream if we are the `Dialer`. This prevents the Listener from opening a redundant stream, eliminating the "connect_with_stream failed" error on Node C.
5.  **Enhanced Logging:** Added detailed debug logs in `maybe_request_dialback` and `track_established`.

This ensures that even if the operator's config for `external_multiaddrs` is slightly off (or missing), the relay's observation of our public IP is used for the hole punch, AND the peer correctly identifies our DCUtR support.
