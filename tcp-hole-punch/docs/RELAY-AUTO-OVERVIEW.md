# Relay Auto Overview

Purpose
- Remove reliance on lab scripts; enable real-world relay discovery and reservation.
- Make private nodes reachable in real deployments.
- Keep inbound load bounded and reduce eclipse risk.

Why now (lab vs real world)
- Clean branch works because lab scripts hard-code relay + manual reservation.
- Real network needs auto discovery, selection, reservation, and role signaling.
- Relay pool prevents naive selection (spoof/overload risk) and keeps reservations resilient.

Scope (this branch)
- Relay capability metadata in NetAddress (services bit + relay port/capacity/ttl/role).
- Relay capability gossip in P2P proto + conversions.
- Address manager metadata merge for relay capability signals.
- Auto relay selection + reservation worker.
- Relay pool scoring, rotation, backoff, and multi-source filtering.
- Auto role promotion via AutoNAT (public vs private role).
- Inbound caps: per-relay, unknown-relay, and private role caps.
- Helper API metrics + relay/DCUtR metrics.
- Config + CLI knobs for all of the above.

Key concepts
- Relay capability: a public node advertises it can serve as a relay.
- Reservation: a private node reserves a circuit on a relay so inbound peers can dial it.
- Relay pool: ranked set of relay candidates used for auto selection.
- AutoNAT: detects public reachability and flips role/advertising accordingly.

How it works (high level)
1) Public nodes advertise relay capability (service bit + relay port + capacity + ttl + role).
2) Private nodes collect relay candidates (gossip + config list).
3) Relay pool scores, filters (multi-source), and selects relays.
4) Auto worker reserves circuits and rotates on failures/age.
5) Inbound peers dial reserved circuit; DCUtR can still upgrade to direct.

Relay capability metadata
- Service bit: NET_ADDRESS_SERVICE_LIBP2P_RELAY indicates relay support.
- Metadata fields: relay_port, relay_capacity, relay_ttl_ms, relay_role.
- Propagated in P2P NetAddress gossip and merged by address manager.

Auto relay selection + reservation
- Runs for Role::Private or Role::Auto when libp2p is enabled.
- Uses relay pool with scoring, backoff, and rotation windows.
- Probes relays to learn peer id when missing.
- Holds active reservation handles and releases on disconnect/rotation.

Relay pool behavior
- Candidate sources: address gossip + relay_candidates config list.
- Multi-source requirement (relay_min_sources) to reduce spoofing risk.
- Scoring considers success/failure, latency, uptime, source count.
- Per-relay peer cap enforced during selection.

Auto role promotion (AutoNAT)
- AutoNAT client/server enabled by config.
- Role flips based on reachability (public vs private).
- Public role advertises relay capability + external multiaddrs.
- Confidence threshold avoids flapping.

Inbound caps and safety
- Per-relay inbound cap (relay_inbound_cap).
- Unknown-relay inbound cap (relay_inbound_unknown_cap).
- Private-role inbound cap for libp2p peers (libp2p_inbound_cap_private).
- Global inbound cap still enforced by connection manager.

Observability
- Helper API "metrics" action returns relay + DCUtR metrics snapshot.
- Relay auto metrics: candidate counts, selection cycles, reservations, probes, rotations, backoff.
- DCUtR metrics: dialback attempts/success/failure/skip reasons.

Config + CLI knobs (high level)
- Mode/role/identity path (persisted identity support).
- Listen ports: libp2p listen + relay listen.
- Reservations list (static/manual) and relay candidates list (auto sources).
- External multiaddrs + advertise addresses.
- Relay advertise capacity + ttl.
- AutoNAT allow-private + confidence threshold.
- Relay pool knobs: max relays, max peers per relay, min sources, rng seed.
- Inbound caps: private, per-relay, unknown-relay.

Lab focus coverage
- Auto relay selection + reservation (no manual scripts).
- Auto role promotion (AutoNAT).
- Relay capability gossip + caps enforcement.
