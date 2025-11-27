# Lab Report 6 – Proxmox NAT DCUtR Verification

## Context
**Date:** 2025-11-27
**Branch:** tcp-hole-punch-clean
**Commit:** cf88ef52631ae87d33d1d4ec955a5f4cfe6d2067

**Objective:** Verify DCUtR and TCP Hole Punching in the Proxmox NAT lab environment using the `tcp-hole-punch-clean` branch.

## Environment Setup
- **Node C (Relay/Public):** 10.0.3.50 (Peer ID: `12D3KooWKQFGzGUTKnN3pLZiGz7e4hH3omkxjYmXEjZkMf5vct2P`)
- **Node A (NAT):** 192.168.1.10 (Peer ID: `12D3KooWD56f6cpfFURi5tTVFNpBSXFgJDFno1UPqLzNLAX9azJq`)
- **Node B (NAT):** 192.168.2.10 (Peer ID: `12D3KooWQ9wLWb1a2BbbZ8vycWtwa2D8WrRinrpMrfm78MAfX3Wo`)
- **Helper API:**
  - Node C: 0.0.0.0:38080
  - Node A/B: 127.0.0.1:38080

**Build Command:**
```bash
cargo build --release --features libp2p --bin kaspad
```

**Start Commands:**
Used `tcp-hole-punch/lab/hole_punch_lab.sh`.
```bash
export C_PEER_ID=12D3KooWKQFGzGUTKnN3pLZiGz7e4hH3omkxjYmXEjZkMf5vct2P
bash tcp-hole-punch/lab/hole_punch_lab.sh start-ab
```

## Findings

### 1. Identify & DCUtR Advertisement
Initially, Identify events showed `dcutr=false` from the relay (Node C) to A/B, and A/B did not immediately advertise DCUtR.
However, after a short period (likely connection stabilization or Identify push), logs on Node A showed B advertising DCUtR support:
```
INFO identify received from 12D3KooWQ9wLWb1a2BbbZ8vycWtwa2D8WrRinrpMrfm78MAfX3Wo: protocols=[... "/libp2p/dcutr" ...] (dcutr=true)
```

### 2. AutoNAT Status
AutoNAT appears to be failing on all nodes (A, B, and C).
Logs consistently show:
```
DEBUG libp2p autonat event: OutboundProbe(Error { probe_id: ProbeId(0), peer: None, error: NoServer })
```
This indicates that A and B could not find a suitable AutoNAT server, even though Node C advertises `/libp2p/autonat/1.0.0`. This failure to confirm "Private" status might be impacting DCUtR initiation or reliability.

### 3. DCUtR Hole Punch Attempts
Automatic hole punching was attempted but failed.
Log sequence on Node A:
```
INFO libp2p dcutr: initiated dial-back to 12D3KooWD56f6cpfFURi5tTVFNpBSXFgJDFno1UPqLzNLAX9azJq via relay 12D3KooWKQFGzGUTKnN3pLZiGz7e4hH3omkxjYmXEjZkMf5vct2P
...
DEBUG libp2p dcutr event: Event { remote_peer_id: ..., result: Err(Error { inner: OutboundError(Io(Kind(UnexpectedEof))) }) }
```
On the inbound side (or subsequent retry), errors like `InboundError(Protocol(NoAddresses))` were also observed, suggesting issues with address exchange or selection during the DCUtR handshake.

### 4. Helper API Trigger
Manual triggering of the hole punch using the helper API failed because the nodes were already connected via the relay.
Request:
```bash
echo '{"action":"dial","multiaddr":"/ip4/10.0.3.50/tcp/16112/p2p/12D3KooWKQFGzGUTKnN3pLZiGz7e4hH3omkxjYmXEjZkMf5vct2P/p2p-circuit/p2p/12D3KooWQ9wLWb1a2BbbZ8vycWtwa2D8WrRinrpMrfm78MAfX3Wo"}' | nc -w 5 127.0.0.1 38080
```
Response:
```json
{"ok":false,"error":"dial failed: libp2p dial failed: libp2p dial cancelled"}
```
This cancellation is expected behavior when a connection (even via relay) already exists, but it confirms we cannot force a *new* direct dial if the system considers the relay connection sufficient/active and DCUtR hasn't upgraded it.

### 5. Kaspa Connectivity
Kaspa peers are established between A and B, but they appear to remain routed via the relay.
Log confirmation:
```
INFO libp2p_bridge: connecting Kaspa peer from libp2p stream with metadata TransportMetadata { ..., path: Relay { relay_id: Some("...") }, ... }
```
No `path: Direct` upgrade was observed for the A-B connection.

## Conclusion
**DCUtR hole punch failed.**
The chain breaks at the hole punch execution step:
1.  **AutoNAT** fails to verify A/B as Private (`NoServer`), which may prevent optimal DCUtR behavior.
2.  **DCUtR** protocol initiates but fails with `UnexpectedEof` (connection drop during negotiation/punch) or `NoAddresses`.
3.  **Kaspa Bridge** successfully connects peers A and B via **Relay**, but the connection is never upgraded to a direct TCP link.

## Recommendations
- Investigate why Node C is not accepted/functioning as an AutoNAT server for A/B.
- Debug the `UnexpectedEof` in the DCUtR handler—this suggests a premature socket closure or NAT incompatibility (though `dcutr` should handle full cone NATs).
- Verify if `NoAddresses` implies missing public address advertisements in the DCUtR protocol payload, despite Identify showing them.
