# NAT Lab Record (Posterity)

Canonical infrastructure record for the DCUtR Proxmox/OpenWrt lab.

This file is for repeatability and historical context.
It complements `tcp-hole-punch/docs/DCUTR-LAB-RUNBOOK.md` (execution playbook).

## Non-Negotiable Rule

Do not change router config while running baseline verification.
Only read-only inspection commands are allowed during evidence capture.

## Scope

- Document lab topology and addressing used for DCUtR validation.
- Document router setup as observed in existing reports/runbook.
- Explain why this setup mimics home-router NAT behavior for hole punching.
- Give a reproducible mimic recipe for future contributors.

## Sources

- `tcp-hole-punch/docs/DCUTR-LAB-RUNBOOK.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-1.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-2.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-3.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-4.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-5.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-6.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-7.md`
- `tcp-hole-punch/docs/reports/LAB-REPORT-8.md`
- `tcp-hole-punch/docs/reports/STRESS-TEST-REPORT.md`

## Live Verification (2026-02-22 UTC)

Read-only live audit completed on Sunday, February 22, 2026.
No config mutations were executed.

### Access paths used

- Router B direct management: `ssh root@10.0.3.62` (port `22`).
- Router A management from LAN side via Node A: `ssh -J root@10.0.3.11 ubuntu@192.168.1.10`, then `ssh root@192.168.1.1`.
- Note: `10.0.3.61:22` is closed/refused from transit side.
- Note: `10.0.3.61:2222` is DNAT to Node A SSH (`192.168.1.10:22`), not router management SSH.

### Captured raw artifacts

- `tcp-hole-punch/docs/reports/artifacts/router-a-snapshot-2026-02-22.txt`
- `tcp-hole-punch/docs/reports/artifacts/router-b-snapshot-2026-02-22.txt`

### Confirmed facts from live snapshots

- Both routers run OpenWrt SNAPSHOT `r31958-7f5c7b8626` on kernel `6.12.58`.
- Router A addressing: WAN `10.0.3.61/24` (gw `10.0.3.1`), LAN `192.168.1.1/24`.
- Router B addressing: WAN `10.0.3.62/24` (gw `10.0.3.1`), LAN `192.168.2.1/24`.
- Firewall WAN zone on both routers: `masq='1'`, `fullcone='1'`, `mtu_fix='1'`.
- `nft_fullcone` kernel module loaded on both routers.
- nftables ruleset on both routers includes `meta nfproto ipv4 fullcone` in `srcnat_wan`.
- Router A port-forward present: `2222 -> 192.168.1.10:22` (`ssh-node-a`).
- Router B port-forward present: `2223 -> 192.168.2.10:22` (`ssh-node-b`).

## Lab Invariants

- Three-node shape: one relay/public anchor and two NATed clients.
- Two independent private LANs: `192.168.1.0/24` (Router A), `192.168.2.0/24` (Router B).
- Router WAN side sits on shared `10.0.3.0/24`.
- No manual port-forward dependency for the hole-punch path.
- `--disable-upnp` used in historical lab commands.
- Dedicated libp2p port (`16112`) preferred over shared TCP P2P port (`16111`).

## Canonical Addressing

### Current runbook profile

| Component | Addressing | Notes |
|---|---|---|
| Relay | `10.0.3.26` | Directly reachable on lab transit segment |
| Router A WAN | `10.0.3.61` | NAT IP used by Node A external multiaddr |
| Router A LAN GW | `192.168.1.1/24` | Node A default gateway |
| Node A | `192.168.1.10` | NATed client |
| Router B WAN | `10.0.3.62` | NAT IP used by Node B external multiaddr |
| Router B LAN GW | `192.168.2.1/24` | Node B default gateway |
| Node B | `192.168.2.10` | NATed client |
| Jump host | `10.0.3.11` | Used with `ssh -J` in runbook |

### Historical profile (reports dated 2025-11-27 to 2025-11-30)

| Component | Addressing | Notes |
|---|---|---|
| Relay | `10.0.3.26` | Current relay/public anchor |
| Router A WAN | `10.0.3.61` | Same as current |
| Node A | `192.168.1.10` | Same as current |
| Router B WAN | `10.0.3.62` | Same as current |
| Node B | `192.168.2.10` | Same as current |

Use one profile consistently per run.
Do not mix relay addresses between profiles.

## Router Setup (As Observed)

### Router A

- Platform: OpenWrt (full-cone NAT lab router).
- WAN/transit side: `10.0.3.61`.
- LAN side: `192.168.1.1/24`.
- Client subnet: Node A at `192.168.1.10/24`, gateway `192.168.1.1`.
- Proxmox bridge label seen in reports: `vmbr10`.

### Router B

- Platform: OpenWrt (full-cone NAT lab router).
- WAN/transit side: `10.0.3.62`.
- LAN side: `192.168.2.1/24`.
- Client subnet: Node B at `192.168.2.10/24`, gateway `192.168.2.1`.
- Proxmox bridge label seen in reports: `vmbr20`.

### Router NAT mode requirement

- Full-cone NAT on both routers.
- Runbook troubleshooting explicitly calls out `nft_fullcone` when NAT behavior is wrong.
- External multiaddrs must use router WAN NAT IPs, not private LAN IPs.

## Why This Mimics Home-Router NAT

This lab targets the practical behavior hole punching depends on:

- Endpoint-independent mapping: outbound flow from private node establishes a reusable public mapping on router WAN IP.
- Endpoint-independent filtering: once mapping exists, inbound packets to mapped tuple can be accepted from peers reached during coordination.
- No explicit manual port forwarding: mirrors typical consumer-router usage where apps rely on outbound-created mappings.
- Relay + simultaneous dial: matches DCUtR flow (coordinate over relay, then attempt direct path through NAT mappings).

This is intentionally closer to home setups than symmetric NAT behavior.
Symmetric NAT often breaks this path and should be treated as a different test matrix.

## Read-Only Router Snapshot Procedure

Run on each router to preserve exact config evidence without mutation.

```bash
set -euo pipefail

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="/tmp/router-snapshot-${STAMP}.txt"

{
  echo "=== system ==="
  uname -a
  cat /etc/openwrt_release 2>/dev/null || true

  echo "=== interfaces/routes ==="
  ip -4 addr show
  ip -4 route show

  echo "=== uci network ==="
  uci show network

  echo "=== uci firewall ==="
  uci show firewall

  echo "=== firewall config file ==="
  cat /etc/config/firewall

  echo "=== full-cone/module checks ==="
  lsmod | grep -E 'nft_fullcone|fullcone|nf_nat' || true

  echo "=== nft ruleset ==="
  nft list ruleset
} > "${OUT}"

echo "snapshot: ${OUT}"
```

Optional: copy snapshot off-router for long-term storage alongside report artifacts.

## Recreate/Mimic Guide

1. Build three VMs/hosts: relay on shared transit network, Node A behind Router A (`192.168.1.0/24`), Node B behind Router B (`192.168.2.0/24`).
2. Put router WAN interfaces on shared transit network (`10.0.3.0/24`).
3. Ensure both routers provide full-cone NAT behavior.
4. Build libp2p-enabled `kaspad` on all nodes.
5. Follow `tcp-hole-punch/docs/DCUTR-LAB-RUNBOOK.md` exactly.
6. Set `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true` on all lab nodes.
7. Set `--libp2p-external-multiaddrs` to NAT WAN IPs (`10.0.3.61`/`10.0.3.62`), never `192.168.x.x`.
8. Trigger relay-circuit dial and validate direct upgrade signals.

## Success Signals

- Reservation acceptance on relay for both NATed nodes.
- DCUtR success event in logs (`result: Ok(ConnectionId(...))`).
- Bridge path upgrade to direct (`path: Direct`).
- Helper peers output contains `"path":"direct"` for the target.
- Step 5 runbook classification: `PASS` or `PASS-FLAKY`.

## Common Failure Signatures

| Symptom | Likely cause | Action |
|---|---|---|
| `AttemptsExceeded(3)` | AutoNAT private-IP handling missing | Set `KASPAD_LIBP2P_AUTONAT_ALLOW_PRIVATE=true` on all nodes |
| `AttemptsExceeded(3)` | NAT not behaving full-cone | Verify router full-cone config/modules (`nft_fullcone`) |
| `No path: Direct` | Wrong external multiaddr | Use router WAN NAT IPs |
| `reservation rejected` | Relay peer ID mismatch | Re-check relay peer ID and reservation multiaddr |

## Change Control

- If topology or addressing changes, update this file and `tcp-hole-punch/docs/DCUTR-LAB-RUNBOOK.md` in the same commit.
- Keep report dates explicit when documenting new runs.
- Never edit router config mid-baseline; collect evidence first, then plan changes separately.
