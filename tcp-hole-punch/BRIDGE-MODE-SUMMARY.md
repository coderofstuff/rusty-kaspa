# Bridge Mode Summary

- Modes: `off` (TCP only), `bridge` (TCP outbound with libp2p runtime running), `full/helper` (libp2p-only overlay; helper is an alias).
- Roles: `public` advertises relay service/port and uses the existing libp2p inbound split; `private/auto` do not advertise relay and cap libp2p inbound peers (default 16) while leaving TCP inbound unchanged. Auto currently resolves to private unless a helper listen address is set.
- Outbound: Daemon uses TCP for bridge mode; the libp2p connector also has a single libp2p attempt with TCP fallback in bridge mode, gated by a 10-minute per-address cooldown.
- Status/RPC: bridge is reported as `full` until the RPC enum grows a bridge variant.
- Quick CLI:
  - Mainnet-safe hybrid: `kaspad --libp2p-mode=bridge`
  - Public relay: `kaspad --libp2p-mode=bridge --libp2p-role=public --libp2p-helper-listen=0.0.0.0:38080`
  - Private/DCUtR node: `kaspad --libp2p-mode=bridge --libp2p-role=private --libp2p-reservations=... --libp2p-external-multiaddrs=...`
