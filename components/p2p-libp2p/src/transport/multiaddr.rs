use std::str::FromStr;

use kaspa_p2p_lib::PathKind;
use kaspa_utils::networking::{IpAddress, NetAddress};
use libp2p::PeerId;
use libp2p::multiaddr::{Multiaddr, Protocol};

use super::{Libp2pError, RelayInfo, ReservationTarget};

/// Translate a NetAddress (ip:port) into a libp2p multiaddr.
pub fn to_multiaddr(address: NetAddress) -> Result<Multiaddr, Libp2pError> {
    let multiaddr: Multiaddr = match address.ip {
        IpAddress(std::net::IpAddr::V4(v4)) => format!("/ip4/{}/tcp/{}", v4, address.port).parse::<Multiaddr>(),
        IpAddress(std::net::IpAddr::V6(v6)) => format!("/ip6/{}/tcp/{}", v6, address.port).parse::<Multiaddr>(),
    }
    .map_err(|e: libp2p::multiaddr::Error| Libp2pError::Multiaddr(e.to_string()))?;
    Ok(multiaddr)
}

/// Extract networking metadata (NetAddress + path info) from a multiaddr.
pub fn multiaddr_to_metadata(address: &Multiaddr) -> (Option<NetAddress>, PathKind) {
    let mut ip: Option<std::net::IpAddr> = None;
    let mut port: Option<u16> = None;
    let mut relay_id: Option<String> = None;
    let mut saw_circuit = false;
    let mut last_peer_id: Option<String> = None;

    for component in address.iter() {
        match component {
            Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
            Protocol::Tcp(p) => port = Some(p),
            Protocol::P2p(hash) => {
                let pid = hash.to_string();
                if saw_circuit && relay_id.is_none() {
                    relay_id = last_peer_id.take();
                }
                last_peer_id = Some(pid);
            }
            Protocol::P2pCircuit => {
                saw_circuit = true;
                relay_id = last_peer_id.take();
            }
            _ => {}
        }
    }

    let net = ip.map(|i| NetAddress::new(i.into(), port.unwrap_or(0)));
    let path = if saw_circuit {
        PathKind::Relay { relay_id }
    } else if net.is_some() {
        PathKind::Direct
    } else {
        PathKind::Unknown
    };

    (net, path)
}

pub(super) fn parse_multiaddrs(addrs: &[String]) -> Result<Vec<Multiaddr>, Libp2pError> {
    addrs.iter().map(|raw| Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))).collect()
}

pub(super) fn parse_reservation_targets(reservations: &[String]) -> Result<Vec<ReservationTarget>, Libp2pError> {
    reservations
        .iter()
        .map(|raw| {
            let multiaddr: Multiaddr = Multiaddr::from_str(raw).map_err(|e| Libp2pError::Multiaddr(e.to_string()))?;
            let peer_id = multiaddr
                .iter()
                .find_map(|p| if let Protocol::P2p(peer_id) = p { Some(peer_id) } else { None })
                .ok_or_else(|| Libp2pError::Multiaddr("reservation multiaddr missing peer id".into()))?;
            Ok(ReservationTarget { multiaddr, peer_id })
        })
        .collect()
}

pub(super) fn default_listen_addr() -> Multiaddr {
    Multiaddr::from_str("/ip4/0.0.0.0/tcp/0").expect("static multiaddr should parse")
}

pub(super) fn extract_remote_dcutr_candidates(listen_addrs: &[Multiaddr], allow_private_addrs: bool) -> Vec<Multiaddr> {
    let mut candidates: Vec<_> = listen_addrs
        .iter()
        .filter(|addr| {
            if !is_tcp_dialable(addr) {
                return false;
            }
            let Some(ip) = candidate_ip_addr(addr) else {
                return false;
            };
            is_usable_dcutr_candidate_ip(ip, allow_private_addrs)
        })
        .cloned()
        .collect();
    candidates.sort_by_key(|addr| {
        let score = candidate_ip_addr(addr)
            .map(|ip| {
                if IpAddress::new(ip).is_publicly_routable() {
                    3_u8
                } else if allow_private_addrs {
                    2_u8
                } else {
                    1_u8
                }
            })
            .unwrap_or(0_u8);
        (std::cmp::Reverse(score), addr.to_string())
    });
    candidates.dedup();
    candidates
}

pub(super) fn candidate_ip_addr(addr: &Multiaddr) -> Option<std::net::IpAddr> {
    let mut ip = None;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(v4) => ip = Some(std::net::IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(std::net::IpAddr::V6(v6)),
            _ => {}
        }
    }
    ip
}

pub(super) fn is_usable_dcutr_candidate_ip(ip: std::net::IpAddr, allow_private_addrs: bool) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    if allow_private_addrs {
        return true;
    }
    IpAddress::new(ip).is_publicly_routable()
}

pub(super) fn extract_relay_peer(addr: &Multiaddr) -> Option<PeerId> {
    let components: Vec<_> = addr.iter().collect();
    for window in components.windows(2) {
        if let [Protocol::P2p(peer), Protocol::P2pCircuit] = window {
            return Some(*peer);
        }
    }
    None
}

pub(super) fn relay_id_from_multiaddr(addr: &Multiaddr) -> Option<String> {
    extract_relay_peer(addr).map(|p| p.to_string())
}

/// Extracts the target peer from a relay circuit address.
/// For `/ip4/.../p2p/RELAY/p2p-circuit/p2p/TARGET`, returns TARGET.
pub(super) fn extract_circuit_target_peer(addr: &Multiaddr) -> Option<PeerId> {
    let components: Vec<_> = addr.iter().collect();
    // Find p2p-circuit, then look for the next P2p component
    let mut after_circuit = false;
    for p in components {
        if matches!(p, Protocol::P2pCircuit) {
            after_circuit = true;
        } else if after_circuit && let Protocol::P2p(peer_id) = p {
            return Some(peer_id);
        }
    }
    None
}

pub(super) fn relay_info_from_multiaddr(addr: &Multiaddr, local_peer_id: PeerId) -> Option<RelayInfo> {
    let relay_peer = extract_relay_peer(addr)?;
    let mut circuit_base = addr.clone();
    if !circuit_base.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
        circuit_base.push(Protocol::P2pCircuit);
    }
    strip_peer_suffix(&mut circuit_base, local_peer_id);
    Some(RelayInfo { relay_peer, circuit_base })
}

pub(super) fn strip_peer_suffix(addr: &mut Multiaddr, peer_id: PeerId) {
    if let Some(Protocol::P2p(last)) = addr.iter().last()
        && last == peer_id
    {
        addr.pop();
    }
}

pub(super) fn addr_uses_relay(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| matches!(p, Protocol::P2pCircuit))
}

pub(super) fn relay_probe_base(addr: &Multiaddr) -> Multiaddr {
    let mut base = Multiaddr::empty();
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2pCircuit) {
            break;
        }
        base.push(protocol);
    }
    base
}

pub(super) fn insert_relay_peer(addr: &Multiaddr, relay_peer: PeerId) -> Multiaddr {
    let mut out = Multiaddr::empty();
    let mut inserted = false;
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2pCircuit) && !inserted {
            out.push(Protocol::P2p(relay_peer));
            inserted = true;
        }
        out.push(protocol);
    }
    if !inserted {
        out.push(Protocol::P2p(relay_peer));
        out.push(Protocol::P2pCircuit);
    }
    out
}

pub(super) fn is_tcp_dialable(addr: &Multiaddr) -> bool {
    let mut has_ip = false;
    let mut has_tcp = false;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(_) | Protocol::Ip6(_) => has_ip = true,
            Protocol::Tcp(_) => has_tcp = true,
            Protocol::P2pCircuit => return false,
            _ => {}
        }
    }
    has_ip && has_tcp
}

pub(super) fn endpoint_uses_relay(endpoint: &libp2p::core::ConnectedPoint) -> bool {
    match endpoint {
        libp2p::core::ConnectedPoint::Dialer { address, .. } => addr_uses_relay(address),
        libp2p::core::ConnectedPoint::Listener { send_back_addr, local_addr } => {
            addr_uses_relay(send_back_addr) || addr_uses_relay(local_addr)
        }
    }
}
