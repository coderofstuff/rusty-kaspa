use super::{RelayCandidateUpdate, RelaySource};
use kaspa_utils::networking::NetAddress;
use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

pub fn relay_key_from_parts(ip: IpAddr, port: u16) -> String {
    format!("{ip}:{port}")
}

pub fn relay_update_from_netaddr(
    net_address: NetAddress,
    relay_port: u16,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Result<RelayCandidateUpdate, libp2p::multiaddr::Error> {
    let key = relay_key_from_parts(net_address.ip.0, relay_port);
    let address = match net_address.ip.0 {
        IpAddr::V4(v4) => Multiaddr::from_str(&format!("/ip4/{}/tcp/{}", v4, relay_port))?,
        IpAddr::V6(v6) => Multiaddr::from_str(&format!("/ip6/{}/tcp/{}", v6, relay_port))?,
    };
    Ok(RelayCandidateUpdate {
        key,
        address,
        net_address: Some(NetAddress::new(net_address.ip, relay_port)),
        relay_peer_id: None,
        capacity,
        ttl: Some(ttl),
        source,
    })
}

pub fn relay_update_from_multiaddr(
    address: Multiaddr,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Option<RelayCandidateUpdate> {
    let mut ip: Option<IpAddr> = None;
    let mut port: Option<u16> = None;
    for protocol in address.iter() {
        match protocol {
            Protocol::Ip4(v4) => ip = Some(IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(IpAddr::V6(v6)),
            Protocol::Tcp(p) => port = Some(p),
            _ => {}
        }
    }
    let relay_peer_id = relay_peer_from_multiaddr(&address);
    let ip = ip?;
    let port = port?;
    let key = relay_key_from_parts(ip, port);
    let net_address = NetAddress::new(ip.into(), port);
    Some(RelayCandidateUpdate { key, address, net_address: Some(net_address), relay_peer_id, capacity, ttl: Some(ttl), source })
}

pub fn relay_update_from_multiaddr_str(
    address: &str,
    ttl: Duration,
    source: RelaySource,
    capacity: Option<usize>,
) -> Option<RelayCandidateUpdate> {
    let addr = Multiaddr::from_str(address).ok()?;
    relay_update_from_multiaddr(addr, ttl, source, capacity)
}

fn relay_peer_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    let mut saw_circuit = false;
    let mut last_peer_before_circuit: Option<PeerId> = None;
    let mut last_peer: Option<PeerId> = None;
    for protocol in address.iter() {
        match protocol {
            Protocol::P2p(peer_id) => {
                last_peer = Some(peer_id);
                if !saw_circuit {
                    last_peer_before_circuit = Some(peer_id);
                }
            }
            Protocol::P2pCircuit => saw_circuit = true,
            _ => {}
        }
    }
    if saw_circuit { last_peer_before_circuit } else { last_peer }
}
