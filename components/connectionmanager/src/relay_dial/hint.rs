use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

pub(super) fn relay_circuit_addr(hint: &str, target_peer_id: &str) -> Option<String> {
    let mut base = if hint.starts_with('/') {
        hint.trim_end_matches('/').to_string()
    } else {
        let socket = SocketAddr::from_str(hint).ok()?;
        match socket.ip() {
            IpAddr::V4(ip) => format!("/ip4/{ip}/tcp/{}", socket.port()),
            IpAddr::V6(ip) => format!("/ip6/{ip}/tcp/{}", socket.port()),
        }
    };

    if !base.contains("/p2p-circuit") {
        base.push_str("/p2p-circuit");
    }
    base.push_str("/p2p/");
    base.push_str(target_peer_id);
    Some(base)
}

pub(super) fn relay_hint_key(hint: &str) -> Option<String> {
    if hint.starts_with('/') {
        if let Some((ip, port)) = parse_multiaddr_ip_port(hint) {
            return Some(SocketAddr::new(ip, port).to_string());
        }
        return Some(hint.trim_end_matches('/').to_string());
    }

    SocketAddr::from_str(hint).ok().map(|socket| socket.to_string())
}

pub(super) fn relay_hint_peer_id(hint: &str) -> Option<String> {
    if !hint.starts_with('/') {
        return None;
    }
    let parts: Vec<&str> = hint.split('/').filter(|part| !part.is_empty()).collect();
    for idx in 0..parts.len().saturating_sub(1) {
        if parts[idx] == "p2p" {
            return Some(parts[idx + 1].to_string());
        }
    }
    None
}

fn parse_multiaddr_ip_port(hint: &str) -> Option<(IpAddr, u16)> {
    let parts: Vec<&str> = hint.split('/').filter(|part| !part.is_empty()).collect();
    let mut ip: Option<IpAddr> = None;
    let mut port: Option<u16> = None;
    for idx in 0..parts.len().saturating_sub(1) {
        match parts[idx] {
            "ip4" | "ip6" if ip.is_none() => {
                if let Ok(parsed) = IpAddr::from_str(parts[idx + 1]) {
                    ip = Some(parsed);
                }
            }
            "tcp" if port.is_none() => {
                if let Ok(parsed) = parts[idx + 1].parse::<u16>() {
                    port = Some(parsed);
                }
            }
            _ => {}
        }
    }
    match (ip, port) {
        (Some(ip), Some(port)) => Some((ip, port)),
        _ => None,
    }
}
