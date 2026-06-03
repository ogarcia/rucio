//! HighID / LowID classification logic.
//!
//! ## Algorithm
//!
//! libp2p's `Identify` protocol lets remote peers tell us the address they
//! see us at (our *observed address*).  We collect these observations and
//! apply the following rules:
//!
//! 1. If no observed address has been received yet → **Unknown**
//! 2. If *any* observed address is a publicly routable IP (not RFC-1918 /
//!    loopback / link-local) → **HighId** (we are reachable from the internet)
//! 3. Otherwise (all observations are private addresses, e.g. LAN peers
//!    seeing our LAN IP) → **LowId**
//!
//! This is a best-effort heuristic.  A full confirmation would require an
//! external peer to actually dial us back; that can be layered on later.
//! For now this covers the common cases:
//!
//! - VPS / server with a public IP → HighId immediately
//! - Home user behind NAT seen only by LAN peers → stays Unknown until a
//!   WAN peer connects, then LowId (observed addr is the NAT external IP
//!   but on a non-listen port → still classified as LowId because the
//!   port mapping is ephemeral)
//! - Home user behind NAT with UPnP / port forward → HighId once a WAN
//!   peer reports the mapped address on our listen port

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use rucio_core::protocol::node::NodeClass;

/// Accumulated observations from remote peers via Identify.
#[derive(Debug, Default)]
pub struct ClassificationState {
    /// observed_addr → set of peers that reported it
    observations: HashMap<Multiaddr, Vec<PeerId>>,
    /// External addresses confirmed reachable by AutoNAT (a peer successfully
    /// dialled them back). Authoritative evidence of `HighId`, independent of
    /// the observation heuristic.
    confirmed_external: HashSet<Multiaddr>,
    /// The listen ports this node is bound to (from `NewListenAddr` events).
    listen_ports: Vec<u16>,
    /// Current classification.
    current: NodeClass,
}

impl ClassificationState {
    /// Record an observed address reported by `peer`.
    /// Returns the new class if it changed, `None` if unchanged.
    pub fn record_observation(
        &mut self,
        addr: Multiaddr,
        peer: PeerId,
        listen_addrs: &[Multiaddr],
    ) -> Option<NodeClass> {
        // Keep listen_ports in sync
        self.listen_ports = listen_addrs.iter().filter_map(port_of).collect();

        self.observations.entry(addr).or_default().push(peer);
        let new_class = self.classify();
        if new_class != self.current {
            self.current = new_class.clone();
            Some(new_class)
        } else {
            None
        }
    }

    /// Record an AutoNAT verdict for an external address: `confirmed = true`
    /// when a peer dialled it back successfully, `false` when it expired.
    /// Returns the new class if it changed, `None` if unchanged.
    pub fn record_confirmed_external(
        &mut self,
        addr: Multiaddr,
        confirmed: bool,
        listen_addrs: &[Multiaddr],
    ) -> Option<NodeClass> {
        self.listen_ports = listen_addrs.iter().filter_map(port_of).collect();
        if confirmed {
            self.confirmed_external.insert(addr);
        } else {
            self.confirmed_external.remove(&addr);
        }
        let new_class = self.classify();
        if new_class != self.current {
            self.current = new_class.clone();
            Some(new_class)
        } else {
            None
        }
    }

    pub fn current(&self) -> &NodeClass {
        &self.current
    }

    fn classify(&self) -> NodeClass {
        // AutoNAT-confirmed reachability is authoritative: if a peer dialled
        // back a public address on one of our listen ports, we are HighId
        // regardless of what the (outbound-only) observations suggested.
        if self
            .confirmed_external
            .iter()
            .any(|addr| is_public_addr(addr) && observed_on_listen_port(addr, &self.listen_ports))
        {
            return NodeClass::HighId;
        }

        if self.observations.is_empty() {
            return NodeClass::Unknown;
        }

        for addr in self.observations.keys() {
            if is_public_addr(addr) && observed_on_listen_port(addr, &self.listen_ports) {
                return NodeClass::HighId;
            }
        }

        // We have observations but none are publicly routable on a listen port.
        // Could be LAN-only peers or NAT mapping. Classify as LowId rather than
        // Unknown so the daemon doesn't advertise itself as a DHT provider.
        NodeClass::LowId
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return `true` if `addr` is a publicly reachable address on one of the
/// node's listen ports.
///
/// Used by the network task to decide whether to surface an observed address
/// as a confirmed external address.  Ephemeral NAT source ports (e.g. the
/// source port of an outgoing connection as echoed back by `identify`) are
/// public IPs but on random high ports — this function correctly rejects them.
pub fn is_stable_external_addr(addr: &Multiaddr, listen_addrs: &[Multiaddr]) -> bool {
    let listen_ports: Vec<u16> = listen_addrs.iter().filter_map(port_of).collect();
    is_public_addr(addr) && observed_on_listen_port(addr, &listen_ports)
}

/// Extract the TCP or UDP port from a multiaddr.
fn port_of(addr: &Multiaddr) -> Option<u16> {
    for proto in addr.iter() {
        match proto {
            Protocol::Tcp(p) | Protocol::Udp(p) => return Some(p),
            _ => {}
        }
    }
    None
}

/// Return true if the multiaddr's IP component is publicly routable.
fn is_public_addr(addr: &Multiaddr) -> bool {
    for proto in addr.iter() {
        let ip: IpAddr = match proto {
            Protocol::Ip4(a) => IpAddr::V4(a),
            Protocol::Ip6(a) => IpAddr::V6(a),
            _ => continue,
        };
        return is_public_ip(ip);
    }
    false
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            !a.is_loopback()
                && !a.is_private()
                && !a.is_link_local()
                && !a.is_broadcast()
                && !a.is_documentation()
                && !a.is_unspecified()
        }
        IpAddr::V6(a) => {
            !a.is_loopback()
                && !a.is_unspecified()
                // fc00::/7 — unique local
                && (a.segments()[0] & 0xfe00) != 0xfc00
                // fe80::/10 — link local
                && (a.segments()[0] & 0xffc0) != 0xfe80
        }
    }
}

/// Return true if the multiaddr's port matches one of our listen ports.
/// An observed address on a random high port (NAT mapping) is not reliable
/// for inbound connectivity.
fn observed_on_listen_port(addr: &Multiaddr, listen_ports: &[u16]) -> bool {
    if listen_ports.is_empty() {
        // No listen port info yet — be optimistic
        return true;
    }
    match port_of(addr) {
        Some(p) => listen_ports.contains(&p),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn peer() -> PeerId {
        PeerId::random()
    }

    fn addr(s: &str) -> Multiaddr {
        Multiaddr::from_str(s).unwrap()
    }

    fn listen(s: &str) -> Vec<Multiaddr> {
        vec![addr(s)]
    }

    #[test]
    fn unknown_with_no_observations() {
        let state = ClassificationState::default();
        assert_eq!(*state.current(), NodeClass::Unknown);
    }

    #[test]
    fn highid_on_public_ip_with_matching_port() {
        let mut state = ClassificationState::default();
        let result = state.record_observation(
            addr("/ip4/1.2.3.4/tcp/4321"),
            peer(),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::HighId));
        assert_eq!(*state.current(), NodeClass::HighId);
    }

    #[test]
    fn lowid_on_public_ip_with_nat_port() {
        let mut state = ClassificationState::default();
        // Observed on port 54321 (NAT ephemeral), listen on 4321
        let result = state.record_observation(
            addr("/ip4/1.2.3.4/tcp/54321"),
            peer(),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::LowId));
    }

    #[test]
    fn lowid_on_private_ip() {
        let mut state = ClassificationState::default();
        let result = state.record_observation(
            addr("/ip4/192.168.1.10/tcp/4321"),
            peer(),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::LowId));
    }

    #[test]
    fn autonat_confirmation_upgrades_lowid_to_highid() {
        let mut state = ClassificationState::default();
        // Outbound-only: observed on an ephemeral NAT port → LowId.
        let result = state.record_observation(
            addr("/ip4/1.2.3.4/tcp/54321"),
            peer(),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::LowId));

        // AutoNAT then confirms the translated listen-port address → HighId.
        let result = state.record_confirmed_external(
            addr("/ip4/1.2.3.4/tcp/4321"),
            true,
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::HighId));
        assert_eq!(*state.current(), NodeClass::HighId);

        // Expiry drops us back to LowId (the ephemeral observation remains).
        let result = state.record_confirmed_external(
            addr("/ip4/1.2.3.4/tcp/4321"),
            false,
            &listen("/ip4/0.0.0.0/tcp/4321"),
        );
        assert_eq!(result, Some(NodeClass::LowId));
    }

    #[test]
    fn is_stable_external_addr_accepts_public_listen_port() {
        assert!(is_stable_external_addr(
            &addr("/ip4/1.2.3.4/tcp/4321"),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        ));
    }

    #[test]
    fn is_stable_external_addr_rejects_ephemeral_port() {
        assert!(!is_stable_external_addr(
            &addr("/ip4/1.2.3.4/tcp/51958"),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        ));
    }

    #[test]
    fn is_stable_external_addr_rejects_private_ip() {
        assert!(!is_stable_external_addr(
            &addr("/ip4/192.168.1.10/tcp/4321"),
            &listen("/ip4/0.0.0.0/tcp/4321"),
        ));
    }

    #[test]
    fn no_change_on_same_class() {
        let mut state = ClassificationState::default();
        let listen = listen("/ip4/0.0.0.0/tcp/4321");
        state.record_observation(addr("/ip4/1.2.3.4/tcp/4321"), peer(), &listen);
        // Second observation, same class
        let result = state.record_observation(addr("/ip4/5.6.7.8/tcp/4321"), peer(), &listen);
        assert_eq!(result, None); // no change
    }
}
