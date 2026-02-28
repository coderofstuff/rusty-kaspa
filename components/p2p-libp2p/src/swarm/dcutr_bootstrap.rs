use super::*;

/// A minimal handler/behaviour that:
/// 1. Advertises `/libp2p/dcutr` as a supported protocol so Identify includes it in the protocol list
/// 2. Seeds configured external addresses as DCUtR candidates via ToSwarm events
///
/// The address seeding is critical because `swarm.add_external_address()` bypasses DCUtR's candidate
/// mechanism. DCUtR only sees addresses that flow through `NewExternalAddrCandidate` events.
///
/// This behaviour intercepts `ExternalAddrConfirmed` events and re-emits them as `NewExternalAddrCandidate`
/// so that DCUtR's internal address cache gets populated. Without this, addresses added via
/// `swarm.add_external_address()` (e.g., from Identify or bootstrap) would never reach DCUtR.
#[derive(Clone, Default)]
pub struct DcutrBootstrapBehaviour {
    /// Addresses waiting to be emitted as NewExternalAddrCandidate
    pending_candidates: VecDeque<Multiaddr>,
    /// Addresses that have been proposed as candidates and are waiting to be confirmed
    /// (only used for addresses from constructor, not from swarm events)
    pending_confirms: VecDeque<Multiaddr>,
    /// Addresses we've already emitted as NewExternalAddrCandidate (to avoid duplicates/loops)
    emitted_candidates: HashSet<Multiaddr>,
}

impl DcutrBootstrapBehaviour {
    /// Create a new bootstrap behaviour that will seed the given addresses as DCUtR candidates.
    pub fn new(external_addrs: Vec<Multiaddr>) -> Self {
        Self {
            pending_candidates: external_addrs.into_iter().collect(),
            pending_confirms: VecDeque::new(),
            emitted_candidates: HashSet::new(),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct DcutrAdvertiseUpgrade;

#[derive(Clone, Default)]
pub struct DcutrAdvertiseHandler;

impl NetworkBehaviour for DcutrBootstrapBehaviour {
    type ConnectionHandler = DcutrAdvertiseHandler;
    type ToSwarm = ();

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: libp2p::PeerId,
        _: &libp2p::Multiaddr,
        _: &libp2p::Multiaddr,
    ) -> Result<Self::ConnectionHandler, swarm::ConnectionDenied> {
        Ok(DcutrAdvertiseHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: libp2p::PeerId,
        _: &libp2p::Multiaddr,
        _: libp2p::core::Endpoint,
        _: libp2p::core::transport::PortUse,
    ) -> Result<Self::ConnectionHandler, swarm::ConnectionDenied> {
        Ok(DcutrAdvertiseHandler)
    }

    fn on_swarm_event(&mut self, event: swarm::behaviour::FromSwarm) {
        // Intercept ExternalAddrConfirmed events and re-emit them as NewExternalAddrCandidate
        // so that DCUtR's internal address cache gets populated.
        match &event {
            swarm::behaviour::FromSwarm::NewExternalAddrCandidate(e) => {
                debug!("DcutrBootstrapBehaviour received FromSwarm::NewExternalAddrCandidate: {}", e.addr);
            }
            swarm::behaviour::FromSwarm::ExternalAddrConfirmed(e) => {
                debug!("DcutrBootstrapBehaviour received FromSwarm::ExternalAddrConfirmed: {}", e.addr);
                // If we haven't emitted this address as a candidate yet, queue it for emission.
                // This ensures addresses added via swarm.add_external_address() (which emits
                // ExternalAddrConfirmed directly) get re-emitted as NewExternalAddrCandidate
                // so that DCUtR sees them.
                if !self.emitted_candidates.contains(e.addr) {
                    debug!("DcutrBootstrapBehaviour: queueing {} for NewExternalAddrCandidate emission", e.addr);
                    self.pending_candidates.push_back(e.addr.clone());
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(&mut self, _: libp2p::PeerId, _: ConnectionId, _: swarm::THandlerOutEvent<Self>) {}

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // First, emit any pending candidates (NewExternalAddrCandidate)
        if let Some(addr) = self.pending_candidates.pop_front() {
            // Track that we've emitted this address to avoid re-emitting it later
            // when we receive the ExternalAddrConfirmed event for it
            self.emitted_candidates.insert(addr.clone());
            debug!("DcutrBootstrapBehaviour: emitting NewExternalAddrCandidate for {}", addr);
            // Queue it for confirmation after the candidate event is processed
            // (only for addresses from constructor, not from swarm events which are already confirmed)
            self.pending_confirms.push_back(addr.clone());
            return Poll::Ready(ToSwarm::NewExternalAddrCandidate(addr));
        }

        // Then, confirm any pending addresses (ExternalAddrConfirmed)
        // This is only for addresses from the constructor that need to be confirmed
        if let Some(addr) = self.pending_confirms.pop_front() {
            debug!("DcutrBootstrapBehaviour: emitting ExternalAddrConfirmed for {}", addr);
            return Poll::Ready(ToSwarm::ExternalAddrConfirmed(addr));
        }

        Poll::Pending
    }
}

impl ConnectionHandler for DcutrAdvertiseHandler {
    type FromBehaviour = ();
    type ToBehaviour = ();
    type InboundProtocol = DcutrAdvertiseUpgrade;
    type OutboundProtocol = DcutrAdvertiseUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DcutrAdvertiseUpgrade, ())
    }

    fn on_behaviour_event(&mut self, _: Self::FromBehaviour) {}

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>> {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<Self::InboundProtocol, Self::OutboundProtocol, Self::InboundOpenInfo, Self::OutboundOpenInfo>,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound { .. }) => {}
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound { .. }) => {}
            ConnectionEvent::DialUpgradeError(DialUpgradeError { .. }) => {}
            ConnectionEvent::ListenUpgradeError(_) => {}
            ConnectionEvent::AddressChange(_) => {}
            ConnectionEvent::LocalProtocolsChange(_) => {}
            ConnectionEvent::RemoteProtocolsChange(_) => {}
            _ => {}
        }
    }
}

impl UpgradeInfo for DcutrAdvertiseUpgrade {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<StreamProtocol>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(dcutr::PROTOCOL_NAME.clone())
    }
}

impl InboundUpgrade<Stream> for DcutrAdvertiseUpgrade {
    type Output = ();
    type Error = Infallible;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, _: Stream, _: Self::Info) -> Self::Future {
        future::ready(Ok(()))
    }
}

impl OutboundUpgrade<Stream> for DcutrAdvertiseUpgrade {
    type Output = ();
    type Error = Infallible;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, _: Stream, _: Self::Info) -> Self::Future {
        future::ready(Ok(()))
    }
}
