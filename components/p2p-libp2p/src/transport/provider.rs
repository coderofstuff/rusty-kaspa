use super::*;

/// Libp2p stream provider backed by a libp2p swarm.
pub struct SwarmStreamProvider {
    config: Config,
    command_tx: mpsc::Sender<SwarmCommand>,
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    shutdown: Trigger,
    task: Mutex<Option<JoinHandle<()>>>,
    role_updates: Option<watch::Receiver<crate::Role>>,
    relay_hint_updates: Option<watch::Receiver<Option<String>>>,
    metrics: Arc<Libp2pMetrics>,
}

impl SwarmStreamProvider {
    pub fn new(config: Config, identity: Libp2pIdentity) -> Result<Self, Libp2pError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| Libp2pError::ListenFailed("missing tokio runtime".into()))?;
        Self::with_handle(config, identity, handle)
    }

    pub fn with_handle(config: Config, identity: Libp2pIdentity, handle: tokio::runtime::Handle) -> Result<Self, Libp2pError> {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_BOUND);
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_CHANNEL_BOUND);
        let (shutdown, shutdown_listener) = triggered::trigger();
        let protocol = default_stream_protocol();
        let metrics = Libp2pMetrics::new();
        // Pass config to build_streaming_swarm to configure AutoNAT
        let swarm = build_streaming_swarm(&identity, &config, protocol.clone())?;

        let listen_multiaddrs = if config.listen_addresses.is_empty() {
            vec![default_listen_addr()]
        } else {
            config
                .listen_addresses
                .iter()
                .filter_map(|addr| match to_multiaddr(NetAddress::new((*addr).ip().into(), addr.port())) {
                    Ok(ma) => Some(ma),
                    Err(err) => {
                        warn!("invalid libp2p listen address {}: {err}", addr);
                        None
                    }
                })
                .collect()
        };
        let mut external_multiaddrs = parse_multiaddrs(&config.external_multiaddrs)?;
        external_multiaddrs.extend(config.advertise_addresses.iter().filter_map(|addr| {
            match to_multiaddr(NetAddress::new((*addr).ip().into(), addr.port())) {
                Ok(ma) => Some(ma),
                Err(err) => {
                    warn!("invalid libp2p advertise address {}: {err}", addr);
                    None
                }
            }
        }));
        let reservations = parse_reservation_targets(&config.reservations)?;
        let effective_role = if matches!(config.role, crate::Role::Auto) { crate::Role::Private } else { config.role };
        let (role_tx, role_rx) = watch::channel(effective_role);
        let (relay_hint_tx, relay_hint_rx) = watch::channel(None);
        let auto_role_required_autonat = config.autonat.confidence_threshold.max(1);
        let auto_role_required_direct = AUTO_ROLE_REQUIRED_DIRECT.max(1);
        let allow_private_addrs = !config.autonat.server_only_if_public;
        let task = handle.spawn(
            SwarmDriver::new(
                swarm,
                command_rx,
                incoming_tx,
                listen_multiaddrs,
                external_multiaddrs,
                allow_private_addrs,
                reservations,
                role_tx,
                relay_hint_tx,
                config.role,
                config.max_peers_per_relay,
                AUTO_ROLE_WINDOW,
                auto_role_required_autonat,
                auto_role_required_direct,
                shutdown_listener,
                Some(metrics.clone()),
            )
            .run(),
        );

        Ok(Self {
            config,
            command_tx,
            incoming: Mutex::new(incoming_rx),
            shutdown,
            task: Mutex::new(Some(task)),
            role_updates: Some(role_rx),
            relay_hint_updates: Some(relay_hint_rx),
            metrics,
        })
    }

    async fn ensure_listening(&self) -> Result<(), Libp2pError> {
        let (tx, rx) = oneshot::channel();
        info!("libp2p ensure listening on configured addresses");
        self.command_tx
            .send(SwarmCommand::EnsureListening { respond_to: tx })
            .await
            .map_err(|_| Libp2pError::ListenFailed("libp2p driver stopped".into()))?;

        rx.await.map_err(|_| Libp2pError::ListenFailed("libp2p driver stopped".into()))?
    }
}

impl Libp2pStreamProvider for SwarmStreamProvider {
    fn dial<'a>(&'a self, address: NetAddress) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let multiaddr = to_multiaddr(address)?;
            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Dial { address: multiaddr, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await
                .unwrap_or_else(|_| Err(Libp2pError::DialFailed("libp2p dial cancelled".into())))
                .map(|(metadata, _, stream)| (metadata, stream))
        })
    }

    fn dial_multiaddr<'a>(
        &'a self,
        multiaddr: Multiaddr,
    ) -> BoxFuture<'a, Result<(TransportMetadata, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Dial { address: multiaddr, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await
                .unwrap_or_else(|_| Err(Libp2pError::DialFailed("libp2p dial cancelled".into())))
                .map(|(metadata, _, stream)| (metadata, stream))
        })
    }

    fn probe_relay<'a>(&'a self, address: Multiaddr) -> BoxFuture<'a, Result<PeerId, Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::ProbeRelay { address, respond_to })
                .await
                .map_err(|_| Libp2pError::DialFailed("libp2p driver stopped".into()))?;

            rx.await.unwrap_or_else(|_| Err(Libp2pError::DialFailed("relay probe cancelled".into())))
        })
    }

    fn listen<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<(TransportMetadata, StreamDirection, Box<dyn FnOnce() + Send>, BoxedLibp2pStream), Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let incoming = &self.incoming;
        let provider = self;
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            provider.ensure_listening().await?;

            let mut rx = incoming.lock().await;
            match rx.recv().await {
                Some(incoming) => {
                    let closer: Box<dyn FnOnce() + Send> = Box::new(|| {});
                    Ok((incoming.metadata, incoming.direction, closer, incoming.stream))
                }
                None => Err(Libp2pError::ListenFailed("libp2p incoming channel closed".into())),
            }
        })
    }

    fn reserve<'a>(&'a self, target: Multiaddr) -> BoxFuture<'a, Result<ReservationHandle, Libp2pError>> {
        let enabled = self.config.mode.is_enabled();
        let tx = self.command_tx.clone();
        Box::pin(async move {
            if !enabled {
                return Err(Libp2pError::Disabled);
            }

            let (respond_to, rx) = oneshot::channel();
            tx.send(SwarmCommand::Reserve { target, respond_to })
                .await
                .map_err(|_| Libp2pError::ReservationFailed("libp2p driver stopped".into()))?;

            let listener = rx.await.unwrap_or_else(|_| Err(Libp2pError::ReservationFailed("libp2p reservation cancelled".into())))?;
            let release_tx = tx.clone();
            Ok(ReservationHandle::new(async move {
                let (ack_tx, ack_rx) = oneshot::channel();
                if release_tx.send(SwarmCommand::ReleaseReservation { listener_id: listener, respond_to: ack_tx }).await.is_ok() {
                    let _ = ack_rx.await;
                }
            }))
        })
    }

    fn peers_snapshot<'a>(&'a self) -> BoxFuture<'a, Vec<PeerSnapshot>> {
        let tx = self.command_tx.clone();
        Box::pin(async move {
            let (respond_to, rx) = oneshot::channel();
            if tx.send(SwarmCommand::PeersSnapshot { respond_to }).await.is_err() {
                return Vec::new();
            }
            rx.await.unwrap_or_default()
        })
    }

    fn role_updates(&self) -> Option<watch::Receiver<crate::Role>> {
        self.role_updates.clone()
    }

    fn relay_hint_updates(&self) -> Option<watch::Receiver<Option<String>>> {
        self.relay_hint_updates.clone()
    }

    fn metrics(&self) -> Option<Arc<Libp2pMetrics>> {
        Some(self.metrics.clone())
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        let trigger = self.shutdown.clone();
        let task = &self.task;
        Box::pin(async move {
            trigger.trigger();
            if let Some(handle) = task.lock().await.take() {
                let _ = handle.await;
            }
        })
    }
}
