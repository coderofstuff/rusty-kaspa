use async_channel::unbounded;
use kaspa_addressmanager::{AddressManager, NetAddress};
use kaspa_consensus::consensus::ctl::Ctl;
use kaspa_consensus::consensus::factory::MultiConsensusManagementStore;
use kaspa_consensus::model::stores::DB;
use kaspa_consensus_notify::root::ConsensusNotificationRoot;
use kaspa_consensusmanager::{ConsensusFactory, ConsensusInstance, ConsensusManager, DynConsensusCtl, SessionLock};
use kaspa_core::task::service::AsyncService;
use kaspa_core::task::tick::TickService;
use kaspa_core::time::unix_now;
use kaspa_core::{info, warn};
use kaspa_mining::manager::{MiningManager, MiningManagerProxy};
use kaspa_mining::MiningCounters;
use kaspa_p2p_flows::flow_context::FlowContext;
use kaspa_p2p_flows::service::P2pService;
use parking_lot::RwLock;
use std::fs;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

use super::idler::Idler;
use super::miner::Miner;

use kaspa_consensus::config::Config;
use kaspa_consensus::consensus::Consensus;
use kaspa_consensus_core::block::Block;
use kaspa_database::prelude::ConnBuilder;
use kaspa_database::utils::DbLifetime;
use kaspa_database::{create_permanent_db, create_temp_db};
use kaspa_utils::fd_budget;
use kaspa_utils::sim::Simulation;

use itertools::Itertools;

type ConsensusWrapper = (Arc<Consensus>, Vec<JoinHandle<()>>, DbLifetime, Vec<Arc<dyn AsyncService>>);

struct SimulatorConsensusFactory {
    consensus: Arc<Consensus>,
    config: Arc<Config>,
    management_store: Arc<RwLock<MultiConsensusManagementStore>>,
    db: Arc<DB>,
}

impl SimulatorConsensusFactory {
    fn new(notification_root: Arc<ConsensusNotificationRoot>, management_store: Arc<DB>, db: Arc<DB>, config: Arc<Config>) -> Self {
        let consensus = Arc::new(Consensus::new(
            db.clone(),
            config.clone(),
            Default::default(),
            notification_root,
            Default::default(),
            Default::default(),
            unix_now(),
        ));

        Self { consensus, config, db, management_store: Arc::new(RwLock::new(MultiConsensusManagementStore::new(management_store))) }
    }

    fn get_consensus(&self) -> Arc<Consensus> {
        self.consensus.clone()
    }
}

impl ConsensusFactory for SimulatorConsensusFactory {
    fn new_staging_consensus(&self) -> (kaspa_consensusmanager::ConsensusInstance, kaspa_consensusmanager::DynConsensusCtl) {
        let entry = self.management_store.write().new_staging_consensus_entry().unwrap();
        let dir = self.db.path().to_path_buf().join(entry.directory_name);
        let db = kaspa_database::prelude::ConnBuilder::default()
            .with_db_path(dir)
            .with_parallelism(num_cpus::get())
            .with_files_limit(10) // active and staging consensuses should have equal budgets
            .build()
            .unwrap();

        let session_lock = SessionLock::new();
        let consensus = Arc::new(Consensus::new(
            db.clone(),
            Arc::new(self.config.to_builder().skip_adding_genesis().build()),
            session_lock.clone(),
            self.consensus.notification_root().clone(),
            Default::default(),
            Default::default(),
            entry.creation_timestamp,
        ));
        // let dir = self.db.path().to_path_buf().join("tmp");
        // let db = kaspa_database::prelude::ConnBuilder::default()
        //     .with_db_path(dir)
        //     .with_files_limit(10) // active and staging consensuses should have equal budgets
        //     .build()
        //     .unwrap();

        // let session_lock = SessionLock::new();
        // let consensus = Arc::new(Consensus::new(
        //     db.clone(),
        //     Arc::new(self.config.to_builder().skip_adding_genesis().build()),
        //     session_lock.clone(),
        //     self.consensus.notification_root().clone(),
        //     Default::default(),
        //     Default::default(),
        //     unix_now(),
        // ));

        (ConsensusInstance::new(session_lock, consensus.clone()), Arc::new(Ctl::new(self.management_store.clone(), db, consensus)))
    }

    fn close(&self) {
        self.consensus.notification_root().close();
    }

    fn delete_inactive_consensus_entries(&self) {
        // Staging entry is deleted also by archival nodes since it represents non-final data
        self.delete_staging_entry();

        if self.config.is_archival {
            return;
        }

        let mut write_guard = self.management_store.write();
        let entries_to_delete = write_guard
            .iterate_inactive_entries()
            .filter_map(|entry_result| {
                let entry = entry_result.unwrap();
                let dir = self.db.path().to_path_buf().join(entry.directory_name.clone());
                if dir.exists() {
                    match fs::remove_dir_all(dir) {
                        Ok(_) => Some(entry),
                        Err(e) => {
                            warn!("Error deleting consensus entry {}: {}", entry.key, e);
                            None
                        }
                    }
                } else {
                    Some(entry)
                }
            })
            .collect_vec();

        for entry in entries_to_delete {
            write_guard.delete_entry(entry).unwrap();
        }
    }

    fn delete_staging_entry(&self) {
        let mut write_guard = self.management_store.write();
        if let Some(entry) = write_guard.staging_consensus_entry() {
            let dir = self.db.path().to_path_buf().join(entry.directory_name.clone());
            match fs::remove_dir_all(dir) {
                Ok(_) => {
                    write_guard.delete_entry(entry).unwrap();
                }
                Err(e) => {
                    warn!("Error deleting staging consensus entry {}: {}", entry.key, e);
                }
            };
            write_guard.cancel_staging_consensus().unwrap();
        }
    }

    fn new_active_consensus(&self) -> (ConsensusInstance, DynConsensusCtl) {
        let session_lock = SessionLock::new();

        (
            ConsensusInstance::new(session_lock, self.consensus.clone()),
            Arc::new(Ctl::new(self.management_store.clone(), self.db.clone(), self.consensus.clone())),
        )
    }
}

pub struct KaspaNetworkSimulator {
    // Internal simulation env
    pub(super) simulation: Simulation<Block>,

    // Consensus instances
    consensuses: Vec<ConsensusWrapper>,

    config: Arc<Config>,        // Consensus config
    bps: f64,                   // Blocks per second
    target_blocks: Option<u64>, // Target simulation blocks
    output_dir: Option<String>, // Possible permanent output directory
    add_peers: Vec<NetAddress>,
    runtime: Arc<Runtime>,
}

impl KaspaNetworkSimulator {
    pub fn new(
        delay: f64,
        bps: f64,
        target_blocks: Option<u64>,
        config: Arc<Config>,
        output_dir: Option<String>,
        add_peers: Vec<NetAddress>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            simulation: Simulation::with_start_time((delay * 1000.0) as u64, config.genesis.timestamp),
            consensuses: Vec::new(),
            bps,
            config,
            target_blocks,
            output_dir,
            add_peers,
            runtime,
        }
    }

    pub fn init(
        &mut self,
        num_miners: u64,
        target_txs_per_block: u64,
        rocksdb_stats: bool,
        rocksdb_stats_period_sec: Option<u32>,
        rocksdb_files_limit: Option<i32>,
        rocksdb_mem_budget: Option<usize>,
        wait_time: u64,
    ) -> &mut Self {
        let secp = secp256k1::Secp256k1::new();
        let mut rng = rand::thread_rng();
        for i in 0..num_miners {
            let mut builder = ConnBuilder::default().with_files_limit(fd_budget::limit() / 4 / num_miners as i32);
            if let Some(rocksdb_files_limit) = rocksdb_files_limit {
                builder = builder.with_files_limit(rocksdb_files_limit);
            }
            if let Some(rocksdb_mem_budget) = rocksdb_mem_budget {
                builder = builder.with_mem_budget(rocksdb_mem_budget);
            }
            let (lifetime, db) = match (i == 0, &self.output_dir, rocksdb_stats, rocksdb_stats_period_sec) {
                (true, Some(dir), true, Some(rocksdb_stats_period_sec)) => {
                    create_permanent_db!(dir, builder.enable_stats().with_stats_period(rocksdb_stats_period_sec))
                }
                (true, Some(dir), true, None) => create_permanent_db!(dir, builder.enable_stats()),
                (true, Some(dir), false, _) => create_permanent_db!(dir, builder),

                (_, _, true, Some(rocksdb_stats_period_sec)) => {
                    create_temp_db!(builder.enable_stats().with_stats_period(rocksdb_stats_period_sec))
                }
                (_, _, true, None) => create_temp_db!(builder.enable_stats()),
                (_, _, false, _) => create_temp_db!(builder),
            };

            // <P2P Setup>
            let meta_db = kaspa_database::prelude::ConnBuilder::default()
                .with_db_path(db.path().to_path_buf().join("meta"))
                .with_files_limit(5)
                .build()
                .unwrap();

            let tick_service = Arc::new(TickService::new());
            let mining_counters = Arc::new(MiningCounters::default());
            let mining_manager = MiningManagerProxy::new(Arc::new(MiningManager::new_with_extended_config(
                self.config.target_time_per_block,
                false,
                self.config.max_block_mass,
                self.config.ram_scale,
                self.config.block_template_cache_lifetime,
                mining_counters,
            )));
            let (address_manager, _) = AddressManager::new(self.config.clone(), meta_db.clone(), tick_service.clone());
            let (dummy_notification_sender, _) = unbounded();
            let notification_root = Arc::new(ConsensusNotificationRoot::new(dummy_notification_sender));

            let consensus_factory =
                Arc::new(SimulatorConsensusFactory::new(notification_root.clone(), meta_db, db.clone(), self.config.clone()));
            let consensus_manager = Arc::new(ConsensusManager::new(consensus_factory.clone()));

            let flow_context = Arc::new(FlowContext::new(
                consensus_manager.clone(),
                address_manager,
                self.config.clone(),
                mining_manager.clone(),
                tick_service.clone(),
                notification_root.clone(),
            ));

            let p2p_service: Arc<dyn AsyncService> = Arc::new(P2pService::new(
                flow_context.clone(),
                vec![],
                self.add_peers.clone(),
                self.config.p2p_listen_address.normalize(self.config.default_p2p_port()).into(),
                8,
                8,
                Default::default(),
                self.config.default_p2p_port(),
                Default::default(),
            ));

            let mut services: Vec<Arc<dyn AsyncService>> = vec![];
            services.push(tick_service.clone());
            services.push(p2p_service.clone());

            self.runtime.spawn(async { tick_service.start().await });
            self.runtime.spawn(async { p2p_service.start().await });
            // </P2P Setup>

            let consensus = consensus_factory.get_consensus();

            let handles = consensus.run_processors();
            let (sk, pk) = secp.generate_keypair(&mut rng);
            let miner_process = Box::new(Miner::new(
                i,
                self.bps,
                1f64 / num_miners as f64,
                sk,
                pk,
                consensus.clone(),
                &self.config,
                target_txs_per_block,
                self.target_blocks,
                self.runtime.clone(),
                flow_context.hub().clone(),
                wait_time,
            ));
            self.simulation.register(i, miner_process);
            self.consensuses.push((consensus, handles, lifetime, services));
        }

        if num_miners == 0 {
            let mut builder = ConnBuilder::default().with_files_limit(fd_budget::limit() / 4 as i32);
            if let Some(rocksdb_files_limit) = rocksdb_files_limit {
                builder = builder.with_files_limit(rocksdb_files_limit);
            }
            if let Some(rocksdb_mem_budget) = rocksdb_mem_budget {
                builder = builder.with_mem_budget(rocksdb_mem_budget);
            }
            let (lifetime, db) = match (true, &self.output_dir, rocksdb_stats, rocksdb_stats_period_sec) {
                (true, Some(dir), true, Some(rocksdb_stats_period_sec)) => {
                    create_permanent_db!(dir, builder.enable_stats().with_stats_period(rocksdb_stats_period_sec))
                }
                (true, Some(dir), true, None) => create_permanent_db!(dir, builder.enable_stats()),
                (true, Some(dir), false, _) => create_permanent_db!(dir, builder),

                (_, _, true, Some(rocksdb_stats_period_sec)) => {
                    create_temp_db!(builder.enable_stats().with_stats_period(rocksdb_stats_period_sec))
                }
                (_, _, true, None) => create_temp_db!(builder.enable_stats()),
                (_, _, false, _) => create_temp_db!(builder),
            };

            // <P2P Setup>
            let meta_db = kaspa_database::prelude::ConnBuilder::default()
                .with_db_path(db.path().to_path_buf().join("meta"))
                .with_files_limit(5)
                .build()
                .unwrap();

            let tick_service = Arc::new(TickService::new());
            let mining_counters = Arc::new(MiningCounters::default());
            let mining_manager = MiningManagerProxy::new(Arc::new(MiningManager::new_with_extended_config(
                self.config.target_time_per_block,
                false,
                self.config.max_block_mass,
                self.config.ram_scale,
                self.config.block_template_cache_lifetime,
                mining_counters,
            )));
            let (address_manager, _) = AddressManager::new(self.config.clone(), meta_db.clone(), tick_service.clone());
            let (dummy_notification_sender, _) = unbounded();
            let notification_root = Arc::new(ConsensusNotificationRoot::new(dummy_notification_sender));

            let consensus_factory =
                Arc::new(SimulatorConsensusFactory::new(notification_root.clone(), meta_db, db.clone(), self.config.clone()));
            let consensus_manager = Arc::new(ConsensusManager::new(consensus_factory.clone()));

            let flow_context = Arc::new(FlowContext::new(
                consensus_manager.clone(),
                address_manager,
                self.config.clone(),
                mining_manager.clone(),
                tick_service.clone(),
                notification_root.clone(),
            ));

            let p2p_service: Arc<dyn AsyncService> = Arc::new(P2pService::new(
                flow_context.clone(),
                vec![],
                self.add_peers.clone(),
                self.config.p2p_listen_address.normalize(self.config.default_p2p_port()).into(),
                8,
                8,
                Default::default(),
                self.config.default_p2p_port(),
                Default::default(),
            ));

            let mut services: Vec<Arc<dyn AsyncService>> = vec![];
            services.push(tick_service.clone());
            services.push(p2p_service.clone());

            self.runtime.spawn(async { tick_service.start().await });
            self.runtime.spawn(async { p2p_service.start().await });
            // </P2P Setup>

            let consensus = consensus_factory.get_consensus();

            let handles = consensus.run_processors();
            self.consensuses.push((consensus, handles, lifetime, services));

            self.simulation.register(0, Box::new(Idler::new()));
        }
        self
    }

    pub fn run(&mut self, until: u64) -> ConsensusWrapper {
        self.simulation.run(until);
        for (consensus, handles, _, services) in self.consensuses.drain(1..) {
            for service in services {
                info!("Signal exit: {:#?}", service.clone().ident());
                service.signal_exit();
            }

            consensus.shutdown(handles);
        }
        self.consensuses.pop().unwrap()
    }
}
