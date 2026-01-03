#![allow(dead_code)]
#![allow(unused_imports)]

use chrono::{TimeZone, Utc};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    mem::size_of,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{CommandFactory, Parser};

use kaspa_consensus::{
    consensus::{services::ConsensusServices, storage::ConsensusStorage},
    model::stores::{
        acceptance_data::AcceptanceDataStoreReader, ghostdag::GhostdagStoreReader, headers::HeaderStoreReader,
        pruning::PruningStoreReader, relations::RelationsStoreReader,
    },
    params::FINALITY_DURATION,
    processes::ghostdag::ordering::SortableBlock,
};
use kaspa_consensus_core::{
    config::ConfigBuilder,
    network::{NetworkId, NetworkType},
    tx::{ScriptVec, TransactionOutpoint, UtxoEntry},
    BlockHashSet,
};
use kaspa_core::info;
use kaspad_lib::daemon::{get_app_dir, CONSENSUS_DB, DEFAULT_DATA_DIR, META_DB, UTXOINDEX_DB};

#[derive(Debug, Parser)]
#[command(about = "Kaspa Rocks DB exploration tool CLI")]
struct Args {
    /// Full path to the consensus directory
    /// (e.g. /path/to/folder/kaspa-mainnet/datadir/consensus/consensus-002)
    #[arg(value_name = "consensusdir")]
    consensusdir: Option<PathBuf>,
    /// Run RocksDB compaction after printing info
    #[arg(long = "compact", help = "Run RocksDB compaction after printing info")]
    compact: bool,
    /// Verify consistency instead of or in addition to other actions
    #[arg(long = "verify", help = "Verify consensus DB consistency (default: false)")]
    verify: bool,
}

fn main() {
    kaspa_core::log::init_logger(None, "");
    let network = NetworkId::new(NetworkType::Mainnet);
    let args = Args::parse();

    // If no consensus directory was provided, print usage and exit.
    let consensus_db_dir = match args.consensusdir {
        Some(p) => p,
        None => {
            let mut cmd = Args::command();
            cmd.print_help().ok();
            println!();
            std::process::exit(1);
        }
    };
    // let utxoindex_db_dir = db_dir.join(UTXOINDEX_DB);
    // let meta_db_dir = db_dir.join(META_DB);

    let config = Arc::new(ConfigBuilder::new(network.into()).adjust_perf_params_to_consensus_params().build());
    let db =
        kaspa_database::prelude::ConnBuilder::default().with_db_path(consensus_db_dir).with_files_limit(128).build_readonly().unwrap();

    let storage = ConsensusStorage::new(db.clone(), config.clone());

    let sink = storage.lkg_virtual_state.load().ghostdag_data.selected_parent;
    let sink_timestamp = storage.headers_store.get_timestamp(sink).unwrap();

    let pruning_read = storage.pruning_point_store.read();
    let pp = pruning_read.pruning_point().unwrap();
    let pp_timestamp = storage.headers_store.get_timestamp(pp).unwrap();

    let rr = pruning_read.retention_period_root().unwrap();
    let rr_timestamp = storage.headers_store.get_timestamp(rr).unwrap();

    info!("Pruning Point\t | {} | {}", pp, pp_timestamp);
    info!("Retention Root\t | {} | {}", rr, rr_timestamp);
    info!("Sink\t\t | {} | {}", sink, sink_timestamp);

    if args.verify {
        let services = ConsensusServices::new(db.clone(), storage.clone(), config, Default::default(), Default::default());
        let mut found = false;

        // let mut heap = BinaryHeap::<Reverse<SortableBlock>>::new();
        // let mut visited = BlockHashSet::default();
        let mut queue = VecDeque::new();
        queue.push_back(sink);

        let mut seen: u64 = 0;
        let mut last_seen: u64 = 0;

        let est_blocks = storage.headers_store.get_blue_score(sink).unwrap() - storage.headers_store.get_blue_score(rr).unwrap();
        info!("Verifying retention root presence in sink's past, estimated {} blocks to go through...", est_blocks);

        for chain_block in services.reachability_service.backward_chain_iterator(sink, rr, true) {
            let current = chain_block;

            // let msb = storage.ghostdag_store.get_mergeset_blues(current).unwrap();
            // seen += msb.len() as u64 + 1; // +1 for the current block
            seen += 1;

            if seen / 10000 > last_seen {
                last_seen = seen / 10000;
                info!("Seen {} chain blocks so far... | total blocks: {}", seen, est_blocks);
            }

            if current == rr {
                found = true;
                break;
            }
        }
        // while !found && !queue.is_empty() {
        //     let current = queue.pop_front().unwrap();
        //     if visited.contains(&current) {
        //         continue;
        //     }
        //     visited.insert(current);
        //     seen += 1;

        //     if seen % 1000 == 0 {
        //         info!("Seen {} / ~{} blocks so far...", seen, est_blocks);
        //     }

        //     let bw = storage.ghostdag_store.get_blue_work(current).unwrap();
        //     heap.push(Reverse(SortableBlock { hash: current, blue_work: bw }));

        //     if current == rr {
        //         found = true;
        //         break;
        //     }

        //     for &parent in services.relations_service.get_parents(current).unwrap().iter() {
        //         queue.push_back(parent);
        //     }

        //     while heap.len() > (10 * FINALITY_DURATION as usize) {
        //         visited.remove(&heap.pop().unwrap().0.hash);
        //     }
        // }

        if found {
            info!("Retention root found in chain from sink.");
        } else {
            info!("Retention root NOT found in chain from sink!");
        }
    }

    if args.compact {
        db.compact_range::<&[u8], &[u8]>(None, None);
    }
}
