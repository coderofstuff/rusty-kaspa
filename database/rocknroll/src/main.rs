#![allow(dead_code)]
#![allow(unused_imports)]

use chrono::{TimeZone, Utc};
use std::{
    mem::size_of,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use kaspa_consensus::{
    consensus::{services::ConsensusServices, storage::ConsensusStorage},
    model::stores::{acceptance_data::AcceptanceDataStoreReader, headers::HeaderStoreReader, pruning::PruningStoreReader},
};
use kaspa_consensus_core::{
    config::ConfigBuilder,
    network::{NetworkId, NetworkType},
    tx::{ScriptVec, TransactionOutpoint, UtxoEntry},
};
use kaspa_core::info;
use kaspad_lib::daemon::{get_app_dir, CONSENSUS_DB, DEFAULT_DATA_DIR, META_DB, UTXOINDEX_DB};

fn main() {
    kaspa_core::log::init_logger(None, "");
    let network = NetworkId::new(NetworkType::Mainnet);
    let app_dir = get_app_dir();
    let db_dir = app_dir.join(network.to_prefixed()).join(DEFAULT_DATA_DIR);
    let consensus_db_dir = db_dir.join(CONSENSUS_DB).join("consensus-003"); // check your own index
                                                                            // let utxoindex_db_dir = db_dir.join(UTXOINDEX_DB);
                                                                            // let meta_db_dir = db_dir.join(META_DB);

    let config = Arc::new(ConfigBuilder::new(network.into()).adjust_perf_params_to_consensus_params().build());
    let db =
        kaspa_database::prelude::ConnBuilder::default().with_db_path(consensus_db_dir).with_files_limit(128).build_readonly().unwrap();

    let storage = ConsensusStorage::new(db.clone(), config.clone());
    let services = ConsensusServices::new(db, storage.clone(), config, Default::default(), Default::default());

    let start_datetime = Utc.with_ymd_and_hms(2025, 10, 5, 0, 0, 0).unwrap();
    let end_datetime = Utc.with_ymd_and_hms(2025, 10, 6, 0, 0, 0).unwrap();
    let start: SystemTime = start_datetime.into();
    let end: SystemTime = end_datetime.into();

    let pp = storage.pruning_point_store.read().pruning_point().unwrap();
    let sink = storage.lkg_virtual_state.load().ghostdag_data.selected_parent;

    let (start, end) =
        (start.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64, end.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64);
    let mut count = 0;

    for cb in services.reachability_service.forward_chain_iterator(pp, sink, false) {
        let timestamp = storage.headers_store.get_timestamp(cb).unwrap();
        if start <= timestamp && timestamp < end {
            let ad = storage.acceptance_data_store.get(cb).unwrap();
            let mergeset_accepted_txs_count = ad.iter().map(|d| d.accepted_transactions.len()).sum::<usize>();
            count += mergeset_accepted_txs_count;
            if (count - mergeset_accepted_txs_count) / 10_000_000 != count / 10_000_000 {
                info!("Accepted txs in range: {}", count);
            }
        }
    }
    info!(
        "\n=======================================\n\tAccepted txs in range {} - {}: {}\n=======================================",
        start_datetime.format("%d/%m/%Y %H:%M"), end_datetime.format("%d/%m/%Y %H:%M"), count
    );
}
