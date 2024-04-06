use std::{thread::sleep, time::Duration};

use kaspa_consensus_core::block::Block;
use kaspa_utils::sim::{Environment, Process, Resumption, Suspension};

pub struct Idler {}

impl Idler {
    pub fn new() -> Self {
        Self {}
    }
}

impl Process<Block> for Idler {
    fn resume(&mut self, _resumption: Resumption<Block>, _env: &mut Environment<Block>) -> Suspension {
        sleep(Duration::from_secs(1));
        Suspension::Timeout(1000)
    }
}
