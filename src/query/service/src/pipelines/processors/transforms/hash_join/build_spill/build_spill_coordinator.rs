// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use parking_lot::RwLock;

/// Coordinate all hash join build processors to spill.
/// It's shared by all hash join build processors.
/// When hash join build needs to spill, all processor will stop executing and prepare to spill.
/// The last one will be as the coordinator to spill all processors and then wake up all processors to continue executing.
pub struct BuildSpillCoordinator {
    /// Need to spill, if one of the builders need to spill, this flag will be set to true.
    need_spill: bool,
    /// Current waiting spilling processor count.
    pub(crate) waiting_spill_count: usize,
    /// Total processor count.
    pub(crate) total_builder_count: usize,
    /// Spill tasks, the size is the same as the total active processor count.
    pub(crate) spill_tasks: VecDeque<Vec<(u8, DataBlock)>>,
    /// If send partition set to probe
    pub send_partition_set: bool,
    /// When a build processor won't trigger spill, the field will plus one
    pub non_spill_processors: usize,
}

impl BuildSpillCoordinator {
    pub fn create(total_builder_count: usize) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            need_spill: false,
            waiting_spill_count: 0,
            total_builder_count,
            spill_tasks: Default::default(),
            send_partition_set: Default::default(),
            non_spill_processors: Default::default(),
        }))
    }

    // Called by hash join build processor, if current processor need to spill, then set `need_spill` to true.
    pub fn need_spill(&mut self) -> Result<()> {
        self.need_spill = true;
        Ok(())
    }

    // If current waiting spilling builder is the last one, then spill all builders.
    pub(crate) fn wait_spill(&mut self) -> Result<bool> {
        let non_spill_processors = self.non_spill_processors;
        let waiting_spill_count = &mut self.waiting_spill_count;
        *waiting_spill_count += 1;
        if *waiting_spill_count == (self.total_builder_count - non_spill_processors) {
            // Reset waiting_spill_count
            *waiting_spill_count = 0;
            // No need to wait spill, the processor is the last one
            return Ok(false);
        }
        Ok(true)
    }

    // Get the need_spill flag.
    pub fn get_need_spill(&self) -> bool {
        self.need_spill
    }

    // Set the need_spill flag to false.
    pub fn no_need_spill(&mut self) {
        self.need_spill = false;
    }
}
