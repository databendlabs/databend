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

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_storage::DataOperator;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spiller::Spiller;
use crate::spiller::SpillerConfig;

/// Define some states for hash join build spilling
pub struct BuildSpillState {
    /// Spilling memory threshold
    pub(crate) spill_memory_threshold: usize,
    /// Hash join build spilling coordinator
    pub(crate) spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub(crate) spiller: Spiller,
    /// DataBlocks need to be spilled for the processor
    pub(crate) input_data: RwLock<Vec<DataBlock>>,
}

impl BuildSpillState {
    pub fn create(_ctx: Arc<QueryContext>, spill_coordinator: Arc<BuildSpillCoordinator>) -> Self {
        let spill_config = SpillerConfig::create("hash_join_build_spill".to_string());
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(operator, spill_config);
        Self {
            spill_memory_threshold: 1024,
            spill_coordinator,
            spiller,
            input_data: Default::default(),
        }
    }
}

/// Define some spill-related APIs for hash join build
impl HashJoinBuildState {
    // Start to spill `input_data`.
    pub(crate) fn spill(&self) -> Result<()> {
        todo!()
    }

    // Check if need to spill.
    // Notes: even if input can fit into memory, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self, input: &DataBlock) -> Result<bool> {
        todo!()
    }

    // Spill last build processors' input data
    pub(crate) fn spill_input(&self, data_block: DataBlock) -> Result<()> {
        todo!()
    }

    // Add data that will be spilled to `input_data`
    pub(crate) fn add_unspilled_data(&self, input: DataBlock) {
        let mut input_data = self.spill_state.input_data.write();
        input_data.push(input)
    }
}
