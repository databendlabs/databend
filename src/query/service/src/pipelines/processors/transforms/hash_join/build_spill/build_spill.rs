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

use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::spiller::Spiller;

/// Define some states for hash join build spilling
struct BuildSpillState {
    /// Spilling memory threshold
    spill_memory_threshold: usize,
    /// Hash join build spilling coordinator
    spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    spiller: Spiller,
}

/// Define some spill-related APIs for hash join build
impl HashJoinBuildState {
    // Start to spill.
    async fn spill(&mut self) -> Result<()> {
        todo!()
    }

    // Check if need to spill.
    // Notes: even if input can fit into memory, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self, input: &DataBlock) -> Result<bool> {
        todo!()
    }
}
