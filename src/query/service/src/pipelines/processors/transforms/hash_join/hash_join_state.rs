// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_expression::DataBlock;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::JoinState;

#[async_trait::async_trait]
/// Concurrent hash table for hash join.
pub trait HashJoinState: Send + Sync {
    /// Build hash table with input DataBlock
    fn build(&self, input: DataBlock) -> Result<()>;

    /// Probe the hash table and retrieve matched rows as DataBlocks
    fn probe(&self, input: &DataBlock, probe_state: &mut ProbeState) -> Result<Vec<DataBlock>>;

    fn interrupt(&self);

    fn join_state(&self) -> &JoinState;

    /// Attach to state
    fn attach(&self) -> Result<()>;

    /// Get mark join results
    fn mark_join_blocks(&self) -> Result<Vec<DataBlock>>;

    /// Get right join results
    fn right_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;

    /// Get right semi/anti join results
    fn right_semi_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;

    /// Get left join results
    fn left_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;

    /// TODO(dousir9)
    fn build_end(&self) -> Result<()>;

    /// TODO(dousir9)
    fn finalize_end(&self) -> Result<()>;

    /// TODO(dousir9)
    fn finalize(&self) -> Result<bool>;

    /// TODO(dousir9)
    fn divide_finalize_task(&self) -> Result<()>;

    /// TODO(dousir9)
    fn set_max_threads(&self, max_threads: usize) -> Result<()>;

    /// TODO(dousir9)
    async fn wait_build_finish(&self) -> Result<()>;

    /// TODO(dousir9)
    async fn wait_finalize_finish(&self) -> Result<()>;
}
