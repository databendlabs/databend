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

use common_exception::Result;
use common_expression::DataBlock;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::JoinState;

#[async_trait::async_trait]
/// Concurrent hash table for hash join.
pub trait HashJoinState: Send + Sync {
    /// Add input `DataBlock` to `row_space`.
    fn build(&self, input: DataBlock) -> Result<()>;

    /// Probe the hash table and retrieve matched rows as DataBlocks.
    fn probe(&self, input: &DataBlock, probe_state: &mut ProbeState) -> Result<Vec<DataBlock>>;

    fn interrupt(&self);

    fn join_state(&self) -> &JoinState;

    /// Attach to state: `build_count` and `finalize_count`
    fn attach(&self) -> Result<()>;

    /// Detach to state: `build_count`, create finalize task and initialize the hash table.
    fn build_end(&self) -> Result<()>;

    /// Divide the finalize phase into multiple tasks.
    fn divide_finalize_task(&self) -> Result<()>;

    /// Get the finalize task and using the `chunks` in `row_space` to build hash table in parallel.
    fn finalize(&self) -> Result<bool>;

    /// Detach to state: `finalize_count`.
    fn finalize_end(&self) -> Result<()>;

    /// Wait until the build phase is finished.
    async fn wait_build_finish(&self) -> Result<()>;

    /// Wait until the finalize phase is finished.
    async fn wait_finalize_finish(&self) -> Result<()>;

    /// Get mark join results.
    fn mark_join_blocks(&self) -> Result<Vec<DataBlock>>;

    /// Get right join results.
    fn right_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;

    /// Get right semi/anti join results.
    fn right_semi_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;

    /// Get left join results.
    fn left_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>>;
}
