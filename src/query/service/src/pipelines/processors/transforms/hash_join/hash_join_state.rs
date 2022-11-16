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
use common_expression::Chunk;

use super::ProbeState;

#[async_trait::async_trait]
/// Concurrent hash table for hash join.
pub trait HashJoinState: Send + Sync {
    /// Build hash table with input Chunk
    fn build(&self, input: Chunk) -> Result<()>;

    /// Probe the hash table and retrieve matched rows as Chunks
    fn probe(&self, input: &Chunk, probe_state: &mut ProbeState) -> Result<Vec<Chunk>>;

    fn interrupt(&self);

    /// Attach to state
    fn attach(&self) -> Result<()>;

    /// Detach to state
    fn detach(&self) -> Result<()>;

    /// Is building finished.
    fn is_finished(&self) -> Result<bool>;

    /// Finish building hash table, will be called only once as soon as all handles
    /// have been detached from current state.
    fn finish(&self) -> Result<()>;

    /// Wait until the build phase is finished
    async fn wait_finish(&self) -> Result<()>;

    /// Get mark join results
    fn mark_join_chunks(&self) -> Result<Vec<Chunk>>;

    /// Get right join results
    fn right_join_chunks(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>>;

    /// Get right semi/anti join results
    fn right_semi_join_chunks(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>>;

    /// Get left join results
    fn left_join_chunks(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>>;
}
