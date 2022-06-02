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

use common_datablocks::DataBlock;
use common_exception::Result;

/// Concurrent hash table for hash join.
pub trait HashJoinState: Send + Sync {
    /// Build hash table with input DataBlock
    fn build(&self, input: DataBlock) -> Result<()>;

    /// Probe the hash table and retrieve matched rows as DataBlocks
    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>>;

    /// Attach to state
    fn attach(&self) -> Result<()>;

    /// Detach to state
    fn detach(&self) -> Result<()>;

    /// Is building finished.
    fn is_finished(&self) -> Result<bool>;

    /// Finish building hash table, will be called only once as soon as all handles
    /// have been detached from current state.
    fn finish(&self) -> Result<()>;
}
