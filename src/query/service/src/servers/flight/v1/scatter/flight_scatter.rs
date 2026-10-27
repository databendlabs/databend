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

use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

pub trait FlightScatter: Sync + Send {
    fn name(&self) -> &'static str;

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>>;

    /// Compute per-row partition indices for use with BlockPartitionStream.
    /// Returns None if this scatter type doesn't support index-based partitioning
    /// (e.g., broadcast scatter clones the entire block to all destinations,
    /// and a producer may route blocks holding their data in the meta); such
    /// blocks are routed whole by `execute`.
    fn scatter_indices(&self, _data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        Ok(None)
    }
}

/// Builds the scatter of a global hash shuffle once its partitions are known,
/// for producers that route their blocks themselves. `nodes[i]` holds the
/// partitions of the i-th destination node, in order. A partition is a thread
/// of its node, which receives only that partition, so the scatter of every
/// sender must route the same group of rows to the same partition.
pub type GlobalScatterBuilder =
    Arc<dyn Fn(&[Range<usize>]) -> Result<Box<dyn FlightScatter>> + Send + Sync>;
