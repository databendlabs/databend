// Copyright 2021 Datafuse Labs.
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
//

use common_datablocks::DataBlock;
use common_exception::Result;
use common_planners::PlanNode;

use crate::Index;
use crate::MinMaxIndex;
use crate::SparseIndex;

pub struct Indexer {}

impl Indexer {
    pub fn create() -> Self {
        Indexer {}
    }
}

/// Create index for one parquet file.
/// All blocks are belong to a Parquet file and globally sorted.
/// Each block is one row group of a parquet file and sorted by primary key.
/// For example:
/// parquet.file
/// | sorted-block | sorted-block | ... |
impl Index for Indexer {
    fn create_min_max_idx(
        &self,
        keys: &[String],
        blocks: &[DataBlock],
    ) -> Result<Vec<MinMaxIndex>> {
        MinMaxIndex::create_index(keys, blocks)
    }

    fn create_sparse_idx(
        &self,
        keys: &[String],
        blocks: &[DataBlock],
    ) -> common_exception::Result<Vec<SparseIndex>> {
        SparseIndex::create_index(keys, blocks)
    }

    // Search parts by plan.
    fn search_index(&self, _plan: &PlanNode) -> common_exception::Result<()> {
        todo!()
    }
}
