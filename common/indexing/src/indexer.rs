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
use crate::SparseIndexValue;

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
        let first = 0;
        let last = blocks.len() - 1;
        let mut keys_idx = vec![];

        for key in keys {
            let min = blocks[first].first(key)?;
            let max = blocks[last].last(key)?;
            let min_max = MinMaxIndex::create(key.clone(), min, max);
            keys_idx.push(min_max);
        }
        Ok(keys_idx)
    }

    fn create_sparse_idx(
        &self,
        keys: &[String],
        blocks: &[DataBlock],
    ) -> common_exception::Result<Vec<SparseIndex>> {
        let mut keys_idx = vec![];

        for key in keys {
            let mut sparse = SparseIndex::create(key.clone());
            for (page_no, page) in blocks.iter().enumerate() {
                let min = page.first(key.as_str())?;
                let max = page.last(key.as_str())?;
                sparse.push(SparseIndexValue {
                    min,
                    max,
                    page_no: Some(page_no as i64),
                })?;
            }
            keys_idx.push(sparse);
        }
        Ok(keys_idx)
    }

    // Search parts by plan.
    fn search_index(&self, _plan: &PlanNode) -> common_exception::Result<()> {
        todo!()
    }
}
