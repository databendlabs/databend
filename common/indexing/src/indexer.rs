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
use common_planners::PlanNode;

use crate::Index;
use crate::IndexSchema;
use crate::IndexSchemaVersion;
use crate::MinMaxIndex;
use crate::SparseIndex;
use crate::SparseIndexValue;

pub struct Indexer {}

impl Indexer {
    pub fn create() -> Self {
        Indexer {}
    }
}

impl Index for Indexer {
    /// Create index for one parquet file.
    /// All blocks are belong to a Parquet file and globally sorted.
    /// Each block is one row group of a parquet file and sorted by primary key.
    /// For example:
    /// parquet.file
    /// | sorted-block | sorted-block | ... |
    fn create_index(
        &self,
        keys: &[String],
        pages: &[DataBlock],
    ) -> common_exception::Result<Vec<IndexSchema>> {
        let first = 0;
        let last = pages.len() - 1;

        let mut idxes = vec![];
        for key in keys {
            // Min and max index.
            let min = pages[first].first(key)?;
            let max = pages[last].last(key)?;
            let min_max = MinMaxIndex::create(min, max);

            // Sparse index by the page.
            let mut sparse = SparseIndex::create();
            for (page_no, page) in pages.iter().enumerate() {
                let min = page.first(key)?;
                let max = page.last(key)?;
                sparse.push(SparseIndexValue {
                    min,
                    max,
                    page_no: Some(page_no as i64),
                })?;
            }
            idxes.push(IndexSchema {
                col: key.to_string(),
                min_max,
                sparse,
                version: IndexSchemaVersion::V1,
            });
        }
        Ok(idxes)
    }

    // Search parts by plan.
    fn search_index(&self, _plan: &PlanNode) -> common_exception::Result<()> {
        todo!()
    }
}
