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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_exception::Result;

use super::Compactor;
use super::TransformCompact;
use crate::processors::transforms::Aborting;

pub struct SortMergeCompactor {
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    aborting: Arc<AtomicBool>,
}

impl SortMergeCompactor {
    pub fn new(
        block_size: usize,
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Self {
        SortMergeCompactor {
            block_size,
            limit,
            sort_columns_descriptions,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for SortMergeCompactor {
    fn name() -> &'static str {
        "SortMergeTransform"
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_final(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            Ok(vec![])
        } else {
            let aborting = self.aborting.clone();
            let aborting: Aborting = Arc::new(Box::new(move || aborting.load(Ordering::Relaxed)));

            let block = DataBlock::merge_sort_blocks(
                blocks,
                &self.sort_columns_descriptions,
                self.limit,
                aborting,
            )?;
            // split block by `self.block_size`
            let num_rows = block.num_rows();
            let num_blocks =
                num_rows / self.block_size + usize::from(num_rows % self.block_size > 0);
            let mut start = 0;
            let mut output = Vec::with_capacity(num_blocks);
            for _ in 0..num_blocks {
                let end = std::cmp::min(start + self.block_size, num_rows);
                let block = DataBlock::block_take_by_slices_limit(
                    &block,
                    (start, end - start),
                    self.limit,
                )?;
                start = end;
                output.push(block);
            }
            Ok(output)
        }
    }
}

pub type TransformSortMerge = TransformCompact<SortMergeCompactor>;
