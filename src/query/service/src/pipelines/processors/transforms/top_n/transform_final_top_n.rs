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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::sorts::core::Rows;

use super::TopNCandidates;
use super::split_block;

/// The final stage of TopN.
///
/// It consumes internally sorted candidate blocks (in arbitrary interleaving
/// across streams and nodes), keeps the best `limit + offset` rows, and at
/// the end of input applies the offset, truncates to `limit` rows and strips
/// the appended order column. Its input volume is bounded by the upstream
/// partial stages, so no spilling is needed here.
pub struct TransformFinalTopN<R: Rows> {
    candidates: TopNCandidates<R>,
    limit: usize,
    offset: usize,
    /// Remove the order column from the output. `false` when the order column
    /// is a source column of the plan output.
    remove_order_col: bool,
    sort_row_offset: usize,
    max_block_size: usize,
}

impl<R: Rows> TransformFinalTopN<R> {
    pub fn new(
        limit: usize,
        offset: usize,
        remove_order_col: bool,
        sort_row_offset: usize,
        max_block_size: usize,
    ) -> Self {
        let capacity = limit.saturating_add(offset);
        Self {
            candidates: TopNCandidates::new(capacity, sort_row_offset),
            limit,
            offset,
            remove_order_col,
            sort_row_offset,
            max_block_size,
        }
    }
}

impl<R: Rows> AccumulatingTransform for TransformFinalTopN<R> {
    const NAME: &'static str = "TransformFinalTopN";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if block.is_empty() {
            return Ok(vec![]);
        }

        let rows = R::from_column(&block.get_by_offset(self.sort_row_offset).to_column())?;
        self.candidates.sift_sorted(block, rows)?;
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            return Ok(vec![]);
        }

        let Some((block, _)) = self.candidates.finish() else {
            return Ok(vec![]);
        };

        let num_rows = block.num_rows();
        if self.offset >= num_rows || self.limit == 0 {
            return Ok(vec![]);
        }

        let end = num_rows.min(self.offset.saturating_add(self.limit));
        let mut block = block.slice(self.offset..end);
        if self.remove_order_col {
            block.remove_column(self.sort_row_offset);
        }

        Ok(split_block(block, self.max_block_size))
    }
}
