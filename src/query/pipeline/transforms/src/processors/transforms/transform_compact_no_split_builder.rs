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
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;

use crate::processors::AccumulatingTransform;
use crate::processors::BlockCompactMeta;
use crate::processors::TransformCompactBlock;
use crate::processors::TransformPipelineHelper;

pub fn build_compact_block_no_split_pipeline(
    pipeline: &mut Pipeline,
    thresholds: BlockThresholds,
    max_threads: usize,
) -> Result<()> {
    pipeline.try_resize(1)?;
    pipeline.add_accumulating_transformer(|| BlockCompactNoSplitBuilder::new(thresholds));
    pipeline.try_resize(max_threads)?;
    pipeline.add_block_meta_transformer(TransformCompactBlock::default);
    Ok(())
}

pub struct BlockCompactNoSplitBuilder {
    thresholds: BlockThresholds,
    // Holds blocks that exceeded the threshold and are waiting to be compacted.
    staged_blocks: Vec<DataBlock>,
    // Holds blocks that are partially accumulated but haven't reached the threshold.
    pending_blocks: Vec<DataBlock>,
    accumulated_rows: usize,
    accumulated_bytes: usize,
}

impl BlockCompactNoSplitBuilder {
    pub fn new(thresholds: BlockThresholds) -> Self {
        Self {
            thresholds,
            staged_blocks: vec![],
            pending_blocks: vec![],
            accumulated_rows: 0,
            accumulated_bytes: 0,
        }
    }

    fn create_output_data(blocks: &mut Vec<DataBlock>) -> DataBlock {
        if blocks.len() > 1 {
            DataBlock::empty_with_meta(Box::new(BlockCompactMeta::Concat(std::mem::take(blocks))))
        } else {
            DataBlock::empty_with_meta(Box::new(BlockCompactMeta::NoChange(std::mem::take(blocks))))
        }
    }

    fn reset_accumulated(&mut self) {
        self.accumulated_rows = 0;
        self.accumulated_bytes = 0;
    }

    fn check_for_compact(&self) -> bool {
        self.thresholds
            .check_for_compact(self.accumulated_rows, self.accumulated_bytes)
    }
}

impl AccumulatingTransform for BlockCompactNoSplitBuilder {
    const NAME: &'static str = "BlockCompactNoSplitBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        self.accumulated_rows += data.num_rows();
        self.accumulated_bytes += crate::processors::memory_size(&data);
        if !self
            .thresholds
            .check_large_enough(self.accumulated_rows, self.accumulated_bytes)
        {
            // blocks < N
            self.pending_blocks.push(data);
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(2);
        if !self.staged_blocks.is_empty() {
            res.push(Self::create_output_data(&mut self.staged_blocks));
        }

        if self.pending_blocks.is_empty() || self.check_for_compact() {
            // N <= blocks < 2N
            std::mem::swap(&mut self.staged_blocks, &mut self.pending_blocks);
        } else {
            // blocks >= 2N
            res.push(Self::create_output_data(&mut self.pending_blocks));
        }
        self.staged_blocks.push(data);
        self.reset_accumulated();
        Ok(res)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        self.staged_blocks.append(&mut self.pending_blocks);
        if self.staged_blocks.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![Self::create_output_data(&mut self.staged_blocks)])
        }
    }
}
