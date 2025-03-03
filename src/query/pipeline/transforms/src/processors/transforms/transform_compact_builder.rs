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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::Pipeline;

use crate::processors::AccumulatingTransform;
use crate::processors::BlockCompactMeta;
use crate::processors::TransformCompactBlock;
use crate::processors::TransformPipelineHelper;

pub fn build_compact_block_pipeline(
    pipeline: &mut Pipeline,
    thresholds: BlockThresholds,
) -> Result<()> {
    let output_len = pipeline.output_len();
    pipeline.try_resize(1)?;
    pipeline.add_accumulating_transformer(|| BlockCompactBuilder::new(thresholds));
    pipeline.try_resize(output_len)?;
    pipeline.add_block_meta_transformer(TransformCompactBlock::default);
    Ok(())
}

pub struct BlockCompactBuilder {
    thresholds: BlockThresholds,
    // Holds blocks that are partially accumulated but haven't reached the threshold.
    pending_blocks: Vec<DataBlock>,
    accumulated_rows: usize,
    accumulated_bytes: usize,
}

impl BlockCompactBuilder {
    pub fn new(thresholds: BlockThresholds) -> Self {
        BlockCompactBuilder {
            thresholds,
            pending_blocks: vec![],
            accumulated_rows: 0,
            accumulated_bytes: 0,
        }
    }

    fn reset_accumulated(&mut self) {
        self.accumulated_rows = 0;
        self.accumulated_bytes = 0;
    }
}

impl AccumulatingTransform for BlockCompactBuilder {
    const NAME: &'static str = "BlockCompactBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let num_rows = data.num_rows();
        let num_bytes = memory_size(&data);

        if !self.thresholds.check_for_compact(num_rows, num_bytes) {
            // holding slices of blocks to merge later may lead to oom, so
            // 1. we expect blocks from file formats are not slice.
            // 2. if block is split here, cut evenly and emit them at once.
            let rows_per_block = self
                .thresholds
                .calc_rows_per_block(num_bytes, num_rows, None);
            Ok(vec![DataBlock::empty_with_meta(Box::new(
                BlockCompactMeta::Split {
                    block: data,
                    rows_per_block,
                },
            ))])
        } else if self.thresholds.check_large_enough(num_rows, num_bytes) {
            // pass through the new data block just arrived
            Ok(vec![DataBlock::empty_with_meta(Box::new(
                BlockCompactMeta::NoChange(vec![data]),
            ))])
        } else {
            self.accumulated_rows += num_rows;
            self.accumulated_bytes += num_bytes;
            self.pending_blocks.push(data);

            if self
                .thresholds
                .check_large_enough(self.accumulated_rows, self.accumulated_bytes)
            {
                self.reset_accumulated();
                Ok(vec![DataBlock::empty_with_meta(Box::new(
                    BlockCompactMeta::Concat(std::mem::take(&mut self.pending_blocks)),
                ))])
            } else {
                Ok(vec![])
            }
        }
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        match self.pending_blocks.len() {
            0 => Ok(vec![]),
            1 => Ok(vec![DataBlock::empty_with_meta(Box::new(
                BlockCompactMeta::NoChange(std::mem::take(&mut self.pending_blocks)),
            ))]),
            _ => Ok(vec![DataBlock::empty_with_meta(Box::new(
                BlockCompactMeta::Concat(std::mem::take(&mut self.pending_blocks)),
            ))]),
        }
    }
}

pub fn memory_size(data_block: &DataBlock) -> usize {
    data_block
        .columns()
        .iter()
        .map(|entry| match &entry.value {
            Value::Column(Column::Nullable(col)) if col.validity.true_count() == 0 => 0,
            _ => entry.memory_size(),
        })
        .sum()
}
