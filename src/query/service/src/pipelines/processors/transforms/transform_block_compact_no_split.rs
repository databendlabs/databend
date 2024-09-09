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
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::UnknownMode;

use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;

pub struct BlockCompactBuilder {
    thresholds: BlockThresholds,
    // Holds blocks that exceeded the threshold and are waiting to be compacted.
    staged_blocks: Vec<DataBlock>,
    // Holds blocks that are partially accumulated but haven't reached the threshold.
    pending_blocks: Vec<DataBlock>,
    accumulated_rows: usize,
    accumulated_bytes: usize,
}

impl BlockCompactBuilder {
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
        // This function creates a DataBlock with ExchangeShuffleMeta,
        // but the metadata is only used internally and is not intended
        // for inter-node communication. The ExchangeShuffleMeta is simply
        // being reused here for its structure, and no data will be transferred
        // between nodes.
        DataBlock::empty_with_meta(ExchangeShuffleMeta::create(std::mem::take(blocks)))
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

impl AccumulatingTransform for BlockCompactBuilder {
    const NAME: &'static str = "BlockCompactBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        self.accumulated_rows += data.num_rows();
        self.accumulated_bytes += data.memory_size();
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

        if !self.check_for_compact() && !self.pending_blocks.is_empty() {
            // blocks > 2N
            res.push(Self::create_output_data(&mut self.pending_blocks));
        } else {
            // N <= blocks < 2N
            std::mem::swap(&mut self.staged_blocks, &mut self.pending_blocks);
        }
        self.staged_blocks.push(data);
        self.reset_accumulated();
        Ok(res)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        match (
            self.pending_blocks.is_empty(),
            self.staged_blocks.is_empty(),
        ) {
            (true, true) => Ok(vec![]),
            (true, false) => Ok(vec![Self::create_output_data(&mut self.staged_blocks)]),
            (false, true) => Ok(vec![Self::create_output_data(&mut self.pending_blocks)]),
            (false, false) => {
                for block in &self.staged_blocks {
                    self.accumulated_rows += block.num_rows();
                    self.accumulated_bytes += block.memory_size();
                }
                if self.check_for_compact() {
                    self.staged_blocks.append(&mut self.pending_blocks);
                    Ok(vec![Self::create_output_data(&mut self.staged_blocks)])
                } else {
                    // blocks > 2N
                    Ok(vec![
                        Self::create_output_data(&mut self.staged_blocks),
                        Self::create_output_data(&mut self.pending_blocks),
                    ])
                }
            }
        }
    }
}

pub struct TransformBlockConcat;

#[async_trait::async_trait]
impl BlockMetaTransform<ExchangeShuffleMeta> for TransformBlockConcat {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformBlockConcat";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
        if meta.blocks.len() > 1 {
            Ok(vec![DataBlock::concat(&meta.blocks)?])
        } else {
            Ok(meta.blocks)
        }
    }
}
