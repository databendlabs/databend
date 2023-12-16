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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;

use super::Compactor;

pub struct BlockCompactor {
    thresholds: BlockThresholds,
    aborting: Arc<AtomicBool>,
}

impl BlockCompactor {
    pub fn new(thresholds: BlockThresholds) -> Self {
        BlockCompactor {
            thresholds,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for BlockCompactor {
    fn name() -> &'static str {
        "BlockCompactTransform"
    }

    fn use_partial_compact(&self) -> bool {
        true
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_partial(&mut self, blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let size = blocks.len();
        let mut res = Vec::with_capacity(size);
        let block = blocks[size - 1].clone();

        // perfect block
        if self
            .thresholds
            .check_perfect_block(block.num_rows(), block.memory_size())
        {
            res.push(block);
            blocks.remove(size - 1);
        } else {
            let accumulated_rows: usize = blocks.iter_mut().map(|b| b.num_rows()).sum();
            let accumulated_bytes: usize = blocks.iter_mut().map(|b| b.memory_size()).sum();

            let merged = DataBlock::concat(blocks)?;
            blocks.clear();

            if accumulated_rows >= self.thresholds.max_rows_per_block {
                let (perfect, remain) = merged.split_by_rows(self.thresholds.max_rows_per_block);
                res.extend(perfect);
                if let Some(b) = remain {
                    blocks.push(b);
                }
            } else if accumulated_bytes >= self.thresholds.max_bytes_per_block {
                // too large for merged block, flush to results
                res.push(merged);
            } else {
                // keep the merged block into blocks for future merge
                blocks.push(merged);
            }
        }

        Ok(res)
    }

    fn compact_final(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let mut res = Vec::with_capacity(blocks.len());
        let mut temp_blocks = vec![];
        let mut accumulated_rows = 0;
        let aborted_query_err = || {
            Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ))
        };
        for block in blocks.iter() {
            if self.aborting.load(Ordering::Relaxed) {
                return aborted_query_err();
            }

            // Perfect block, no need to compact
            if self
                .thresholds
                .check_perfect_block(block.num_rows(), block.memory_size())
            {
                res.push(block.clone());
            } else {
                let block = if block.num_rows() > self.thresholds.max_rows_per_block {
                    let b = block.slice(0..self.thresholds.max_rows_per_block);
                    res.push(b);
                    block.slice(self.thresholds.max_rows_per_block..block.num_rows())
                } else {
                    block.clone()
                };

                accumulated_rows += block.num_rows();
                temp_blocks.push(block);

                while accumulated_rows >= self.thresholds.max_rows_per_block {
                    if self.aborting.load(Ordering::Relaxed) {
                        return aborted_query_err();
                    }

                    let block = DataBlock::concat(&temp_blocks)?;
                    res.push(block.slice(0..self.thresholds.max_rows_per_block));
                    accumulated_rows -= self.thresholds.max_rows_per_block;

                    temp_blocks.clear();
                    if accumulated_rows != 0 {
                        temp_blocks.push(
                            block.slice(self.thresholds.max_rows_per_block..block.num_rows()),
                        );
                    }
                }
            }
        }

        if accumulated_rows != 0 {
            if self.aborting.load(Ordering::Relaxed) {
                return aborted_query_err();
            }

            let block = DataBlock::concat(&temp_blocks)?;
            res.push(block);
        }

        Ok(res)
    }
}
