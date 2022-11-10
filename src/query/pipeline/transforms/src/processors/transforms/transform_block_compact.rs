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

use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ChunkCompactThresholds;

use super::Compactor;
use super::TransformCompact;

pub struct BlockCompactor {
    thresholds: ChunkCompactThresholds,
    // A flag denoting whether it is a recluster operation.
    // Will be removed later.
    is_recluster: bool,
    aborting: Arc<AtomicBool>,
}

impl BlockCompactor {
    pub fn new(thresholds: ChunkCompactThresholds, is_recluster: bool) -> Self {
        BlockCompactor {
            thresholds,
            is_recluster,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for BlockCompactor {
    fn name() -> &'static str {
        "BlockCompactTransform"
    }

    fn use_partial_compact() -> bool {
        true
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_partial(&mut self, blocks: &mut Vec<Chunk>) -> Result<Vec<Chunk>> {
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

            let merged = Chunk::concat(blocks)?;
            blocks.clear();

            if accumulated_rows >= self.thresholds.max_rows_per_block {
                // Used for recluster opreation, will be removed later.
                if self.is_recluster {
                    let mut offset = 0;
                    let mut remain_rows = accumulated_rows;
                    while remain_rows >= self.thresholds.max_rows_per_block {
                        let range = Range {
                            start: offset,
                            end: self.thresholds.max_rows_per_block,
                        };
                        let cut = merged.slice(range);
                        res.push(cut);
                        offset += self.thresholds.max_rows_per_block;
                        remain_rows -= self.thresholds.max_rows_per_block;
                    }

                    if remain_rows > 0 {
                        let range = Range {
                            start: offset,
                            end: remain_rows,
                        };
                        blocks.push(merged.slice(range));
                    }
                } else {
                    // we can't use slice here, it did not deallocate memory
                    res.push(merged);
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

    fn compact_final(&self, blocks: &[Chunk]) -> Result<Vec<Chunk>> {
        let mut res = Vec::with_capacity(blocks.len());
        let mut temp_blocks = vec![];
        let mut accumulated_rows = 0;

        for block in blocks.iter() {
            if self.aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            // Perfect block, no need to compact
            if self
                .thresholds
                .check_perfect_block(block.num_rows(), block.memory_size())
            {
                res.push(block.clone());
            } else {
                let block = if block.num_rows() > self.thresholds.max_rows_per_block {
                    let range = Range {
                        start: 0,
                        end: self.thresholds.max_rows_per_block,
                    };
                    let b = block.slice(range);
                    res.push(b);
                    let range = Range {
                        start: self.thresholds.max_rows_per_block,
                        end: block.num_rows() - self.thresholds.max_rows_per_block,
                    };
                    block.slice(range)
                } else {
                    block.clone()
                };

                accumulated_rows += block.num_rows();
                temp_blocks.push(block);

                while accumulated_rows >= self.thresholds.max_rows_per_block {
                    if self.aborting.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let block = Chunk::concat(&temp_blocks)?;
                    let range = Range {
                        start: 0,
                        end: self.thresholds.max_rows_per_block,
                    };
                    res.push(block.slice(range));
                    accumulated_rows -= self.thresholds.max_rows_per_block;

                    temp_blocks.clear();
                    if accumulated_rows != 0 {
                        let range = Range {
                            start: self.thresholds.max_rows_per_block,
                            end: block.num_rows() - self.thresholds.max_rows_per_block,
                        };
                        temp_blocks.push(block.slice(range));
                    }
                }
            }
        }

        if accumulated_rows != 0 {
            if self.aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let block = Chunk::concat(&temp_blocks)?;
            res.push(block);
        }

        Ok(res)
    }
}

pub type TransformBlockCompact = TransformCompact<BlockCompactor>;
