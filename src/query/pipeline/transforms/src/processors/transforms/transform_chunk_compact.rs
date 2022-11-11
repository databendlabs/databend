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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ChunkCompactThresholds;

use super::Compactor;
use super::TransformCompact;

pub struct ChunkCompactor {
    thresholds: ChunkCompactThresholds,
    // A flag denoting whether it is a recluster operation.
    // Will be removed later.
    is_recluster: bool,
    aborting: Arc<AtomicBool>,
}

impl ChunkCompactor {
    pub fn new(thresholds: ChunkCompactThresholds, is_recluster: bool) -> Self {
        ChunkCompactor {
            thresholds,
            is_recluster,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for ChunkCompactor {
    fn name() -> &'static str {
        "ChunkCompactTransform"
    }

    fn use_partial_compact() -> bool {
        true
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_partial(&mut self, chunks: &mut Vec<Chunk>) -> Result<Vec<Chunk>> {
        if chunks.is_empty() {
            return Ok(vec![]);
        }

        let size = chunks.len();
        let mut res = Vec::with_capacity(size);
        let chunk = chunks[size - 1].clone();

        // perfect chunk
        if self
            .thresholds
            .check_perfect_chunk(chunk.num_rows(), chunk.memory_size())
        {
            res.push(chunk);
            chunks.remove(size - 1);
        } else {
            let accumulated_rows: usize = chunks.iter_mut().map(|b| b.num_rows()).sum();
            let accumulated_bytes: usize = chunks.iter_mut().map(|b| b.memory_size()).sum();

            let merged = Chunk::concat(chunks)?;
            chunks.clear();

            if accumulated_rows >= self.thresholds.max_rows_per_chunk {
                // Used for recluster opreation, will be removed later.
                if self.is_recluster {
                    let mut offset = 0;
                    let mut remain_rows = accumulated_rows;
                    while remain_rows >= self.thresholds.max_rows_per_chunk {
                        let cut =
                            merged.slice(offset..(offset + self.thresholds.max_rows_per_chunk));
                        res.push(cut);
                        offset += self.thresholds.max_rows_per_chunk;
                        remain_rows -= self.thresholds.max_rows_per_chunk;
                    }

                    if remain_rows > 0 {
                        chunks.push(merged.slice(offset..(offset + remain_rows)));
                    }
                } else {
                    // we can't use slice here, it did not deallocate memory
                    res.push(merged);
                }
            } else if accumulated_bytes >= self.thresholds.max_bytes_per_chunk {
                // too large for merged chunk, flush to results
                res.push(merged);
            } else {
                // keep the merged chunk into chunks for future merge
                chunks.push(merged);
            }
        }

        Ok(res)
    }

    fn compact_final(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>> {
        let mut res = Vec::with_capacity(chunks.len());
        let mut temp_chunks = vec![];
        let mut accumulated_rows = 0;

        for chunk in chunks.iter() {
            if self.aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            // Perfect chunk, no need to compact
            if self
                .thresholds
                .check_perfect_chunk(chunk.num_rows(), chunk.memory_size())
            {
                res.push(chunk.clone());
            } else {
                let chunk = if chunk.num_rows() > self.thresholds.max_rows_per_chunk {
                    let b = chunk.slice(0..self.thresholds.max_rows_per_chunk);
                    res.push(b);
                    chunk.slice(self.thresholds.max_rows_per_chunk..chunk.num_rows())
                } else {
                    chunk.clone()
                };

                accumulated_rows += chunk.num_rows();
                temp_chunks.push(chunk);

                while accumulated_rows >= self.thresholds.max_rows_per_chunk {
                    if self.aborting.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    let chunk = Chunk::concat(&temp_chunks)?;
                    res.push(chunk.slice(0..self.thresholds.max_rows_per_chunk));
                    accumulated_rows -= self.thresholds.max_rows_per_chunk;

                    temp_chunks.clear();
                    if accumulated_rows != 0 {
                        temp_chunks.push(
                            chunk.slice(self.thresholds.max_rows_per_chunk..chunk.num_rows()),
                        );
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

            let chunk = Chunk::concat(&temp_chunks)?;
            res.push(chunk);
        }

        Ok(res)
    }
}

pub type TransformChunkCompact = TransformCompact<ChunkCompactor>;
