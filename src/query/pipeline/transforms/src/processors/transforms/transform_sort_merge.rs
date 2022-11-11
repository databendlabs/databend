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

use common_exception::Result;
use common_expression::Chunk;
use common_expression::SortColumnDescription;

use super::Compactor;
use super::TransformCompact;
use crate::processors::transforms::Aborting;

pub struct SortMergeCompactor {
    chunk_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    aborting: Arc<AtomicBool>,
}

impl SortMergeCompactor {
    pub fn new(
        chunk_size: usize,
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Self {
        SortMergeCompactor {
            chunk_size,
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

    fn compact_final(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>> {
        if chunks.is_empty() {
            Ok(vec![])
        } else {
            let aborting = self.aborting.clone();
            let aborting: Aborting = Arc::new(Box::new(move || aborting.load(Ordering::Relaxed)));

            let chunk = Chunk::merge_sort(
                chunks,
                &self.sort_columns_descriptions,
                self.limit,
                aborting,
            )?;
            // split chunk by `self.chunk_size`
            let num_rows = chunk.num_rows();
            let num_chunks =
                num_rows / self.chunk_size + usize::from(num_rows % self.chunk_size > 0);
            let mut start = 0;
            let mut output = Vec::with_capacity(num_chunks);
            for _ in 0..num_chunks {
                let end = std::cmp::min(start + self.chunk_size, num_rows);
                let chunk = Chunk::take_by_slice_limit(&chunk, (start, end - start), self.limit);
                start = end;
                output.push(chunk);
            }
            Ok(output)
        }
    }
}

pub type TransformSortMerge = TransformCompact<SortMergeCompactor>;
