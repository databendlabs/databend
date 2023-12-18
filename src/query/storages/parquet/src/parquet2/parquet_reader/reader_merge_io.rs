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

use std::collections::HashMap;
use std::ops::Range;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;

pub struct OwnerMemory {
    chunks: HashMap<usize, Bytes>,
}

impl OwnerMemory {
    pub fn create(chunks: Vec<(usize, Vec<u8>)>) -> OwnerMemory {
        let chunks = chunks
            .into_iter()
            .map(|(idx, chunk)| (idx, Bytes::from(chunk)))
            .collect();
        OwnerMemory { chunks }
    }

    pub fn get_chunk(&self, index: usize, path: &str) -> Result<Bytes> {
        match self.chunks.get(&index) {
            Some(chunk) => Ok(chunk.clone()),
            None => Err(ErrorCode::Internal(format!(
                "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                index, path
            ))),
        }
    }
}

pub struct MergeIOReadResult {
    block_path: String,
    columns_chunk_offsets: HashMap<FieldIndex, (usize, Range<usize>)>,
    owner_memory: OwnerMemory,
}

impl MergeIOReadResult {
    pub fn create(owner_memory: OwnerMemory, capacity: usize, path: String) -> MergeIOReadResult {
        MergeIOReadResult {
            block_path: path,
            columns_chunk_offsets: HashMap::with_capacity(capacity),
            owner_memory,
        }
    }

    pub fn column_buffers(&self) -> Result<HashMap<FieldIndex, Bytes>> {
        let mut res = HashMap::with_capacity(self.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.columns_chunk_offsets {
            let chunk = self.owner_memory.get_chunk(*chunk_idx, &self.block_path)?;
            res.insert(*column_id, chunk.slice(range.clone()));
        }

        Ok(res)
    }

    pub fn add_column_chunk(
        &mut self,
        chunk_index: usize,
        column_id: FieldIndex,
        range: Range<usize>,
    ) {
        self.columns_chunk_offsets
            .insert(column_id, (chunk_index, range));
    }
}
