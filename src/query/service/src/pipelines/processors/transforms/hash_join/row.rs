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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::RwLock;

use common_exception::Result;
use common_expression::Chunk as DataChunk;
use common_expression::DataSchemaRef;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;

pub type ColumnVector = Vec<ColumnRef>;

pub struct Chunk {
    pub chunk: DataChunk,
    pub cols: ColumnVector,
    pub keys_state: Option<KeysState>,
}

impl Chunk {
    pub fn num_rows(&self) -> usize {
        self.chunk.num_rows()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RowPtr {
    pub chunk_index: usize,
    pub row_index: usize,
    pub marker: Option<MarkerKind>,
}

impl RowPtr {
    pub fn new(chunk_index: usize, row_index: usize) -> Self {
        RowPtr {
            chunk_index,
            row_index,
            marker: None,
        }
    }
}

pub struct RowSpace {
    pub data_schema: DataSchemaRef,
    pub chunks: RwLock<Vec<Chunk>>,
}

impl RowSpace {
    pub fn new(data_schema: DataSchemaRef) -> Self {
        Self {
            data_schema,
            chunks: RwLock::new(vec![]),
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.data_schema.clone()
    }

    pub fn push_cols(&self, data_chunk: DataChunk, cols: ColumnVector) -> Result<()> {
        let chunk = Chunk {
            chunk: data_chunk,
            cols,
            keys_state: None,
        };

        {
            // Acquire write lock in current scope
            let mut chunks = self.chunks.write().unwrap();
            chunks.push(chunk);
        }

        Ok(())
    }

    pub fn data_chunks(&self) -> Vec<DataChunk> {
        let chunks = self.chunks.read().unwrap();
        chunks.iter().map(|c| c.chunk.clone()).collect()
    }

    pub fn gather(&self, row_ptrs: &[RowPtr]) -> Result<DataChunk> {
        let data_chunks = self.data_chunks();
        let num_rows = data_chunks
            .iter()
            .fold(0, |acc, chunk| acc + chunk.num_rows());
        let mut indices = Vec::with_capacity(row_ptrs.len());

        for row_ptr in row_ptrs.iter() {
            indices.push((row_ptr.chunk_index, row_ptr.row_index, 1usize));
        }

        if !data_chunks.is_empty() && num_rows != 0 {
            let data_chunk = DataChunk::take_chunks(&data_chunks, indices.as_slice());
            Ok(data_chunk)
        } else {
            Ok(DataChunk::empty())
        }
    }
}

impl PartialEq for RowPtr {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_index == other.chunk_index && self.row_index == other.row_index
    }
}

impl Eq for RowPtr {}

impl Hash for RowPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.chunk_index.hash(state);
        self.row_index.hash(state);
    }
}
