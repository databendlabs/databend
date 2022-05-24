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

use std::sync::RwLock;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use smallvec::SmallVec;

pub type KeysVector = Vec<SmallVec<[u8; 16]>>;

pub struct Chunk {
    pub data_block: DataBlock,
    pub keys: KeysVector,
}

impl Chunk {
    pub fn num_rows(&self) -> usize {
        self.data_block.num_rows()
    }
}

#[derive(Clone, Copy)]
pub struct RowPtr {
    pub chunk_index: u32,
    pub row_index: u32,
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

    pub fn push(&self, data_block: DataBlock, keys: KeysVector) -> Result<()> {
        let chunk = Chunk { data_block, keys };

        {
            // Acquire write lock in current scope
            let mut chunks = self.chunks.write().unwrap();
            chunks.push(chunk);
        }

        Ok(())
    }

    pub fn gather(&self, row_ptrs: &[RowPtr]) -> Result<DataBlock> {
        let mut data_blocks = vec![];
        let mut indices = vec![];

        {
            // Acquire read lock in current scope
            let chunks = self.chunks.read().unwrap();
            for row_ptr in row_ptrs.iter() {
                assert!((row_ptr.chunk_index as usize) < chunks.len());
                let block = self.gather_single_chunk(&chunks[row_ptr.chunk_index as usize], &[
                    row_ptr.row_index,
                ])?;
                if !block.is_empty() {
                    indices.push((data_blocks.len(), 0));
                    data_blocks.push(block);
                }
            }
        }

        // If build_key doesn't have duplicated columns, the length of row_ptrs will be one.
        // So we don't need to `gather_blocks`, directly return.
        if data_blocks.len() == 1 {
            return Ok(data_blocks[0].clone());
        }

        if !data_blocks.is_empty() {
            let data_block = DataBlock::gather_blocks(&data_blocks, indices.as_slice())?;
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.data_schema.clone()))
        }
    }

    fn gather_single_chunk(&self, chunk: &Chunk, indices: &[u32]) -> Result<DataBlock> {
        DataBlock::block_take_by_indices(&chunk.data_block, indices)
    }
}
