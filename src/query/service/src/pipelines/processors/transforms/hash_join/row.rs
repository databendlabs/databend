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

use std::sync::Arc;
use std::sync::RwLock;

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::KeysState;
use common_hashtable::RowPtr;
use common_storages_fuse::TableContext;

use crate::sessions::QueryContext;

pub type ColumnVector = Vec<(Column, DataType)>;

pub struct Chunk {
    pub data_block: DataBlock,
    pub cols: ColumnVector,
    pub keys_state: Option<KeysState>,
}

impl Chunk {
    pub fn num_rows(&self) -> usize {
        self.data_block.num_rows()
    }
}

pub struct RowSpace {
    pub data_schema: DataSchemaRef,
    pub chunks: RwLock<Vec<Chunk>>,
    pub buffer: RwLock<Vec<DataBlock>>,
}

impl RowSpace {
    pub fn new(ctx: Arc<QueryContext>, data_schema: DataSchemaRef) -> Result<Self> {
        let buffer_size = ctx.get_settings().get_max_block_size()? * 16;
        Ok(Self {
            data_schema,
            chunks: RwLock::new(vec![]),
            buffer: RwLock::new(Vec::with_capacity(buffer_size as usize)),
        })
    }

    pub fn push_cols(&self, data_block: DataBlock, cols: ColumnVector) -> Result<()> {
        let chunk = Chunk {
            data_block,
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

    pub fn datablocks(&self) -> Vec<DataBlock> {
        let chunks = self.chunks.read().unwrap();
        chunks.iter().map(|c| c.data_block.clone()).collect()
    }

    pub fn gather(
        &self,
        row_ptrs: &[RowPtr],
        data_blocks: &Vec<&DataBlock>,
        num_rows: &usize,
    ) -> Result<DataBlock> {
        let mut indices = Vec::with_capacity(row_ptrs.len());

        for row_ptr in row_ptrs {
            indices.push((row_ptr.chunk_index, row_ptr.row_index, 1usize));
        }

        if !data_blocks.is_empty() && *num_rows != 0 {
            let data_block = DataBlock::take_blocks(data_blocks, indices.as_slice(), indices.len());
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.data_schema.clone()))
        }
    }
}
