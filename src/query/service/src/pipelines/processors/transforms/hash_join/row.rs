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

use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_hashtable::RowPtr;
use common_sql::ColumnSet;
use common_storages_fuse::TableContext;
use parking_lot::RwLock;

use crate::sessions::QueryContext;

pub struct RowSpace {
    pub build_schema: DataSchemaRef,
    pub write_lock: RwLock<bool>,
    pub buffer: RwLock<Vec<DataBlock>>,
    pub buffer_row_size: RwLock<usize>,
}

impl RowSpace {
    pub fn new(
        ctx: Arc<QueryContext>,
        data_schema: DataSchemaRef,
        build_projected_columns: &ColumnSet,
    ) -> Result<Self> {
        let buffer_size = ctx.get_settings().get_max_block_size()? * 16;
        let mut projected_build_fields = vec![];
        for (i, field) in data_schema.fields().iter().enumerate() {
            if build_projected_columns.contains(&i) {
                projected_build_fields.push(field.clone());
            }
        }
        let build_schema = DataSchemaRefExt::create(projected_build_fields);
        Ok(Self {
            build_schema,
            write_lock: RwLock::new(false),
            buffer: RwLock::new(Vec::with_capacity(buffer_size as usize)),
            buffer_row_size: RwLock::new(0),
        })
    }

    pub fn gather(
        &self,
        row_ptrs: &[RowPtr],
        data_blocks: &Vec<Vec<Column>>,
        num_rows: &usize,
    ) -> Result<DataBlock> {
        let mut indices = Vec::with_capacity(row_ptrs.len());

        for row_ptr in row_ptrs {
            indices.push((row_ptr.chunk_index, row_ptr.row_index, 1usize));
        }

        if *num_rows != 0 {
            let data_block =
                DataBlock::hash_join_take_blocks(data_blocks, indices.as_slice(), indices.len());
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.build_schema.clone()))
        }
    }
}
