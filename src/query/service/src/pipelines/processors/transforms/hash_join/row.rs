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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_hashtable::RowPtr;
use databend_common_sql::ColumnSet;
use databend_common_storages_fuse::TableContext;
use parking_lot::RwLock;

use crate::sessions::QueryContext;

pub struct RowSpace {
    pub build_schema: DataSchemaRef,
    pub buffer: RwLock<Vec<DataBlock>>,
    // In `build` method, need to compute how many rows in buffer.
    // However, computing rows count every time will hurt performance,
    // so use `buffer_row_size` to record rows count in buffer.
    pub buffer_row_size: AtomicUsize,
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
            buffer: RwLock::new(Vec::with_capacity(buffer_size as usize)),
            buffer_row_size: AtomicUsize::new(0),
        })
    }

    pub fn gather(
        &self,
        row_ptrs: &[RowPtr],
        build_columns: &[ColumnVec],
        build_columns_data_type: &[DataType],
        num_rows: &usize,
        string_items_buf: &mut Option<Vec<(u64, usize)>>,
    ) -> Result<DataBlock> {
        if *num_rows != 0 {
            let data_block = DataBlock::take_column_vec(
                build_columns,
                build_columns_data_type,
                row_ptrs,
                row_ptrs.len(),
                string_items_buf,
            );
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.build_schema.clone()))
        }
    }

    pub fn reset(&self) {
        let mut buffer = self.buffer.write();
        buffer.clear();
        self.buffer_row_size.store(0, Ordering::Release);
    }
}
