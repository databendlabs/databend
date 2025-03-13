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

use std::mem;
use std::sync::Arc;

use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_storage::FileStatus;
use log::debug;

use crate::read::load_context::LoadContext;

pub struct BlockBuilderState {
    pub column_builders: Vec<ColumnBuilder>,
    pub internal_column_builders: Vec<ColumnBuilder>,
    pub internal_columns: Vec<InternalColumn>,
    pub row_id_column_pos: Option<usize>,
    pub num_rows: usize,
    pub file_status: FileStatus,
    pub file_path: String,
    pub file_full_path: String,
}

impl BlockBuilderState {
    pub(crate) fn create(ctx: Arc<LoadContext>) -> Self {
        let column_builders: Vec<_> = ctx
            .schema
            .fields()
            .iter()
            .map(|f| {
                ColumnBuilder::with_capacity_hint(
                    &f.data_type().into(),
                    // todo(youngsofun): calculate the capacity based on the memory and schema
                    1024,
                    false,
                )
            })
            .collect();
        let internal_column_builders = ctx
            .internal_columns
            .iter()
            .map(|c| ColumnBuilder::with_capacity_hint(&c.data_type(), 1024, false))
            .collect();

        let row_id_column_pos = ctx
            .internal_columns
            .iter()
            .position(|c| c.column_type == InternalColumnType::FileRowNumber);

        BlockBuilderState {
            column_builders,
            internal_column_builders,
            internal_columns: ctx.internal_columns.clone(),
            row_id_column_pos,
            num_rows: 0,
            file_status: Default::default(),
            file_path: "".to_string(),
            file_full_path: "".to_string(),
        }
    }

    pub(crate) fn take_columns(&mut self, on_finish: bool) -> Result<Vec<Column>> {
        // todo(youngsofun): calculate the capacity according to last batch
        let capacity = if on_finish { 0 } else { 1024 };
        self.num_rows = 0;
        let cols: Vec<_> = self
            .column_builders
            .iter_mut()
            .map(|col| {
                let empty_builder =
                    ColumnBuilder::with_capacity_hint(&col.data_type(), capacity, false);
                std::mem::replace(col, empty_builder).build()
            })
            .collect();
        Ok(cols)
    }

    pub(crate) fn take_internal_columns(&mut self, on_finish: bool) -> Result<Vec<Column>> {
        let capacity = if on_finish { 0 } else { 1024 };
        let cols: Vec<_> = self
            .internal_column_builders
            .iter_mut()
            .map(|col| {
                let empty_builder =
                    ColumnBuilder::with_capacity_hint(&col.data_type(), capacity, false);
                std::mem::replace(col, empty_builder).build()
            })
            .collect();
        Ok(cols)
    }

    pub(crate) fn flush_status(&mut self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        let file_status = mem::take(&mut self.file_status);
        ctx.add_file_status(&self.file_path, file_status)
    }

    pub(crate) fn add_row(&mut self, row_id: usize) {
        self.num_rows += 1;
        self.file_status.num_rows_loaded += 1;
        if let Some(pos) = self.row_id_column_pos {
            self.internal_column_builders[pos]
                .push(ScalarRef::Number(NumberScalar::UInt64(row_id as u64)))
        }
    }

    pub(crate) fn add_internals_columns_batch(&mut self, n: usize) {
        for (i, c) in self.internal_columns.iter().enumerate() {
            match c.column_type {
                InternalColumnType::FileName => {
                    self.internal_column_builders[i]
                        .push_repeat(&ScalarRef::String(&self.file_full_path), n);
                }
                InternalColumnType::FileRowNumber => {}
                _ => {
                    unreachable!()
                }
            }
        }
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.column_builders
            .iter()
            .chain(self.internal_column_builders.iter())
            .map(|x| x.memory_size())
            .sum()
    }

    pub fn flush_block(&mut self, on_finish: bool) -> Result<Vec<DataBlock>> {
        let num_rows = self.num_rows;
        let mut columns = self.take_columns(on_finish)?;
        let internal_columns = self.take_internal_columns(on_finish)?;
        columns.extend(internal_columns);
        if columns.is_empty() || num_rows == 0 {
            Ok(vec![])
        } else {
            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }

    pub fn try_flush_block_by_memory(&mut self, ctx: &Arc<LoadContext>) -> Result<Vec<DataBlock>> {
        let mem = self.memory_size();
        debug!(
            "chunk builder added new batch: row {} size {}",
            self.num_rows, mem
        );
        if self.num_rows >= ctx.block_compact_thresholds.min_rows_per_block
            || mem > ctx.block_compact_thresholds.max_bytes_per_block
        {
            self.flush_block(false)
        } else {
            Ok(vec![])
        }
    }
}
