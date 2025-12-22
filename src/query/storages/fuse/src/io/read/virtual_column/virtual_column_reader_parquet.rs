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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::eval_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::Compression;

use super::VirtualColumnReader;
use crate::BlockReadResult;
use crate::io::read::block::parquet::column_chunks_to_record_batch;

pub struct VirtualBlockReadResult {
    pub num_rows: usize,
    pub compression: Compression,
    pub data: BlockReadResult,
    pub schema: TableSchemaRef,
    // Source columns that can be ignored without reading
    pub ignore_column_ids: Option<HashSet<ColumnId>>,
}

impl VirtualBlockReadResult {
    pub fn create(
        num_rows: usize,
        compression: Compression,
        data: BlockReadResult,
        schema: TableSchemaRef,
        ignore_column_ids: Option<HashSet<ColumnId>>,
    ) -> VirtualBlockReadResult {
        VirtualBlockReadResult {
            num_rows,
            compression,
            data,
            schema,
            ignore_column_ids,
        }
    }
}

impl VirtualColumnReader {
    pub async fn read_parquet_data_by_merge_io(
        &self,
        read_settings: &ReadSettings,
        virtual_block_meta: &Option<&VirtualBlockMetaIndex>,
        num_rows: usize,
    ) -> Option<VirtualBlockReadResult> {
        let Some(virtual_block_meta) = virtual_block_meta else {
            return None;
        };

        let mut schema = TableSchema::empty();
        let mut ranges = Vec::with_capacity(virtual_block_meta.virtual_column_metas.len());
        for (virtual_column_id, virtual_column_meta) in &virtual_block_meta.virtual_column_metas {
            let (offset, len) = virtual_column_meta.offset_length();
            ranges.push((*virtual_column_id, offset..(offset + len)));
            let data_type = virtual_column_meta.data_type();

            let name = format!("{}", virtual_column_id);
            schema.add_internal_field(&name, data_type, *virtual_column_id);
        }

        let virtual_loc = &virtual_block_meta.virtual_block_location;
        let merge_io_result =
            MergeIOReader::merge_io_read(read_settings, self.dal.clone(), virtual_loc, &ranges)
                .await
                .ok()?;

        let block_read_res = BlockReadResult::create(merge_io_result, vec![], vec![]);
        let ignore_column_ids =
            self.generate_ignore_column_ids(&virtual_block_meta.ignored_source_column_ids);

        Some(VirtualBlockReadResult::create(
            num_rows,
            self.compression.into(),
            block_read_res,
            Arc::new(schema),
            ignore_column_ids,
        ))
    }

    pub fn deserialize_virtual_columns(
        &self,
        mut data_block: DataBlock,
        virtual_data: Option<VirtualBlockReadResult>,
    ) -> Result<DataBlock> {
        let orig_schema = virtual_data
            .as_ref()
            .map(|virtual_data| virtual_data.schema.clone())
            .unwrap_or_default();
        let record_batch = virtual_data
            .map(|virtual_data| {
                let columns_chunks = virtual_data.data.columns_chunks()?;
                column_chunks_to_record_batch(
                    &virtual_data.schema,
                    virtual_data.num_rows,
                    &columns_chunks,
                    &virtual_data.compression,
                )
            })
            .transpose()?;

        // If the virtual column has already generated, add it directly,
        // otherwise extract it from the source column
        let func_ctx = self.ctx.get_function_context()?;
        for virtual_column_field in self.virtual_column_info.virtual_column_fields.iter() {
            let name = format!("{}", virtual_column_field.column_id);
            if let Some(arrow_array) = record_batch
                .as_ref()
                .and_then(|r| r.column_by_name(&name).cloned())
            {
                let orig_field = orig_schema.field_with_name(&name).unwrap();
                let orig_type: DataType = orig_field.data_type().into();
                let column = Column::from_arrow_rs(arrow_array, &orig_type)?;
                let data_type: DataType = virtual_column_field.data_type.as_ref().into();
                if orig_type != data_type {
                    let cast_func_name = format!(
                        "to_{}",
                        data_type.remove_nullable().to_string().to_lowercase()
                    );
                    let (cast_value, cast_data_type) = eval_function(
                        None,
                        &cast_func_name,
                        [(Value::Column(column), orig_type)],
                        &func_ctx,
                        data_block.num_rows(),
                        &BUILTIN_FUNCTIONS,
                    )?;
                    data_block.add_value(cast_value, cast_data_type);
                } else {
                    data_block.add_column(column);
                };
                continue;
            }
            let src_index = self
                .source_schema
                .index_of(&virtual_column_field.source_name)
                .unwrap();
            let source = data_block.get_by_offset(src_index);
            let src_arg = (source.value(), source.data_type());
            let path_arg = (
                Value::Scalar(virtual_column_field.key_paths.clone()),
                DataType::String,
            );

            let (value, data_type) = eval_function(
                None,
                "get_by_keypath",
                [src_arg, path_arg],
                &func_ctx,
                data_block.num_rows(),
                &BUILTIN_FUNCTIONS,
            )?;

            if let Some(cast_func_name) = &virtual_column_field.cast_func_name {
                let (cast_value, cast_data_type) = eval_function(
                    None,
                    cast_func_name,
                    [(value, data_type)],
                    &func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;
                data_block.add_value(cast_value, cast_data_type);
            } else {
                data_block.add_value(value, data_type);
            };
        }

        Ok(data_block)
    }
}
