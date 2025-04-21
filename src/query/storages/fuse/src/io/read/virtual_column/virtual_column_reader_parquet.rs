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

use arrow_array::RecordBatch;
use databend_common_catalog::plan::VirtualColumnField;
use databend_common_exception::Result;
use databend_common_expression::eval_function;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::VirtualBlockMetaIndex;
use databend_storages_common_table_meta::meta::Compression;

use super::VirtualColumnReader;
use crate::io::read::block::parquet::column_chunks_to_record_batch;
use crate::BlockReadResult;

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
    pub fn sync_read_parquet_data_by_merge_io(
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
        let merge_io_result = MergeIOReader::sync_merge_io_read(
            read_settings,
            self.dal.clone(),
            virtual_loc,
            &ranges,
        )
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

    /// Creates a VirtualColumnDataPaster that handles the integration of virtual column data into DataBlocks.
    ///
    /// This method prepares a paster object that can process virtual column data from virtual block
    /// read result, and later merge this data into existing DataBlocks. It deserializes virtual
    /// column data into record batches according to the optional batch size hint.
    ///
    /// # Arguments
    /// * `virtual_data` - Optional virtual block read result containing the data to be processed
    /// * `batch_size_hint` - Optional hint for controlling the size of generated record batches
    ///
    /// # Returns
    /// * `Result<VirtualColumnDataPaster>` - A paster object that can merge virtual column data
    ///   into DataBlocks, or an error if creation fails
    pub fn try_create_virtual_column_paster(
        &self,
        virtual_data: Option<VirtualBlockReadResult>,
        batch_size_hint: Option<usize>,
    ) -> Result<VirtualColumnDataPaster> {
        let orig_schema = virtual_data
            .as_ref()
            .map(|virtual_data| virtual_data.schema.clone())
            .unwrap_or_default();

        let record_batches = if let Some(virtual_data) = virtual_data {
            let columns_chunks = virtual_data.data.columns_chunks()?;
            let chunks = column_chunks_to_record_batch(
                &self.virtual_column_info.schema,
                virtual_data.num_rows,
                &columns_chunks,
                &virtual_data.compression,
                batch_size_hint,
            )?;
            Some(chunks)
        } else {
            None
        };

        let function_context = self.ctx.get_function_context()?;

        // Unfortunately, Paster cannot hold references to the fields that being cloned,
        // since the caller `DeserializeDataTransform` will take mutable reference of
        // VirtualColumnReader indirectly.
        Ok(VirtualColumnDataPaster {
            record_batches,
            function_context,
            next_record_batch_index: 0,
            virtual_column_fields: self.virtual_column_info.virtual_column_fields.clone(),
            source_schema: self.source_schema.clone(),
            orig_schema,
        })
    }
}

pub struct VirtualColumnDataPaster {
    record_batches: Option<Vec<RecordBatch>>,
    next_record_batch_index: usize,
    function_context: FunctionContext,
    virtual_column_fields: Vec<VirtualColumnField>,
    source_schema: TableSchemaRef,
    orig_schema: TableSchemaRef,
}

impl VirtualColumnDataPaster {
    /// Processes a DataBlock by adding virtual columns to it.
    ///
    /// This method enriches the provided DataBlock with virtual columns by either:
    /// 1. Using pre-computed virtual column data from deserialized record batches if available
    /// 2. Computing virtual column values on-the-fly from source columns
    ///
    /// For each virtual column field:
    /// - If the corresponding data exists in record batches, it is extracted and added directly
    /// - If not available in record batches, it is computed from source columns using path extraction
    /// - Type casting is performed if the source data type doesn't match the target virtual column type
    ///
    /// The method tracks which record batch to use via an internal counter that advances with each call.
    ///
    /// # Arguments
    /// * `data_block` - The input DataBlock to which virtual columns will be added
    ///
    /// # Returns
    /// * `Result<DataBlock>` - The modified DataBlock containing the original columns plus virtual columns,
    ///   or an error if the operation fails
    ///
    /// # Note
    /// This method must be called sequentially for each data block. The internal state keeps track of
    /// which pre-computed record batch to use for each call.
    pub fn paste_virtual_column(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        let record_batch = if let Some(record_batches) = &self.record_batches {
            assert!(record_batches.len() > self.next_record_batch_index);
            Some(&record_batches[self.next_record_batch_index])
        } else {
            None
        };

        self.next_record_batch_index += 1;

        let func_ctx = &self.function_context;

        // If the virtual column has already generated, add it directly,
        // otherwise extract it from the source column
        for virtual_column_field in self.virtual_column_fields.iter() {
            let name = format!("{}", virtual_column_field.column_id);
            if let Some(arrow_array) = record_batch
                .as_ref()
                .and_then(|r| r.column_by_name(&name).cloned())
            {
                let orig_field = self.orig_schema.field_with_name(&name).unwrap();
                let orig_type: DataType = orig_field.data_type().into();
                let value = Value::Column(Column::from_arrow_rs(arrow_array, &orig_type)?);
                let data_type: DataType = virtual_column_field.data_type.as_ref().into();
                let column = if orig_type != data_type {
                    let cast_func_name = format!(
                        "to_{}",
                        data_type.remove_nullable().to_string().to_lowercase()
                    );
                    let (cast_value, cast_data_type) = eval_function(
                        None,
                        &cast_func_name,
                        [(value, orig_type)],
                        &func_ctx,
                        data_block.num_rows(),
                        &BUILTIN_FUNCTIONS,
                    )?;
                    BlockEntry::new(cast_data_type, cast_value)
                } else {
                    BlockEntry::new(data_type, value)
                };
                data_block.add_column(column);
                continue;
            }
            let src_index = self
                .source_schema
                .index_of(&virtual_column_field.source_name)
                .unwrap();
            let source = data_block.get_by_offset(src_index);
            let src_arg = (source.value.clone(), source.data_type.clone());
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

            let column = if let Some(cast_func_name) = &virtual_column_field.cast_func_name {
                let (cast_value, cast_data_type) = eval_function(
                    None,
                    cast_func_name,
                    [(value, data_type)],
                    &func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;
                BlockEntry::new(cast_data_type, cast_value)
            } else {
                BlockEntry::new(data_type, value)
            };
            data_block.add_column(column);
        }

        Ok(data_block)
    }
}
