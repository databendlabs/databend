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
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableField;
use databend_common_expression::Value;
use databend_common_metrics::storage::*;
use databend_common_native::read::reader::NativeReader;
use databend_common_native::read::ColumnIter;
use databend_common_storage::ColumnNode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;

use super::block_reader_deserialize::DeserializedArray;
use super::block_reader_deserialize::FieldDeserializationContext;
use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::BlockReader;
use crate::io::NativeReaderExt;

impl BlockReader {
    /// Deserialize column chunks data from native format to DataBlock.
    pub(super) fn deserialize_native_chunks(
        &self,
        block_path: &str,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
    ) -> Result<DataBlock> {
        let start = Instant::now();

        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let deserialized_res = self.deserialize_native_chunks_with_buffer(
            block_path,
            num_rows,
            column_metas,
            column_chunks,
        );

        // Perf.
        {
            metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        }

        deserialized_res
    }

    /// Deserialize column chunks data from native format to DataBlock with a uncompressed buffer.
    pub(super) fn deserialize_native_chunks_with_buffer(
        &self,
        block_path: &str,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
    ) -> Result<DataBlock> {
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let mut need_default_vals = Vec::with_capacity(self.project_column_nodes.len());
        let mut need_to_fill_default_val = false;
        let mut deserialized_column_arrays = Vec::with_capacity(self.projection.len());
        let field_deserialization_ctx = FieldDeserializationContext {
            column_metas,
            column_chunks: &column_chunks,
        };

        for column_node in &self.project_column_nodes {
            let deserialized_column = self
                .deserialize_native_field(&field_deserialization_ctx, column_node)
                .map_err(|e| {
                    e.add_message(format!(
                        "failed to deserialize column: {:?}, location {} ",
                        column_node, block_path
                    ))
                })?;

            match deserialized_column {
                None => {
                    need_to_fill_default_val = true;
                    need_default_vals.push(true);
                }
                Some(v) => {
                    deserialized_column_arrays.push(v);
                    need_default_vals.push(false);
                }
            }
        }

        // assembly the arrays
        let mut chunk_arrays = vec![];
        for array in &deserialized_column_arrays {
            match array {
                DeserializedArray::Deserialized((_, array, ..)) => {
                    chunk_arrays.push(array.clone());
                }
                DeserializedArray::NoNeedToCache(array) => {
                    chunk_arrays.push(array.clone());
                }
                DeserializedArray::Cached(sized_column) => {
                    chunk_arrays.push(sized_column.0.clone());
                }
            }
        }

        // build data block
        let data_block = if need_to_fill_default_val {
            let mut default_vals = Vec::with_capacity(need_default_vals.len());
            for (i, need_default_val) in need_default_vals.iter().enumerate() {
                if !need_default_val {
                    default_vals.push(None);
                } else {
                    default_vals.push(Some(self.default_vals[i].clone()));
                }
            }
            DataBlock::create_with_opt_default_value(
                chunk_arrays,
                &self.data_schema(),
                &default_vals,
                num_rows,
            )?
        } else {
            debug_assert!(chunk_arrays.len() == self.projected_schema.num_fields());
            let cols = chunk_arrays
                .into_iter()
                .zip(self.data_schema().fields())
                .map(|(arr, f)| {
                    let data_type = f.data_type();
                    Value::from_arrow_rs(arr, data_type)
                        .map(|val| BlockEntry::new(data_type.clone(), val))
                })
                .collect::<Result<Vec<_>>>()?;

            DataBlock::new(cols, num_rows)
        };

        // populate cache if necessary
        if self.put_cache {
            if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
                // populate array cache items
                for item in deserialized_column_arrays.into_iter() {
                    if let DeserializedArray::Deserialized((column_id, array, size)) = item {
                        let meta = column_metas.get(&column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        let key = TableDataCacheKey::new(block_path, column_id, offset, len);
                        cache.insert(key.into(), (array, size));
                    }
                }
            }
        }
        Ok(data_block)
    }

    fn chunks_to_native_column(
        &self,
        metas: Vec<&ColumnMeta>,
        chunks: Vec<&[u8]>,
        field: TableField,
    ) -> Result<Column> {
        let mut page_metas = Vec::with_capacity(chunks.len());
        let mut readers = Vec::with_capacity(chunks.len());
        for (chunk, meta) in chunks.into_iter().zip(metas.into_iter()) {
            let meta = meta.as_native().unwrap();
            let reader = std::io::Cursor::new(chunk);
            readers.push(reader);
            page_metas.push(meta.pages.clone());
        }

        match self.native_columns_reader.batch_read_column(
            readers,
            field.data_type().clone(),
            page_metas,
        ) {
            Ok(array) => Ok(array),
            Err(err) => Err(err.into()),
        }
    }

    fn deserialize_native_field<'a>(
        &self,
        deserialization_context: &'a FieldDeserializationContext,
        column_node: &ColumnNode,
    ) -> Result<Option<DeserializedArray<'a>>> {
        let indices = &column_node.leaf_indices;
        let column_chunks = deserialization_context.column_chunks;
        // column passed in may be a compound field (with sub leaves),
        // or a leaf column of compound field
        let is_nested = column_node.has_children();
        let estimated_cap = indices.len();
        let mut field_column_metas = Vec::with_capacity(estimated_cap);
        let mut field_column_data = Vec::with_capacity(estimated_cap);
        let mut field_leaf_ids = Vec::with_capacity(estimated_cap);
        let mut field_uncompressed_size = 0;

        for (i, leaf_index) in indices.iter().enumerate() {
            let column_id = column_node.leaf_column_ids[i];
            if let Some(column_meta) = deserialization_context.column_metas.get(&column_id) {
                if let Some(chunk) = column_chunks.get(&column_id) {
                    match chunk {
                        DataItem::RawData(data) => {
                            field_column_metas.push(column_meta);
                            field_column_data.push(data.as_ref());
                            field_leaf_ids.push(*leaf_index);
                            field_uncompressed_size += data.len();
                        }
                        DataItem::ColumnArray(column_array) => {
                            if is_nested {
                                // TODO more context info for error message
                                return Err(ErrorCode::StorageOther(
                                    "unexpected nested field: nested leaf field hits cached",
                                ));
                            }
                            // since it is not nested, one column is enough
                            return Ok(Some(DeserializedArray::Cached(column_array)));
                        }
                    }
                } else {
                    // If the column is the source of virtual columns, it may be ignored.
                    // TODO cover more case and add context info for error message
                    // no raw data of given column id, it is unexpected
                    return Ok(None);
                }
            } else {
                break;
            }
        }

        if !field_column_metas.is_empty() {
            let column = self.chunks_to_native_column(
                field_column_metas,
                field_column_data,
                column_node.table_field.clone(),
            )?;
            let array = column.clone().into_arrow_rs();
            // mark the array
            if is_nested {
                // the array is not intended to be cached
                // currently, caching of compound field columns is not support
                Ok(Some(DeserializedArray::NoNeedToCache(array)))
            } else {
                // the array is deserialized from raw bytes, should be cached
                let column_id = column_node.leaf_column_ids[0];
                Ok(Some(DeserializedArray::Deserialized((
                    column_id,
                    array,
                    field_uncompressed_size,
                ))))
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn build_column_iter(
        &self,
        column_node: &ColumnNode,
        readers: Vec<NativeReader<Box<dyn NativeReaderExt>>>,
    ) -> Result<ColumnIter<'static>> {
        match self.native_columns_reader.column_iters(
            readers,
            column_node.table_field.clone(),
            column_node.init.clone(),
        ) {
            Ok(column_iter) => Ok(column_iter),
            Err(err) => Err(err.into()),
        }
    }
}
