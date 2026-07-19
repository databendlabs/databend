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
use std::fmt::Write;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterVisitor;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::types::DataType;
use databend_common_expression::visitor::ValueVisitor;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheEntry;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
mod adapter;
mod deserialize;
mod row_selection;

pub use adapter::RowGroupImplBuilder;
pub use deserialize::column_chunks_to_record_batch;
pub use row_selection::RowSelection;

use crate::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

impl BlockReader {
    pub fn deserialize_part(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            column_chunks,
            &part.compression,
            &part.location,
            selection,
        )
    }

    pub fn page_range_bitmap(
        part: &FuseBlockPartInfo,
    ) -> Option<databend_common_expression::types::Bitmap> {
        part.range().map(|range| {
            RowSelection::from_range(
                part.nums_rows,
                range.start.saturating_mul(part.page_size()),
                range.end.saturating_mul(part.page_size()),
            )
            .bitmap
        })
    }

    pub fn page_range_data_cache_key(&self, part: &FuseBlockPartInfo) -> Option<String> {
        let range = part.range()?;
        let root = self.operator.info().root();
        let mut key = String::with_capacity(root.len() + part.location.len() + 256);
        write!(
            key,
            "fuse-page-data-v1|{}:{}|{}:{}|{}|{}|{}|{}:{}",
            root.len(),
            root,
            part.location.len(),
            part.location,
            part.file_size,
            part.nums_rows,
            part.page_size(),
            range.start,
            range.end,
        )
        .ok()?;

        for (field, column_node) in self
            .projected_schema
            .fields
            .iter()
            .zip(self.project_column_nodes.iter())
        {
            if column_node.is_nested || column_node.leaf_column_ids.as_slice() != [field.column_id]
            {
                return None;
            }
            let column_meta = part.columns_meta.get(&field.column_id)?;
            let (offset, len) = column_meta.offset_length();
            let data_type: DataType = field.data_type().into();
            write!(
                key,
                "|{}:{}:{}:{:?}",
                field.column_id, offset, len, data_type
            )
            .ok()?;
        }
        Some(key)
    }

    pub fn cached_page_range_data(&self, key: &str) -> Option<DataBlock> {
        let cache = CacheManager::instance().get_table_data_cache();
        let cache_entry = cache.get(key)?;
        let data_block = cache_entry.as_data_block()?;
        let memory_size = cache_entry.memory_size();
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytesFromMemory, memory_size);
        self.ctx
            .get_data_cache_metrics()
            .add_cache_metrics(0, 0, memory_size);
        Some(data_block.clone())
    }

    pub fn cache_page_range_data(&self, key: String, data_block: DataBlock) -> DataBlock {
        if !self.put_cache {
            return data_block;
        }
        let Some(cache) = CacheManager::instance().get_table_data_cache() else {
            return data_block;
        };
        let cache_entry = cache.insert(key, TableDataCacheEntry::from_data_block(data_block));
        cache_entry.as_data_block().unwrap().clone()
    }

    pub fn deserialize_parquet_record_batch(
        &self,
        part: &FuseBlockPartInfo,
        record_batch: &RecordBatch,
    ) -> databend_common_exception::Result<DataBlock> {
        let result_rows = record_batch.num_rows();
        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let name_paths = column_name_paths(&self.projection, &self.original_schema);
        for (((i, field), column_node), name_path) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
            .zip(name_paths.iter())
        {
            let data_type = field.data_type().into();
            let exists = column_node
                .leaf_column_ids
                .iter()
                .any(|column_id| part.columns_meta.contains_key(column_id));
            let value = if exists {
                let arrow_array = column_by_name(record_batch, name_path);
                Value::from_arrow_rs(arrow_array, &data_type)?
            } else {
                Value::Scalar(self.default_vals[i].clone())
            };
            entries.push(BlockEntry::new(value, || (data_type, result_rows)));
        }
        Ok(DataBlock::new(entries, result_rows))
    }

    pub fn deserialize_parquet_chunks(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        let result_rows = selection.map(|s| s.selected_rows).unwrap_or(num_rows);
        // If projection is empty, return a DataBlock with the appropriate row count but no columns
        if self.projected_schema.fields.is_empty() {
            return Ok(DataBlock::empty_with_rows(result_rows));
        }

        if result_rows == 0 {
            return Ok(DataBlock::empty_with_schema(&self.data_schema()));
        }

        let has_selection = selection.is_some();
        let parquet_selection = selection.map(|s| s.selection.clone());
        let record_batch = column_chunks_to_record_batch(
            &self.original_schema,
            num_rows,
            &column_chunks,
            compression,
            parquet_selection,
        )?;
        let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
        let name_paths = column_name_paths(&self.projection, &self.original_schema);

        let array_cache = if self.put_cache && !has_selection {
            CacheManager::instance().get_table_data_cache()
        } else {
            None
        };

        for ((i, field), column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
        {
            let data_type = field.data_type().into();

            // NOTE, there is something tricky here:
            // - `column_chunks` always contains data of leaf columns
            // - here we may processing a nested type field
            // - But, even if the field being processed is a field with multiple leaf columns
            //    `column_chunks.get(&field.column_id)` will still return Some(DataItem::_)[^1],
            //    even if we are getting data from `column_chunks` using a non-leaf
            //    `column_id` of `projected_schema.fields`
            //
            //   [^1]: Except in the current block, there is no data stored for the
            //         corresponding field, and a default value has been declared for
            //         the corresponding field.
            //
            //  Yes, it is too obscure, we need to polish it later.

            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(_)) => {
                    // get the deserialized arrow array, which may be a nested array
                    let arrow_array = column_by_name(&record_batch, &name_paths[i]);
                    if !column_node.is_nested {
                        if let Some(cache) = &array_cache {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            let key =
                                TableDataCacheKey::new(block_path, field.column_id, offset, len);
                            let array_memory_size = arrow_array.get_array_memory_size();
                            cache.insert(
                                key.into(),
                                TableDataCacheEntry::from_column_array(
                                    arrow_array.clone(),
                                    array_memory_size,
                                ),
                            );
                        }
                    }
                    Value::from_arrow_rs(arrow_array, &data_type)?
                }
                Some(DataItem::ColumnArray(cached)) => {
                    if column_node.is_nested {
                        // a defensive check, should never happen
                        return Err(ErrorCode::StorageOther(
                            "unexpected nested field: nested leaf field hits cached",
                        ));
                    }
                    let cached_array = cached.as_column_array().ok_or_else(|| {
                        ErrorCode::StorageOther("unexpected data block entry in column array cache")
                    })?;
                    let mut value = Value::from_arrow_rs(cached_array.clone(), &data_type)?;
                    if let Some(selection) = selection {
                        let mut filter_visitor = FilterVisitor::new(&selection.bitmap);
                        filter_visitor.visit_value(value)?;
                        value = filter_visitor.take_result().unwrap();
                    }
                    value
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            entries.push(BlockEntry::new(value, || (data_type, result_rows)));
        }
        Ok(DataBlock::new(entries, result_rows))
    }
}

fn column_by_name(record_batch: &RecordBatch, names: &[String]) -> ArrayRef {
    let mut array = record_batch.column_by_name(&names[0]).unwrap().clone();
    if names.len() > 1 {
        for name in &names[1..] {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array = struct_array.column_by_name(name).unwrap().clone();
        }
    }
    array
}

// This function assumes that projection is valid, isn't responsible for checking it.
fn column_name_paths(projection: &Projection, schema: &TableSchema) -> Vec<Vec<String>> {
    match projection {
        Projection::Columns(field_indices) => field_indices
            .iter()
            .map(|i| vec![schema.fields[*i].name().to_string()])
            .collect(),
        Projection::InnerColumns(path_indices) => {
            let mut name_paths = Vec::with_capacity(path_indices.len());
            for index_path in path_indices.values() {
                let mut name_path = Vec::with_capacity(index_path.len());
                let first_index = index_path[0];
                name_path.push(schema.fields[first_index].name().to_string());
                let mut idx = 1;
                let mut ty = schema.fields[first_index].data_type().clone();
                while idx < index_path.len() {
                    match ty.remove_nullable() {
                        TableDataType::Tuple {
                            fields_name,
                            fields_type,
                        } => {
                            let next_index = index_path[idx];
                            name_path.push(fields_name[next_index].clone());
                            ty = fields_type[next_index].clone();
                        }
                        _ => unreachable!(),
                    }
                    idx += 1;
                }
                name_paths.push(name_path);
            }
            name_paths
        }
    }
}
