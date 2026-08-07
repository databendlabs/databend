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

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterVisitor;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::visitor::ValueVisitor;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
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
            None,
        )
    }

    pub fn deserialize_part_with_filter_hash(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
        filter_hash: Option<u64>,
    ) -> databend_common_exception::Result<DataBlock> {
        self.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            column_chunks,
            &part.compression,
            &part.location,
            selection,
            filter_hash,
        )
    }

    pub fn deserialize_parquet_chunks(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
        selection: Option<&RowSelection>,
        filter_hash: Option<u64>,
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
        let array_cache = if self.put_cache {
            CacheManager::instance().get_table_data_array_cache()
        } else {
            None
        };

        // When selection is active and filter_hash is provided, try predicate-keyed cache.
        // If all RawData columns hit, we can skip parquet decompression entirely.
        if has_selection && filter_hash.is_some() {
            if let Some(cache) = &array_cache {
                let fh = filter_hash.unwrap();
                let mut cached_entries: Vec<Option<(usize, ArrayRef)>> =
                    vec![None; self.projected_schema.fields.len()];
                let mut all_hit = true;

                for (i, (field, column_node)) in self
                    .projected_schema
                    .fields
                    .iter()
                    .zip(self.project_column_nodes.iter())
                    .enumerate()
                {
                    match column_chunks.get(&field.column_id) {
                        Some(DataItem::RawData(_)) if !column_node.is_nested => {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            let key = TableDataCacheKey::new_with_filter(
                                block_path,
                                field.column_id,
                                offset,
                                len,
                                fh,
                            );
                            if let Some(cached) = cache.get(key.as_ref()) {
                                cached_entries[i] = Some((i, cached.0.clone()));
                            } else {
                                all_hit = false;
                                break;
                            }
                        }
                        Some(DataItem::RawData(_)) => {
                            // Nested RawData columns can't be cached individually
                            all_hit = false;
                            break;
                        }
                        _ => {}
                    }
                }

                if all_hit {
                    let mut entries = Vec::with_capacity(self.projected_schema.fields.len());
                    for ((i, field), _column_node) in self
                        .projected_schema
                        .fields
                        .iter()
                        .enumerate()
                        .zip(self.project_column_nodes.iter())
                    {
                        let data_type = field.data_type().into();
                        let value = if let Some((_, arr)) = cached_entries[i].take() {
                            Value::from_arrow_rs(arr, &data_type)?
                        } else {
                            match column_chunks.get(&field.column_id) {
                                Some(DataItem::ColumnArray(cached)) => {
                                    let mut value =
                                        Value::from_arrow_rs(cached.0.clone(), &data_type)?;
                                    if let Some(sel) = selection {
                                        let mut filter_visitor = FilterVisitor::new(&sel.bitmap);
                                        filter_visitor.visit_value(value)?;
                                        value = filter_visitor.take_result().unwrap();
                                    }
                                    value
                                }
                                None => Value::Scalar(self.default_vals[i].clone()),
                                _ => unreachable!(),
                            }
                        };
                        entries.push(BlockEntry::new(value, || (data_type, result_rows)));
                    }
                    return Ok(DataBlock::new(entries, result_rows));
                }
            }
        }

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

        for ((i, field), column_node) in self
            .projected_schema
            .fields
            .iter()
            .enumerate()
            .zip(self.project_column_nodes.iter())
        {
            let data_type = field.data_type().into();

            let value = match column_chunks.get(&field.column_id) {
                Some(DataItem::RawData(_)) => {
                    let arrow_array = column_by_name(&record_batch, &name_paths[i]);
                    if !column_node.is_nested {
                        if let Some(cache) = &array_cache {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            if let Some(fh) = filter_hash {
                                // Cache filtered array with predicate key
                                let key = TableDataCacheKey::new_with_filter(
                                    block_path,
                                    field.column_id,
                                    offset,
                                    len,
                                    fh,
                                );
                                let array_memory_size = arrow_array.get_array_memory_size();
                                cache.insert(key.into(), (arrow_array.clone(), array_memory_size));
                            } else if !has_selection {
                                // Cache full array with base key (only when no selection)
                                let key = TableDataCacheKey::new(
                                    block_path,
                                    field.column_id,
                                    offset,
                                    len,
                                );
                                let array_memory_size = arrow_array.get_array_memory_size();
                                cache.insert(key.into(), (arrow_array.clone(), array_memory_size));
                            }
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
                    let mut value = Value::from_arrow_rs(cached.0.clone(), &data_type)?;
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
