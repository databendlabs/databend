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
        )
    }

    pub fn deserialize_part_with_page_range(
        &self,
        part: &FuseBlockPartInfo,
        column_chunks: HashMap<ColumnId, DataItem>,
        selection: Option<&RowSelection>,
    ) -> databend_common_exception::Result<DataBlock> {
        let page_selection = page_range_selection(part);
        let selection = merge_row_selection(page_selection.as_ref(), selection);
        self.deserialize_part(part, column_chunks, selection.as_ref())
    }

    pub fn page_range_selection(part: &FuseBlockPartInfo) -> Option<RowSelection> {
        page_range_selection(part)
    }

    pub fn merge_row_selection(
        page_selection: Option<&RowSelection>,
        row_selection: Option<&RowSelection>,
    ) -> Option<RowSelection> {
        merge_row_selection(page_selection, row_selection)
    }

    pub fn page_range_bitmap(
        part: &FuseBlockPartInfo,
    ) -> Option<databend_common_expression::types::Bitmap> {
        page_range_selection(part).map(|selection| selection.bitmap)
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
            CacheManager::instance().get_table_data_array_cache()
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
                            cache.insert(key.into(), (arrow_array.clone(), array_memory_size));
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

fn page_range_selection(part: &FuseBlockPartInfo) -> Option<RowSelection> {
    let page_size = part.page_size();
    if page_size == 0 || page_size >= part.nums_rows {
        return None;
    }

    part.range().map(|range| {
        RowSelection::from_range(
            part.nums_rows,
            range.start.saturating_mul(page_size),
            range.end.saturating_mul(page_size),
        )
    })
}

fn merge_row_selection(
    page_selection: Option<&RowSelection>,
    row_selection: Option<&RowSelection>,
) -> Option<RowSelection> {
    match (page_selection, row_selection) {
        (None, None) => None,
        (Some(page_selection), None) => Some(page_selection.clone()),
        (None, Some(row_selection)) => Some(row_selection.clone()),
        (Some(page_selection), Some(row_selection)) => {
            let bitmap = &page_selection.bitmap & &row_selection.bitmap;
            Some(RowSelection::from(&bitmap))
        }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_storages_common_pruner::BlockMetaIndex;

    use super::*;

    fn part(nums_rows: usize, page_size: usize) -> FuseBlockPartInfo {
        FuseBlockPartInfo {
            location: "test.parquet".to_string(),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            spatial_index_location: None,
            spatial_index_size: 0,
            create_on: None,
            nums_rows,
            columns_meta: HashMap::new(),
            columns_stat: None,
            spatial_stats: None,
            compression: Compression::Lz4,
            sort_min_max: None,
            cluster_stats: None,
            preserve_order_stream: None,
            block_meta_index: Some(BlockMetaIndex {
                range: Some(1..2),
                page_size,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_page_range_selection_ignores_parquet_full_block_page_size() {
        let part = part(10, 10);

        assert!(BlockReader::page_range_selection(&part).is_none());
    }

    #[test]
    fn test_page_range_selection_uses_native_page_size() {
        let part = part(10, 3);

        let selection = BlockReader::page_range_selection(&part).unwrap();

        assert_eq!(selection.selected_rows, 3);
        assert_eq!(selection.bitmap.len(), 10);
        for row in 0..10 {
            assert_eq!(selection.bitmap.get_bit(row), (3..6).contains(&row));
        }
    }
}
