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
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::ColumnArrayCache;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::basic::Compression as ParquetCompression;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::adapter::RowGroupImplBuilder;

/// Cache admission for decoded physical columns. GranuleData carries its own
/// byte range; complete raw chunks use the column metadata instead.
pub(crate) struct ArrayCacheContext<'a> {
    pub cache: &'a ColumnArrayCache,
    pub location: &'a str,
    pub column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub complete_column_chunks: bool,
}

impl ArrayCacheContext<'_> {
    fn insert(&self, field: &TableField, item: &DataItem, array: &arrow_array::ArrayRef) {
        let column_id = field.column_id;
        let data_type = DataType::from(field.data_type()).to_string();
        let range = match item {
            DataItem::GranuleData(_, range) => Some(range.clone()),
            DataItem::RawData(_) if self.complete_column_chunks => {
                self.column_metas.get(&column_id).map(|meta| {
                    let (offset, len) = meta.offset_length();
                    offset..offset + len
                })
            }
            _ => None,
        };
        let Some(range) = range else {
            return;
        };
        let key = TableDataCacheKey::new(
            self.location,
            column_id,
            range.start,
            range.end - range.start,
            &data_type,
        );
        self.cache
            .insert(key.into(), (array.clone(), array.get_array_memory_size()));
    }
}

/// Decode cache misses, merge cached physical arrays and apply the same row
/// selection to both. Nested fields are decoded together, never cached as leaves.
/// Query-specific filtering and virtual-path evaluation must not enter the cache.
pub(crate) fn deserialize_column_chunks(
    schema: &TableSchema,
    num_rows: usize,
    chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    selection: Option<RowSelection>,
    cache: Option<ArrayCacheContext<'_>>,
) -> Result<RecordBatch> {
    let result_rows = selection.as_ref().map_or(num_rows, |s| s.row_count());
    // A cached leaf of a nested field cannot reconstruct its parent's validity
    // and repetition levels. Use the same rule for ordinary and virtual columns.
    let arrow_schema = Schema::from(schema);
    let mut scalar_ids = std::collections::HashSet::new();
    for (field, arrow_field) in schema.fields().iter().zip(arrow_schema.fields()) {
        if !arrow_field.data_type().is_nested() {
            scalar_ids.insert(field.column_id);
        }
    }
    for (id, item) in chunks {
        if matches!(item, DataItem::ColumnArray(_)) && !scalar_ids.contains(id) {
            return Err(ErrorCode::StorageOther("unexpected cached nested leaf"));
        }
    }
    let decoded =
        column_chunks_to_record_batch(schema, num_rows, chunks, compression, selection.clone())?;
    let mut filter = None;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (field, arrow_field) in schema.fields().iter().zip(arrow_schema.fields()) {
        let item = chunks.get(&field.column_id);
        let array = match item {
            Some(DataItem::ColumnArray(cached)) => {
                if cached.0.len() != num_rows || cached.0.data_type() != arrow_field.data_type() {
                    return Err(ErrorCode::StorageOther(
                        "cached array does not match physical column",
                    ));
                }
                match &selection {
                    Some(selection) => {
                        let filter = filter.get_or_insert_with(|| selection_filter(selection));
                        arrow::compute::filter(cached.0.as_ref(), filter)?
                    }
                    None => cached.0.clone(),
                }
            }
            _ => {
                let Some(array) = decoded.column_by_name(field.name()) else {
                    continue;
                };
                // Preserve the projected nested schema (inner projections may
                // contain only some of a struct's children).
                if !arrow_field.data_type().is_nested()
                    && selection.is_none()
                    && array.len() == num_rows
                    && let (Some(context), Some(item)) = (&cache, item)
                {
                    context.insert(field, item, array);
                }
                array.clone()
            }
        };
        let data_type = array.data_type().clone();
        let field = arrow_field.as_ref().clone().with_data_type(data_type);
        fields.push(Arc::new(field));
        arrays.push(array);
    }
    let metadata = decoded.schema().metadata().clone();
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let options = RecordBatchOptions::new().with_row_count(Some(result_rows));
    Ok(RecordBatch::try_new_with_options(schema, arrays, &options)?)
}

fn selection_filter(selection: &RowSelection) -> BooleanArray {
    let mut values = Vec::with_capacity(selection.row_count() + selection.skipped_row_count());
    for selector in selection.iter() {
        values.resize(values.len() + selector.row_count, !selector.skip);
    }
    BooleanArray::from(values)
}

/// The returned record batch contains all deserialized columns in the same nested structure as the original schema.
pub fn column_chunks_to_record_batch(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    selection: Option<RowSelection>,
) -> databend_common_exception::Result<RecordBatch> {
    let arrow_schema = Schema::from(original_schema);
    let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema)?;

    let column_id_to_dfs_id = original_schema
        .to_leaf_column_ids()
        .iter()
        .enumerate()
        .map(|(dfs_id, column_id)| (*column_id, dfs_id))
        .collect::<HashMap<_, _>>();
    let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        &parquet_schema,
        ParquetCompression::from(*compression),
    );
    for (column_id, data_item) in column_chunks.iter() {
        match data_item {
            DataItem::RawData(bytes) | DataItem::GranuleData(bytes, _) => {
                let dfs_id = column_id_to_dfs_id.get(column_id).cloned().unwrap();
                projection_mask.push(dfs_id);
                builder.add_column_chunk(dfs_id, bytes.clone());
            }
            DataItem::ColumnArray(_) => {}
        }
    }
    projection_mask.sort_unstable();
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        &parquet_schema,
        ProjectionMask::leaves(&parquet_schema, projection_mask),
        Some(arrow_schema.fields()),
    )?;
    let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &field_levels,
        row_group.as_ref(),
        num_rows,
        selection,
    )?;
    let record = match record_reader.next() {
        Some(record) => record?,
        // The reader yields nothing when `selection` keeps no rows, e.g. a prewhere
        // predicate filtered out every row of the block while the block itself
        // survived min/max pruning. Return an empty batch with the projected schema
        // so callers can build zero-row columns without special casing.
        None => RecordBatch::new_empty(record_reader.schema()),
    };
    assert!(record_reader.next().is_none());
    Ok(record)
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrayRef;
    use arrow_array::Int32Array;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_storages_common_blocks::blocks_to_parquet;
    use databend_storages_common_cache::InMemoryLruCache;
    use databend_storages_common_table_meta::table::TableCompression;
    use opendal::Buffer;
    use parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    #[test]
    fn test_decode_cold_mixed_and_cached_with_selection() {
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            Int32Type::from_data(vec![10, 20, 30]),
        ]);
        let parquet =
            blocks_to_parquet(&schema, vec![block], TableCompression::None, false, None).unwrap();
        let bytes = Buffer::from(parquet.payload);
        let mut chunks = HashMap::new();
        let mut keys = Vec::new();
        for (id, chunk) in parquet.metadata.row_group(0).columns().iter().enumerate() {
            let (offset, len) = chunk.byte_range();
            chunks.insert(
                id as u32,
                DataItem::GranuleData(
                    bytes.slice(offset as usize..(offset + len) as usize),
                    offset..offset + len,
                ),
            );
            keys.push(TableDataCacheKey::new(
                "virtual-column-file-1",
                id as u32,
                offset,
                len,
                "Int32",
            ));
        }
        let cache = InMemoryLruCache::with_bytes_capacity("decode-test".to_string(), 1024 * 1024);
        let metas = HashMap::new();
        let context = || ArrayCacheContext {
            cache: &cache,
            location: "virtual-column-file-1",
            column_metas: &metas,
            complete_column_chunks: false,
        };
        let cold = deserialize_column_chunks(
            &schema,
            3,
            &chunks,
            &Compression::None,
            None,
            Some(context()),
        )
        .unwrap();
        let a = cache.get(&keys[0]).unwrap();
        let b = cache.get(&keys[1]).unwrap();
        assert!(Arc::ptr_eq(cold.column(0), &a.0));
        let selection = RowSelection::from(vec![RowSelector::skip(1), RowSelector::select(2)]);
        let expected = cold.slice(1, 2);
        for cached_b in [false, true] {
            let mut mixed = HashMap::from([(0, DataItem::ColumnArray(&a))]);
            mixed.insert(
                1,
                if cached_b {
                    DataItem::ColumnArray(&b)
                } else {
                    chunks[&1].clone()
                },
            );
            let actual = deserialize_column_chunks(
                &schema,
                3,
                &mixed,
                &Compression::None,
                Some(selection.clone()),
                Some(context()),
            )
            .unwrap();
            assert_eq!(actual, expected);
            let empty = deserialize_column_chunks(
                &schema,
                3,
                &mixed,
                &Compression::None,
                Some(RowSelection::from(vec![RowSelector::skip(3)])),
                Some(context()),
            )
            .unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.num_columns(), 2);
        }
        assert_eq!(cache.get(&keys[0]).unwrap().0.len(), 3);
        assert_eq!(cache.get(&keys[1]).unwrap().0.len(), 3);
        let hot = HashMap::from([
            (0, DataItem::ColumnArray(&a)),
            (1, DataItem::ColumnArray(&b)),
        ]);
        let result =
            deserialize_column_chunks(&schema, 3, &hot, &Compression::None, None, None).unwrap();
        assert!(Arc::ptr_eq(result.column(0), &a.0));
        assert_eq!(result, cold);

        // Filtered cold arrays must never be admitted under an unfiltered range key.
        let filtered_cache = InMemoryLruCache::with_bytes_capacity("filtered".to_string(), 1024);
        deserialize_column_chunks(
            &schema,
            3,
            &chunks,
            &Compression::None,
            Some(selection),
            Some(ArrayCacheContext {
                cache: &filtered_cache,
                location: "virtual-column-file-1",
                column_metas: &metas,
                complete_column_chunks: false,
            }),
        )
        .unwrap();
        assert!(filtered_cache.get(&keys[0]).is_none());
        for (id, item) in &chunks {
            let DataItem::GranuleData(_, range) = item else {
                unreachable!()
            };
            assert!(
                cache
                    .get(TableDataCacheKey::new(
                        "virtual-column-file-2",
                        *id,
                        range.start,
                        range.end - range.start,
                        "Int32",
                    ))
                    .is_none()
            );
        }
    }

    #[test]
    fn test_rejects_cached_nested_leaf() {
        let schema = TableSchema::new(vec![TableField::new(
            "list",
            TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::Int32))),
        )]);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let cached = Arc::new((array, 8));
        let chunks = HashMap::from([(0, DataItem::ColumnArray(&cached))]);
        assert!(
            deserialize_column_chunks(&schema, 2, &chunks, &Compression::None, None, None).is_err()
        );
    }
}
