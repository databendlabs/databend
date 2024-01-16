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

use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_cache_manager::CacheManager;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet_rs::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet_rs::arrow::arrow_reader::RowGroups;
use parquet_rs::arrow::arrow_to_parquet_schema;
use parquet_rs::arrow::parquet_to_arrow_field_levels;
use parquet_rs::arrow::ProjectionMask;
use parquet_rs::column::page::PageIterator;
use parquet_rs::column::page::PageReader;
use parquet_rs::errors::Result as ParquetResult;
use parquet_rs::file::metadata::ColumnChunkMetaData;
use parquet_rs::file::serialized_reader::SerializedPageReader;

use super::block_reader_merge_io::DataItem;
use super::BlockReader;
impl BlockReader {
    pub(crate) fn deserialize_column_chunks_1(
        &self,
        num_rows: usize,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        compression: &Compression,
        block_path: &str,
    ) -> databend_common_exception::Result<DataBlock> {
        // 1. Filter fields that need to be deserialized, use these fields to create a parquet schema
        let projected_table_schema = self.projected_schema.clone();
        let mut need_deserialize_fields = Vec::new();
        for field in projected_table_schema.fields().iter() {
            let data_item = column_chunks.get(&field.column_id).unwrap();
            match data_item {
                DataItem::RawData(_) => need_deserialize_fields.push(field.clone()),
                DataItem::ColumnArray(_) => {}
            }
        }
        let need_deserialize_schema = TableSchema {
            fields: need_deserialize_fields,
            metadata: projected_table_schema.metadata.clone(),
            next_column_id: projected_table_schema.next_column_id,
        };
        let arrow_schema = Schema::from(&need_deserialize_schema);
        let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;
        // 2. Reorder the column chunks and column metas in dfs order according to the filtered schema
        let column_ids = need_deserialize_schema.to_leaf_column_ids();
        let reordered_column_chunks = column_ids
            .iter()
            .map(|id| {
                let data_item = column_chunks.get(id).unwrap();
                match data_item {
                    DataItem::RawData(data) => data.clone(),
                    DataItem::ColumnArray(_) => unreachable!(),
                }
            })
            .collect::<Vec<_>>();
        let reordered_column_metas = column_ids
            .iter()
            .map(|id| column_metas.get(id).unwrap().as_parquet().unwrap().clone())
            .collect::<Vec<_>>();
        // 3. Create RowGroupImpl, which is an adapter between parquet-rs and Fuse engine
        let mut column_chunk_metadatas = Vec::with_capacity(column_ids.len());
        let compression = parquet_rs::basic::Compression::from(*compression);
        for (column_meta, column_descr) in
            reordered_column_metas.iter().zip(parquet_schema.columns())
        {
            let column_chunk_metadata = ColumnChunkMetaData::builder(column_descr.clone())
                .set_compression(compression)
                .set_data_page_offset(0)
                .set_total_compressed_size(column_meta.len as i64)
                .build()?;
            column_chunk_metadatas.push(column_chunk_metadata);
        }

        let row_group = Box::new(RowGroupImpl {
            num_rows,
            column_chunks: reordered_column_chunks,
            column_chunk_metadatas,
        });
        // 4. call parquet-rs API to deserialize
        let field_levels =
            parquet_to_arrow_field_levels(&parquet_schema, ProjectionMask::all(), None)?;
        let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &field_levels,
            row_group.as_ref(),
            num_rows,
            None,
        )?;
        let record = record_reader.next().unwrap()?;
        assert!(record_reader.next().is_none());
        // 5. convert to DataBlock, and put the deserialized array into the cache if necessary
        let mut new_deserialized_arrays = record.columns().iter();
        let mut all_arrays = Vec::with_capacity(projected_table_schema.fields().len());
        for field in projected_table_schema.fields().iter() {
            let data_item = column_chunks.get(&field.column_id).unwrap();
            match data_item {
                DataItem::RawData(raw_data) => {
                    let array = new_deserialized_arrays.next().unwrap();
                    let arrow2_array =
                        <Box<dyn databend_common_arrow::arrow::array::Array>>::from(array.clone());
                    all_arrays.push(arrow2_array.clone());
                    if self.put_cache {
                        if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
                            let meta = column_metas.get(&field.column_id).unwrap();
                            let (offset, len) = meta.offset_length();
                            let key =
                                TableDataCacheKey::new(block_path, field.column_id, offset, len);
                            cache.put(key.into(), Arc::new((arrow2_array, raw_data.len())));
                        }
                    }
                }
                DataItem::ColumnArray(array) => all_arrays.push(array.0.clone()),
            }
        }
        let mut columns = Vec::with_capacity(all_arrays.len());
        for (array, field) in all_arrays.iter().zip(self.data_fields()) {
            columns.push(Column::from_arrow(array.as_ref(), field.data_type())?)
        }
        let data_block = DataBlock::new_from_columns(columns);
        Ok(data_block)
    }
}

// A single row group
pub struct RowGroupImpl {
    pub num_rows: usize,
    pub column_chunks: Vec<Bytes>,
    pub column_chunk_metadatas: Vec<ColumnChunkMetaData>,
}

impl RowGroups for RowGroupImpl {
    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns a [`PageIterator`] for the column chunk with the given leaf column index
    fn column_chunks(&self, i: usize) -> ParquetResult<Box<dyn PageIterator>> {
        let page_reader = Box::new(SerializedPageReader::new(
            Arc::new(self.column_chunks[i].clone()),
            &self.column_chunk_metadatas[i],
            self.num_rows(),
            None,
        )?);

        Ok(Box::new(PageIteratorImpl {
            reader: Some(Ok(page_reader)),
        }))
    }
}

struct PageIteratorImpl {
    reader: Option<ParquetResult<Box<dyn PageReader>>>,
}

impl Iterator for PageIteratorImpl {
    type Item = ParquetResult<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for PageIteratorImpl {}
