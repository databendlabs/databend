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

use bytes::Bytes;
use databend_common_arrow::arrow::chunk::Chunk;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema_ignore_inside_nullable;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
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
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let arrow_schema =
            table_schema_to_arrow_schema_ignore_inside_nullable(&self.original_schema);
        let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;

        let column_id_to_dfs_id = self
            .original_schema
            .to_leaf_column_ids()
            .iter()
            .enumerate()
            .map(|(dfs_id, column_id)| (*column_id, dfs_id))
            .collect::<HashMap<_, _>>();

        let mut projection_mask = Vec::with_capacity(column_chunks.len());
        let compression = parquet_rs::basic::Compression::from(*compression);
        let mut dfs_id_to_column_chunks = HashMap::with_capacity(column_chunks.len());
        let mut dfs_id_to_column_chunk_metadatas = HashMap::with_capacity(column_chunks.len());

        for (column_id, data_item) in column_chunks.iter() {
            match data_item {
                DataItem::RawData(bytes) => {
                    let dfs_id = column_id_to_dfs_id.get(column_id).cloned().unwrap();
                    projection_mask.push(dfs_id);
                    dfs_id_to_column_chunks.insert(dfs_id, bytes.clone());
                    let column_meta = column_metas.get(column_id).unwrap().as_parquet().unwrap();
                    let column_chunk_metadata =
                        ColumnChunkMetaData::builder(parquet_schema.column(dfs_id))
                            .set_compression(compression)
                            .set_data_page_offset(0)
                            .set_total_compressed_size(column_meta.len as i64)
                            .build()?;
                    dfs_id_to_column_chunk_metadatas.insert(dfs_id, column_chunk_metadata);
                }
                DataItem::ColumnArray(_) => {}
            }
        }

        let row_group = Box::new(RowGroupImpl {
            num_rows,
            column_chunks: dfs_id_to_column_chunks,
            column_chunk_metadatas: dfs_id_to_column_chunk_metadatas,
        });

        let field_levels = parquet_to_arrow_field_levels(
            &parquet_schema,
            ProjectionMask::leaves(&parquet_schema, projection_mask),
            None,
        )?;
        let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &field_levels,
            row_group.as_ref(),
            num_rows,
            None,
        )?;
        let record = record_reader.next().unwrap()?;
        assert!(record_reader.next().is_none());

        let mut columns = Vec::with_capacity(self.projected_schema.fields.len());
        for (i, field) in self.projected_schema.fields.iter().enumerate() {
            let column_id = field.column_id;
            let data_type = field.data_type().into();
            let value = match column_chunks.get(&column_id) {
                Some(DataItem::RawData(_)) => {
                    todo!()
                }
                Some(DataItem::ColumnArray(cached)) => {
                    Value::Column(Column::from_arrow(cached.0.as_ref(), &data_type)?)
                }
                None => Value::Scalar(self.default_vals[i].clone()),
            };
            columns.push(BlockEntry::new(data_type, value));
        }

        // let mut new_deserialized_arrays = record.columns().iter();
        // let mut all_arrays = Vec::with_capacity(projected_table_schema.fields().len());
        // for field in projected_table_schema.fields().iter() {
        //     let Some(data_item) = column_chunks.get(&field.column_id) else {
        //         continue;
        //     };
        //     match data_item {
        //         DataItem::RawData(raw_data) => {
        //             let array = new_deserialized_arrays.next().unwrap();
        //             let arrow2_array =
        //                 <Box<dyn databend_common_arrow::arrow::array::Array>>::from(array.clone());
        //             all_arrays.push(arrow2_array.clone());
        //             if self.put_cache {
        //                 if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
        //                     let meta = column_metas.get(&field.column_id).unwrap();
        //                     let (offset, len) = meta.offset_length();
        //                     let key =
        //                         TableDataCacheKey::new(block_path, field.column_id, offset, len);
        //                     cache.put(key.into(), Arc::new((arrow2_array, raw_data.len())));
        //                 }
        //             }
        //         }
        //         DataItem::ColumnArray(array) => all_arrays.push(array.0.clone()),
        //     }
        // }
        // let chunk = Chunk::try_new(all_arrays)?;
        // let mut default_vals = Vec::with_capacity(self.default_vals.len());
        // for (i, field) in projected_table_schema.fields().iter().enumerate() {
        //     let data_item = column_chunks.get(&field.column_id);
        //     match data_item {
        //         Some(_) => default_vals.push(None),
        //         None => default_vals.push(Some(self.default_vals[i].clone())),
        //     }
        // }
        // let data_block = DataBlock::create_with_default_value_and_chunk(
        //     &self.data_schema(),
        //     &chunk,
        //     &default_vals,
        //     num_rows,
        // )?;
        // Ok(data_block)
        todo!()
    }
}

// A single row group
pub struct RowGroupImpl {
    pub num_rows: usize,
    pub column_chunks: HashMap<usize, Bytes>,
    pub column_chunk_metadatas: HashMap<usize, ColumnChunkMetaData>,
}

impl RowGroups for RowGroupImpl {
    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns a [`PageIterator`] for the column chunk with the given leaf column index
    fn column_chunks(&self, i: usize) -> ParquetResult<Box<dyn PageIterator>> {
        let column_chunk = Arc::new(self.column_chunks.get(&i).unwrap().clone());
        let column_chunk_meta = self.column_chunk_metadatas.get(&i).unwrap();
        let page_reader = Box::new(SerializedPageReader::new(
            column_chunk,
            &column_chunk_meta,
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
