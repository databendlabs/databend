// Copyright 2021 Datafuse Labs.
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
use std::time::Instant;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::parquet::compression::Compression as ParquetCompression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table::ColumnId;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use storages_common_cache::CacheAccessor;
use storages_common_cache::TableDataColumnCacheKey;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Compression;

use crate::fuse_part::FusePartInfo;
use crate::io::read::block::block_reader::DataItem;
use crate::io::read::block::decompressor::BuffedBasicDecompressor;
use crate::io::read::ReadSettings;
use crate::io::BlockReader;
use crate::io::UncompressedBuffer;
use crate::metrics::*;

impl BlockReader {
    /// Read a parquet file and convert to DataBlock.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_parquet_by_meta(
        &self,
        settings: &ReadSettings,
        meta: &BlockMeta,
    ) -> Result<DataBlock> {
        //  Build columns meta.
        let columns_meta = meta
            .col_metas
            .iter()
            .map(|(column_id, meta)| (*column_id, meta.clone()))
            .collect::<HashMap<_, _>>();

        // Get the merged IO read result.
        let fetched = self
            .read_columns_data_by_merge_io(settings, &meta.location.0, &columns_meta)
            .await?;

        // Get the columns chunk.
        let column_chunks = fetched.columns_chunks()?;

        let num_rows = meta.row_count as usize;

        self.deserialize_parquet_chunks_with_buffer(
            &meta.location.0,
            num_rows,
            &meta.compression,
            &columns_meta,
            column_chunks,
            None,
        )
    }

    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_parquet_chunks(
        &self,
        part: PartInfoPtr,
        chunks: HashMap<ColumnId, DataItem>,
    ) -> Result<DataBlock> {
        let part = FusePartInfo::from_part(&part)?;
        let start = Instant::now();

        if chunks.is_empty() {
            return Ok(DataBlock::new(vec![], part.nums_rows));
        }

        let deserialized_res = self.deserialize_parquet_chunks_with_buffer(
            &part.location,
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            chunks,
            None,
        );

        // Perf.
        {
            metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        }

        deserialized_res
    }

    pub fn build_default_values_block(&self, num_rows: usize) -> Result<DataBlock> {
        let data_schema = self.data_schema();
        let default_vals = self.default_vals.clone();
        DataBlock::create_with_default_value(&data_schema, &default_vals, num_rows)
    }

    /// Deserialize column chunks data from parquet format to DataBlock with a uncompressed buffer.
    pub fn deserialize_parquet_chunks_with_buffer(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        uncompressed_buffer: Option<Arc<UncompressedBuffer>>,
    ) -> Result<DataBlock> {
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let mut deserialized_column_arrays = Vec::with_capacity(self.projection.len());

        let fields = self.projection.project_column_nodes(&self.column_nodes)?;
        let mut need_default_vals = Vec::with_capacity(fields.len());
        let mut need_to_fill_default_val = false;

        // TODO need refactor
        type ItemIndex = usize;
        enum Holder {
            Cached(ItemIndex),
            Deserialized(ItemIndex),
        }

        let mut column_idx_cached_array = vec![];
        let mut holders = vec![];
        let mut deserialized_item_index: usize = 0;
        let mut cache_flags = vec![];

        for column in &fields {
            let field = column.field.clone();
            let indices = &column.leaf_ids;
            let mut field_column_metas = Vec::with_capacity(indices.len());
            let mut field_column_data = Vec::with_capacity(indices.len());
            let mut field_column_descriptors = Vec::with_capacity(indices.len());
            let mut column_in_block = false;
            let mut field_uncompressed_size = 0;
            for (i, index) in indices.iter().enumerate() {
                let column_id = column.leaf_column_ids[i];
                if let Some(column_meta) = column_metas.get(&column_id) {
                    // TODO need @LiChuang review
                    if let Some(chunk) = column_chunks.get(&(*index as ColumnId)) {
                        column_in_block = true;
                        match chunk {
                            DataItem::RawData(data) => {
                                let column_descriptor =
                                    &self.parquet_schema_descriptor.columns()[*index];
                                field_column_metas.push(column_meta);
                                field_column_data.push(*data);
                                field_column_descriptors.push(column_descriptor);
                                field_uncompressed_size += data.len();
                            }
                            DataItem::ColumnArray(column_array) => {
                                let idx = column_idx_cached_array.len();
                                column_idx_cached_array.push(*column_array);
                                field_uncompressed_size += column_array.1;
                                holders.push(Holder::Cached(idx));
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    column_in_block = false;
                    break;
                }
            }

            if field_column_metas.len() == 0 {
                continue;
            }

            if column_in_block {
                if field_column_metas.len() > 1 {
                    eprintln!("nested");
                    // working on nested field
                    holders.push(Holder::Deserialized(deserialized_item_index));
                    cache_flags.push(None);
                } else {
                    eprintln!("simple");
                    // working on simple field
                    let column_idx = indices[0] as ColumnId;
                    holders.push(Holder::Deserialized(deserialized_item_index));
                    cache_flags.push(Some(column_idx));
                }

                deserialized_item_index += 1;

                let field_name = field.name.clone();
                let mut array_iter = Self::chunks_to_parquet_array_iter(
                    field_column_metas,
                    field_column_data,
                    num_rows,
                    field_column_descriptors,
                    field,
                    compression,
                    uncompressed_buffer
                        .clone()
                        .unwrap_or_else(|| UncompressedBuffer::new(0)),
                )?;
                let array = array_iter.next().transpose()?.ok_or_else(|| {
                    ErrorCode::StorageOther(format!(
                        "unexpected deserialization error, no array found for field {field_name} "
                    ))
                })?;
                deserialized_column_arrays.push((field_uncompressed_size, array));

                need_default_vals.push(false);
            } else {
                need_default_vals.push(true);
                need_to_fill_default_val = true;
            }
        }

        let mut arrays = Vec::with_capacity(self.projection.len());

        // assembly the arrays
        for holder in holders {
            match holder {
                Holder::Cached(idx) => {
                    arrays.push(&column_idx_cached_array[idx].as_ref().0);
                }
                Holder::Deserialized(idx) => {
                    arrays.push(&deserialized_column_arrays[idx].1);
                }
            }
        }
        let chunk = Chunk::try_new(arrays)?;

        let data_block = if !need_to_fill_default_val {
            DataBlock::from_arrow_chunk(&chunk, &self.data_schema())
        } else {
            let data_schema = self.data_schema();
            let schema_default_vals = self.default_vals.clone();
            let mut default_vals = Vec::with_capacity(need_default_vals.len());
            for (i, need_default_val) in need_default_vals.iter().enumerate() {
                if !need_default_val {
                    default_vals.push(None);
                } else {
                    default_vals.push(Some(schema_default_vals[i].clone()));
                }
            }
            DataBlock::create_with_default_value_and_chunk(
                &data_schema,
                &chunk,
                &default_vals,
                num_rows,
            )
        };

        if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
            eprintln!("array cache enabeld");
            // populate array cache items
            for ((size, item), need_tobe_cached) in
                deserialized_column_arrays.into_iter().zip(cache_flags)
            {
                if let Some(column_idx) = need_tobe_cached {
                    let key = TableDataColumnCacheKey::new(block_path, column_idx);
                    eprintln!("array cache put {}", key.as_ref());
                    cache.put(key.into(), Arc::new((item, size)))
                }
            }
        }
        data_block
    }

    fn chunks_to_parquet_array_iter<'a>(
        metas: Vec<&ColumnMeta>,
        chunks: Vec<&'a [u8]>,
        rows: usize,
        column_descriptors: Vec<&ColumnDescriptor>,
        field: Field,
        compression: &Compression,
        uncompressed_buffer: Arc<UncompressedBuffer>,
    ) -> Result<ArrayIter<'a>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let meta = meta.as_parquet().unwrap();

                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: Self::to_parquet_compression(compression)?,
                    descriptor: column_descriptor.descriptor.clone(),
                };
                let pages = PageReader::new_with_page_meta(
                    chunk,
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );

                Ok(BuffedBasicDecompressor::new(
                    pages,
                    uncompressed_buffer.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.descriptor.primitive_type)
            .collect::<Vec<_>>();

        Ok(column_iter_to_arrays(
            columns,
            types,
            field,
            Some(rows),
            rows,
        )?)
    }

    fn to_parquet_compression(meta_compression: &Compression) -> Result<ParquetCompression> {
        match meta_compression {
            Compression::Lz4 => {
                let err_msg = r#"Deprecated compression algorithm [Lz4] detected.

                                        The Legacy compression algorithm [Lz4] is no longer supported.
                                        To migrate data from old format, please consider re-create the table,
                                        by using an old compatible version [v0.8.25-nightly … v0.7.12-nightly].

                                        - Bring up the compatible version of databend-query
                                        - re-create the table
                                           Suppose the name of table is T
                                            ~~~
                                            create table tmp_t as select * from T;
                                            drop table T all;
                                            alter table tmp_t rename to T;
                                            ~~~
                                        Please note that the history of table T WILL BE LOST.
                                       "#;
                Err(ErrorCode::StorageOther(err_msg))
            }
            Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
            Compression::Snappy => Ok(ParquetCompression::Snappy),
            Compression::Zstd => Ok(ParquetCompression::Zstd),
            Compression::Gzip => Ok(ParquetCompression::Gzip),
            Compression::None => Ok(ParquetCompression::Uncompressed),
        }
    }
}
