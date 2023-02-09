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
        let chunks = fetched
            .columns_chunks()?
            .into_iter()
            .map(|(column_idx, column_chunk)| (column_idx, column_chunk))
            .collect::<Vec<_>>();

        let num_rows = meta.row_count as usize;
        let columns_chunk = chunks
            .into_iter()
            .map(|(index, chunk)| (index, chunk))
            .collect::<Vec<_>>();

        self.deserialize_parquet_chunks_with_buffer(
            &meta.location.0,
            num_rows,
            &meta.compression,
            &columns_meta,
            columns_chunk,
            None,
        )
    }

    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_parquet_chunks(
        &self,
        part: PartInfoPtr,
        chunks: Vec<(ColumnId, DataItem)>,
    ) -> Result<DataBlock> {
        let part = FusePartInfo::from_part(&part)?;
        let start = Instant::now();

        if chunks.is_empty() {
            return Ok(DataBlock::new(vec![], part.nums_rows));
        }

        let reads = chunks
            .into_iter()
            .map(|(index, chunk)| (index, chunk))
            .collect::<Vec<_>>();

        let deserialized_res = self.deserialize_parquet_chunks_with_buffer(
            &part.location,
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            reads,
            None,
        );

        // Perf.
        {
            metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        }

        deserialized_res
    }

    /// Deserialize column chunks data from parquet format to DataBlock with a uncompressed buffer.
    pub fn deserialize_parquet_chunks_with_buffer(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        columns_chunks: Vec<(ColumnId, DataItem)>,
        uncompressed_buffer: Option<Arc<UncompressedBuffer>>,
    ) -> Result<DataBlock> {
        if columns_chunks.is_empty() {
            return Ok(DataBlock::new(vec![], num_rows));
        }

        let chunk_map: HashMap<ColumnId, DataItem> = columns_chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let columns = self.projection.project_column_nodes(&self.column_nodes)?;

        // TODO need refactor
        type ItemIndex = usize;
        enum Holder {
            Cached(ItemIndex),
            Deserialized(ItemIndex),
            DeserializedNoCache(ItemIndex),
        }

        let mut column_idx_cached_array = vec![];
        let mut holders = vec![];
        let mut deserialized_item_index: usize = 0;

        for column in &columns {
            let field = column.field.clone();
            let indices = &column.leaf_ids;
            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            if indices.len() == 1 {
                eprintln!("non nestted filed");
                // we only cache non-nested column for the time being
                let column_id = column.leaf_column_id(0);
                let index = indices[0];
                if let Some(column_meta) = columns_meta.get(&column_id) {
                    if let Some(chunk) = chunk_map.get(&column_id) {
                        match chunk {
                            DataItem::RawData(data) => {
                                // TODO why index of type usize is used here? need @LiChuang review
                                let column_descriptor =
                                    &self.parquet_schema_descriptor.columns()[index];
                                column_metas.push(column_meta);
                                column_chunks.push(*data);
                                column_descriptors.push(column_descriptor);
                                holders.push(Holder::Deserialized(deserialized_item_index));
                                deserialized_item_index += 1;
                            }
                            DataItem::ColumnArray(column_array) => {
                                let idx = column_idx_cached_array.len();
                                column_idx_cached_array.push(*column_array);
                                holders.push(Holder::Cached(idx));
                            }
                        }
                    }
                }
            } else {
                eprintln!("nestted filed");
                for (i, index) in indices.iter().enumerate() {
                    let column_id = column.leaf_column_id(i);
                    if let Some(column_meta) = columns_meta.get(&column_id) {
                        if let Some(chunk) = chunk_map.get(&column_id) {
                            match chunk {
                                DataItem::RawData(data) => {
                                    // TODO why index is used here? need @LiChuang review
                                    let column_descriptor =
                                        &self.parquet_schema_descriptor.columns()[*index];
                                    column_metas.push(column_meta);
                                    column_chunks.push(*data);
                                    column_descriptors.push(column_descriptor);
                                }
                                DataItem::ColumnArray(column_array) => {
                                    eprintln!("using cache in nested");
                                    let idx = column_idx_cached_array.len();
                                    column_idx_cached_array.push(*column_array);
                                    holders.push(Holder::Cached(idx));
                                }
                            }
                        }
                    }
                }
                // a field nested structure is expected
                holders.push(Holder::DeserializedNoCache(deserialized_item_index));
                deserialized_item_index += 1;
            }

            if column_metas.len() > 0 {
                columns_array_iter.push(Self::chunks_to_parquet_array_iter(
                    column_metas,
                    column_chunks,
                    // test_chunks,
                    num_rows,
                    column_descriptors,
                    field,
                    compression,
                    uncompressed_buffer
                        .clone()
                        .unwrap_or_else(|| UncompressedBuffer::new(0)),
                )?);
            }
        }

        let mut arrays = Vec::with_capacity(columns_array_iter.len());

        let deserialized_column_arrays = columns_array_iter
            .into_iter()
            .map(|mut v|
                // TODO error handling
                     v.next().unwrap().unwrap())
            .collect::<Vec<_>>();

        for holder in holders {
            match holder {
                Holder::Cached(idx) => {
                    arrays.push(column_idx_cached_array[idx].as_ref());
                }
                Holder::Deserialized(idx) => {
                    let item = &deserialized_column_arrays[idx];
                    arrays.push(item);
                }
                Holder::DeserializedNoCache(idx) => {
                    let item = &deserialized_column_arrays[idx];
                    arrays.push(item);
                }
            }
        }
        let chunk = Chunk::try_new(arrays)?;
        let block = DataBlock::from_arrow_chunk(&chunk, &self.data_schema());

        // if block.is_ok() {
        //    let maybe_column_array_cache = CacheManager::instance().get_table_data_array_cache();
        //    if let Some(cache) = maybe_column_array_cache {
        //        for (idx, array) in deserialized_column_arrays.into_iter() {
        //            let key = TableDataColumnCacheKey::new(block_path, idx as ColumnId);
        //            cache.put(key.into(), array.into());
        //        }
        //    }
        //}

        block
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
