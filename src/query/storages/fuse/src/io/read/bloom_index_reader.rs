//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockBloomFilterIndex;
use common_fuse_meta::meta::BlockBloomFilterIndexVersion;
use common_fuse_meta::meta::Location;
use futures_util::future::try_join_all;
use opendal::Operator;
use tracing::Instrument;
pub use util_v1::load_bloom_filter_by_columns;

#[async_trait::async_trait]
pub trait BlockBloomFilterIndexReader {
    async fn read_bloom_filter_index(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockBloomFilterIndex>;
}

#[async_trait::async_trait]
impl BlockBloomFilterIndexReader for Location {
    async fn read_bloom_filter_index(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockBloomFilterIndex> {
        let index_version = BlockBloomFilterIndexVersion::try_from(self.1)?;
        match index_version {
            BlockBloomFilterIndexVersion::V1(_) => {
                let block =
                    load_bloom_filter_by_columns(ctx, dal, columns, &self.0, index_length).await?;
                Ok(BlockBloomFilterIndex::new(block))
            }
        }
    }
}

mod util_v1 {
    use std::future::Future;

    use common_base::base::GlobalIORuntime;
    use common_base::base::Runtime;
    use common_base::base::TrySpawn;
    use common_fuse_meta::caches::CacheManager;

    use super::*;
    use crate::io::MetaReaders;

    /// load index column data
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn load_bloom_filter_by_columns(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        column_needed: &[String],
        path: &str,
        length: u64,
    ) -> Result<DataBlock> {
        let file_meta = load_index_meta(&ctx, path, length).await?;
        if file_meta.row_groups.len() != 1 {
            return Err(ErrorCode::StorageOther(format!(
                "invalid v1 bloom index filter index, number of row group should be 1, but found {} row groups",
                file_meta.row_groups.len()
            )));
        }
        let row_group = &file_meta.row_groups[0];

        let fields = column_needed
            .iter()
            .map(|name| DataField::new(name, Vu8::to_data_type()))
            .collect::<Vec<_>>();

        let schema = Arc::new(DataSchema::new(fields));

        // 1. load column data, as bytes
        let futs = column_needed
            .iter()
            .map(|col_name| load_column_bytes(&ctx, &file_meta, col_name, path, &dal))
            .collect::<Vec<_>>();
        let cols_data = try_join_all(futs)
            .instrument(tracing::debug_span!("join_columns"))
            .await?;

        let column_descriptors = file_meta.schema_descr.columns();
        let arrow_schema = infer_schema(&file_meta)?;
        let mut columns_array_iter = Vec::with_capacity(cols_data.len());
        let num_values = row_group.num_rows();

        // 2. deserialize column data

        // wrapping around Arc<Vec<u8>>, so that bytes clone can be avoided
        // later in the construction of PageReader
        struct Wrap(Arc<Vec<u8>>);
        impl AsRef<[u8]> for Wrap {
            #[inline]
            fn as_ref(&self) -> &[u8] {
                self.0.as_ref()
            }
        }
        let columns = row_group.columns();
        tracing::debug_span!("build_array_iter").in_scope(|| {
            for (bytes, col_idx) in cols_data.into_iter() {
                let compression_codec = columns[0]
                    .column_chunk()
                    .meta_data
                    .as_ref()
                    .ok_or_else(|| {
                        ErrorCode::UnexpectedError(format!("column meta is none, idx {}", col_idx))
                    })?
                    .codec;
                let compression = Compression::try_from(compression_codec).map_err(|e| {
                    ErrorCode::ParquetError(format!("unrecognized compression. {} ", e))
                })?;
                let descriptor = file_meta.schema_descr.columns()[col_idx].descriptor.clone();

                let page_meta_data = PageMetaData {
                    column_start: 0,
                    num_values: num_values as i64,
                    compression,
                    descriptor,
                };

                let wrapped = Wrap(bytes);
                let page_reader = PageReader::new_with_page_meta(
                    std::io::Cursor::new(wrapped), // we can not use &[u8] as Reader here, lifetime not valid
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );
                let decompressor = BasicDecompressor::new(page_reader, vec![]);
                let decompressors = vec![decompressor];
                let types = vec![&column_descriptors[col_idx].descriptor.primitive_type];
                let field = arrow_schema.fields[col_idx].clone();
                let arrays = tracing::debug_span!("iter_to_arrays").in_scope(|| {
                    column_iter_to_arrays(decompressors, types, field, Some(num_values), num_values)
                })?;
                columns_array_iter.push(arrays);
            }
            Ok::<_, ErrorCode>(())
        })?;

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_values, None);
        let next = tracing::debug_span!("deserializer_next").in_scope(|| deserializer.next());

        match next {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => {
                let span = tracing::info_span!("from_chunk");
                span.in_scope(|| DataBlock::from_chunk(&schema, &chunk))
            }
        }
    }

    /// Loads bytes and index of the given column.
    /// read data from cache, or populate cache items if possible
    #[tracing::instrument(level = "debug", skip_all)]
    async fn load_column_bytes(
        _ctx: &Arc<dyn TableContext>,
        file_meta: &FileMetaData,
        col_name: &str,
        path: &str,
        dal: &Operator,
    ) -> Result<(Arc<Vec<u8>>, usize)> {
        let storage_runtime = GlobalIORuntime::instance();
        let cols = file_meta.row_groups[0].columns();
        if let Some((idx, col_meta)) = cols
            .iter()
            .enumerate()
            .find(|(_, c)| c.descriptor().path_in_schema[0] == col_name)
        {
            let cache_key = format!("{path}-{idx}");
            if let Some(bloom_index_cache) = CacheManager::instance().get_bloom_index_cache() {
                let cache = &mut bloom_index_cache.write().await;
                if let Some(bytes) = cache.get(&cache_key) {
                    Ok((bytes.clone(), idx))
                } else {
                    let bytes = Arc::new(
                        // As suggested by Winter, execute task of loading data in storage runtime
                        load_index_column_data_from_storage(
                            col_meta.clone(),
                            dal.clone(),
                            path.to_owned(),
                        )
                        .execute_in_runtime(&storage_runtime)
                        .await??,
                    );
                    cache.put(cache_key, bytes.clone());
                    Ok((bytes, idx))
                }
            } else {
                let bytes = Arc::new(
                    load_index_column_data_from_storage(
                        col_meta.clone(),
                        dal.clone(),
                        path.to_owned(),
                    )
                    .execute_in_runtime(&storage_runtime)
                    .await??,
                );
                Ok((bytes, idx))
            }
        } else {
            Err(ErrorCode::LogicalError(format!(
                "failed to find bloom index column. no such column {col_name}"
            )))
        }
    }

    /// Loads index meta data
    /// read data from cache, or populate cache items if possible
    #[tracing::instrument(level = "debug", skip_all)]
    async fn load_index_meta(
        ctx: &Arc<dyn TableContext>,
        path: &str,
        length: u64,
    ) -> Result<Arc<FileMetaData>> {
        let storage_runtime = GlobalIORuntime::instance();
        let ctx_cloned = ctx.clone();
        let path_owned = path.to_owned();
        async move {
            let reader = MetaReaders::file_meta_data_reader(ctx_cloned);
            // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
            // so we just pass a zero to reader
            let version = 0;
            reader.read(path_owned, Some(length), version).await
        }
        .execute_in_runtime(&storage_runtime)
        .await?
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn load_index_column_data_from_storage(
        col_meta: ColumnChunkMetaData,
        dal: Operator,
        path: String,
    ) -> Result<Vec<u8>> {
        let chunk_meta = col_meta.metadata();
        let chunk_offset = chunk_meta.data_page_offset as u64;
        let col_len = chunk_meta.total_compressed_size as u64;
        let column_reader = dal.object(&path);
        let bytes = column_reader
            .range_read(chunk_offset..chunk_offset + col_len)
            .await?;
        Ok(bytes)
    }

    #[async_trait::async_trait]
    trait InRuntime
    where Self: Future
    {
        async fn execute_in_runtime(self, runtime: &Runtime) -> Result<Self::Output>;
    }

    #[async_trait::async_trait]
    impl<T> InRuntime for T
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
            runtime
                .try_spawn(self)?
                .await
                .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
        }
    }
}
