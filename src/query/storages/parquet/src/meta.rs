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

use std::future::Future;
use std::intrinsics::unlikely;
use std::sync::Arc;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::plan::FullParquetMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_storage::read_metadata_async;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use futures::Stream;
use futures::StreamExt;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;

use crate::statistics::collect_row_group_stats;

#[derive(Clone)]
struct BasicMetaFileInfo {
    location: String,
    size: u64,
    dedup_key: Option<String>,
}

pub async fn read_metadata_async_cached(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
    dedup_key: &str,
) -> Result<Arc<ParquetMetaData>> {
    let info = operator.info();
    let location = format!("{dedup_key}:{}/{}/{}", info.name(), info.root(), path);
    let reader = MetaReader::meta_data_reader(operator.clone(), location.len() - path.len());
    let load_params = LoadParams {
        location,
        len_hint: file_size,
        ver: 0,
        put_cache: true,
    };
    reader.read(&load_params).await
}

pub fn stream_metas_in_parallel(
    op: &Operator,
    file_infos: Vec<(String, u64, String)>,
    expected: (SchemaDescPtr, String),
    leaf_fields: Arc<Vec<TableField>>,
    num_threads: usize,
    max_memory_usage: u64,
    enable_cache: bool,
) -> impl Stream<Item = Result<Arc<FullParquetMeta>>> + Send + 'static {
    stream_metas_in_parallel_with(file_infos, op, num_threads, move |file_info, op| {
        let (expected_schema, schema_from) = expected.clone();
        let leaf_fields = leaf_fields.clone();

        read_parquet_meta(
            file_info,
            op,
            expected_schema,
            leaf_fields,
            schema_from,
            max_memory_usage,
            enable_cache,
        )
    })
}

pub(crate) fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect.root_schema() != actual.root_schema() {
        // TODO:
        // 1. better print the differs for large schema
        // 2. don't check column id, table name, just check column name and types
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

pub fn stream_metas_in_parallel_for_copy(
    op: &Operator,
    file_infos: Vec<(String, u64)>,
    num_threads: usize,
    max_memory_usage: u64,
) -> impl Stream<Item = Result<Arc<FullParquetMeta>>> + Send + 'static {
    stream_metas_in_parallel_with(file_infos, op, num_threads, move |file_info, op| {
        let (location, size) = file_info;
        read_basic_parquet_meta(
            BasicMetaFileInfo {
                location,
                size,
                // Keep COPY metadata reads uncached. Some object stores may not
                // provide a strong object identity, and COPY correctness should
                // not depend on fallback cache keys.
                dedup_key: None,
            },
            op,
            max_memory_usage,
        )
    })
}

pub fn stream_basic_metas_in_parallel_with_cache(
    op: &Operator,
    file_infos: Vec<(String, u64, Option<String>)>,
    num_threads: usize,
    max_memory_usage: u64,
) -> impl Stream<Item = Result<Arc<FullParquetMeta>>> + Send + 'static {
    stream_metas_in_parallel_with(file_infos, op, num_threads, move |file_info, op| {
        let (location, size, dedup_key) = file_info;
        read_basic_parquet_meta(
            BasicMetaFileInfo {
                location,
                size,
                dedup_key,
            },
            op,
            max_memory_usage,
        )
    })
}

fn stream_metas_in_parallel_with<T, F, Fut>(
    file_infos: Vec<T>,
    op: &Operator,
    num_threads: usize,
    read_meta: F,
) -> impl Stream<Item = Result<Arc<FullParquetMeta>>> + Send + 'static
where
    T: Send + 'static,
    F: Fn(T, Operator) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Arc<FullParquetMeta>>>> + Send + 'static,
{
    let op = op.clone();
    futures::stream::iter(file_infos)
        .map(move |file_info| {
            let read_meta = read_meta.clone();
            let op = op.clone();
            async move { read_meta(file_info, op).await }
        })
        .buffer_unordered(num_threads.max(1))
        .filter_map(|meta| async move { meta.transpose() })
}

async fn read_parquet_meta(
    file_info: (String, u64, String),
    op: Operator,
    expect: SchemaDescPtr,
    leaf_fields: Arc<Vec<TableField>>,
    schema_from: String,
    max_memory_usage: u64,
    enable_cache: bool,
) -> Result<Option<Arc<FullParquetMeta>>> {
    let (location, size, dedup_key) = file_info;
    let meta = load_and_check_parquet_meta(
        &location,
        size,
        op,
        &expect,
        &schema_from,
        enable_cache,
        &dedup_key,
    )
    .await?;
    check_memory_usage(max_memory_usage)?;
    if unlikely(meta.file_metadata().num_rows() == 0) {
        // Don't collect empty files
        return Ok(None);
    }
    let stats = collect_row_group_stats(meta.row_groups(), &leaf_fields, None);
    Ok(Some(Arc::new(FullParquetMeta {
        location,
        size,
        meta,
        row_group_level_stats: stats,
    })))
}

/// Load parquet meta and check if the schema is matched.
#[async_backtrace::framed]
async fn load_and_check_parquet_meta(
    file: &str,
    size: u64,
    op: Operator,
    expect: &SchemaDescriptor,
    schema_from: &str,
    enable_cache: bool,
    dedup_key: &str,
) -> Result<Arc<ParquetMetaData>> {
    let metadata = if enable_cache {
        read_metadata_async_cached(file, &op, Some(size), dedup_key).await?
    } else {
        Arc::new(read_metadata_async(file, &op, Some(size)).await?)
    };
    check_parquet_schema(
        expect,
        metadata.file_metadata().schema_descr(),
        file,
        schema_from,
    )?;
    Ok(metadata)
}

async fn read_basic_parquet_meta(
    file_info: BasicMetaFileInfo,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Option<Arc<FullParquetMeta>>> {
    let meta = load_basic_parquet_meta_data(&file_info, &op).await?;
    check_memory_usage(max_memory_usage)?;
    if unlikely(meta.file_metadata().num_rows() == 0) {
        // Don't collect empty files
        return Ok(None);
    }
    Ok(Some(Arc::new(FullParquetMeta {
        location: file_info.location,
        size: file_info.size,
        meta,
        row_group_level_stats: None,
    })))
}

async fn load_basic_parquet_meta_data(
    file_info: &BasicMetaFileInfo,
    op: &Operator,
) -> Result<Arc<ParquetMetaData>> {
    if let Some(dedup_key) = &file_info.dedup_key {
        read_metadata_async_cached(&file_info.location, op, Some(file_info.size), dedup_key).await
    } else {
        Ok(Arc::new(
            read_metadata_async(&file_info.location, op, Some(file_info.size)).await?,
        ))
    }
}

// TODO(parquet): how to limit the memory when running this method is to be determined.
fn check_memory_usage(max_memory_usage: u64) -> Result<()> {
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if (max_memory_usage - used as u64) < 100 * 1024 * 1024 {
        return Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )));
    }
    Ok(())
}

pub struct LoaderWrapper<T>(T, usize);
pub type ParquetMetaReader = InMemoryCacheReader<ParquetMetaData, LoaderWrapper<Operator>>;

pub struct MetaReader;
impl MetaReader {
    pub fn meta_data_reader(dal: Operator, prefix_len: usize) -> ParquetMetaReader {
        ParquetMetaReader::new(
            CacheManager::instance().get_parquet_meta_data_cache(),
            LoaderWrapper(dal, prefix_len),
        )
    }
}

#[async_trait::async_trait]
impl Loader<ParquetMetaData> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<ParquetMetaData> {
        let location = &params.location[self.1..];
        let size = match params.len_hint {
            Some(v) => v,
            None => self.0.stat(location).await?.content_length(),
        };
        read_metadata_async(location, &self.0, Some(size)).await
    }
}
