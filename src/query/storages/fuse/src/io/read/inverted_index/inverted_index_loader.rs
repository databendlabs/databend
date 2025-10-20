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
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_milliseconds;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::table::TableCompression;
use log::info;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;

use crate::index::InvertedIndexFile;
use crate::io::read::block::parquet::RowGroupImplBuilder;
use crate::io::MetaReaders;

const INDEX_COLUMN_NAMES: [&str; 8] = [
    "fast",
    "store",
    "fieldnorm",
    "pos",
    "idx",
    "term",
    "meta.json",
    ".managed.json",
];

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
    #[async_backtrace::framed]
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
        runtime
            .try_spawn(self, None)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

/// Loads inverted index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub(crate) async fn load_inverted_index_meta(
    dal: Operator,
    path: &str,
) -> Result<Arc<InvertedIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::inverted_index_meta_reader(dal);
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: None,
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

// Used to read inverted index data in old versions; will be removed in the future.
#[fastrace::trace]
pub(crate) async fn legacy_load_inverted_index_files<'a>(
    settings: &ReadSettings,
    columns: Vec<(String, Range<u64>)>,
    location: &'a str,
    operator: &'a Operator,
) -> Result<Vec<Arc<InvertedIndexFile>>> {
    let start = Instant::now();
    info!("load inverted index directory version 2");

    let mut files = vec![];
    let mut ranges = vec![];
    let mut column_id = 0;
    let mut names_map = HashMap::new();
    let inverted_index_file_cache = CacheManager::instance().get_inverted_index_file_cache();
    for (name, range) in columns.into_iter() {
        let cache_key = cache_key_of_column(location, &name);
        if let Some(cache_file) =
            inverted_index_file_cache.get_sized(&cache_key, range.end - range.start)
        {
            files.push(cache_file);
            continue;
        }

        // if cache missed, prepare the ranges to be read
        ranges.push((column_id, range));
        names_map.insert(column_id, (name, cache_key));
        column_id += 1;
    }

    let mut inverted_bytes_len = 0;
    if !ranges.is_empty() {
        let merge_io_result =
            MergeIOReader::merge_io_read(settings, operator.clone(), location, &ranges).await?;

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
            let chunk = merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
            let data = chunk.slice(range.clone()).to_vec();
            inverted_bytes_len += data.len();

            let (name, cache_key) = names_map.remove(column_id).unwrap();
            let file = InvertedIndexFile::create(name, data);

            // add index file to cache
            inverted_index_file_cache.insert(cache_key, file.clone());
            files.push(file.into());
        }
    }

    // Perf.
    {
        metrics_inc_block_inverted_index_read_bytes(inverted_bytes_len as u64);
        metrics_inc_block_inverted_index_read_milliseconds(start.elapsed().as_millis() as u64);
    }

    Ok(files)
}

/// Loads bytes of each inverted index files
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub(crate) async fn load_inverted_index_files<'a>(
    settings: &ReadSettings,
    inverted_index_meta_map: HashMap<String, SingleColumnMeta>,
    location: &'a str,
    operator: &'a Operator,
) -> Result<Vec<Arc<InvertedIndexFile>>> {
    let start = Instant::now();

    let mut inverted_index_fields = Vec::with_capacity(inverted_index_meta_map.len());
    for (name, _) in inverted_index_meta_map.iter() {
        let field = Field::new(name, arrow::datatypes::DataType::Binary, false);
        inverted_index_fields.push(field);
    }

    // 1. read column data, first try to read from cache,
    // if not exists, fetch from object storage
    let mut ranges = Vec::new();
    let mut names_map = HashMap::new();
    let mut inverted_files = Vec::with_capacity(inverted_index_fields.len());
    let inverted_index_file_cache = CacheManager::instance().get_inverted_index_file_cache();
    for (idx, index_field) in inverted_index_fields.iter().enumerate() {
        let name = index_field.name();
        let col_meta = inverted_index_meta_map.get(name).unwrap();
        let cache_key = cache_key_of_column(location, name);
        if let Some(cache_file) = inverted_index_file_cache.get_sized(&cache_key, col_meta.len) {
            inverted_files.push(cache_file);
            continue;
        }

        // if cache missed, prepare the ranges to be read
        let col_range = col_meta.offset..(col_meta.offset + col_meta.len);

        ranges.push((idx as u32, col_range));
        names_map.insert(idx as u32, (name, cache_key));
    }

    let mut inverted_bytes_len = 0;
    if !ranges.is_empty() {
        // 2. read data from object store.
        let merge_io_result =
            MergeIOReader::merge_io_read(settings, operator.clone(), location, &ranges).await?;

        let mut raw_column_data = HashMap::with_capacity(ranges.len());
        for (idx, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
            let chunk = merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
            let data = chunk.slice(range.clone());

            raw_column_data.insert(*idx as usize, data);
        }
        let mut column_indices = Vec::with_capacity(ranges.len());
        for (idx, _) in &ranges {
            column_indices.push(*idx as usize);
        }

        let inverted_index_schema = Schema::new(Fields::from(inverted_index_fields.clone()));
        let inverted_index_schema_desc =
            Arc::new(ArrowSchemaConverter::new().convert(&inverted_index_schema)?);

        // 3. deserialize raw data to inverted index data
        let mut builder = RowGroupImplBuilder::new(
            1,
            &inverted_index_schema_desc,
            TableCompression::Zstd.into(),
        );

        for (idx, column_data) in raw_column_data.into_iter() {
            builder.add_column_chunk(idx, column_data);
        }
        let row_group = Box::new(builder.build());
        let field_levels = parquet_to_arrow_field_levels(
            inverted_index_schema_desc.as_ref(),
            ProjectionMask::leaves(&inverted_index_schema_desc, column_indices),
            None,
        )?;
        let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &field_levels,
            row_group.as_ref(),
            1,
            None,
        )?;
        let record = record_reader.next().unwrap()?;
        assert!(record_reader.next().is_none());

        for (i, (idx, _)) in ranges.iter().enumerate() {
            let (name, cache_key) = names_map.remove(idx).unwrap();
            let inverted_binary = record.column(i).clone();
            let column = Column::from_arrow_rs(
                inverted_binary,
                &databend_common_expression::types::DataType::Binary,
            )?;
            inverted_bytes_len += column.memory_size();
            let value = unsafe { column.index_unchecked(0) };
            let bytes = value.as_binary().unwrap();
            let file = InvertedIndexFile::create(name.clone(), bytes.to_vec());
            // add index file to cache
            inverted_index_file_cache.insert(cache_key, file.clone());
            inverted_files.push(Arc::new(file));
        }
    }

    // Perf.
    {
        metrics_inc_block_inverted_index_read_bytes(inverted_bytes_len as u64);
        metrics_inc_block_inverted_index_read_milliseconds(start.elapsed().as_millis() as u64);
    }

    Ok(inverted_files)
}

/// load inverted index directory
#[fastrace::trace]
pub(crate) async fn load_inverted_index_directory<'a>(
    settings: &ReadSettings,
    location: &'a str,
    operator: &'a Operator,
    version: usize,
    inverted_index_meta_map: HashMap<String, SingleColumnMeta>,
) -> Result<InvertedIndexDirectory> {
    // load inverted index files, usually including following eight files:
    // 1. fast file
    // 2. store file
    // 3. fieldnorm file
    // 4. position file
    // 5. idx file
    // 6. term file
    // 7. meta.json file
    // 8. .managed.json file
    if version == 1 {
        info!("load inverted index directory version 1");
        let mut columns = Vec::with_capacity(inverted_index_meta_map.len());
        for (col_name, col_meta) in inverted_index_meta_map {
            let col_range = col_meta.offset..(col_meta.offset + col_meta.len);
            columns.push((col_name, col_range));
        }
        let files = legacy_load_inverted_index_files(settings, columns, location, operator).await?;
        // use those files to create inverted index directory
        let directory = InvertedIndexDirectory::try_create(files)?;
        return Ok(directory);
    }

    let files =
        load_inverted_index_files(settings, inverted_index_meta_map, location, operator).await?;
    // use those files to create inverted index directory
    let directory = InvertedIndexDirectory::try_create(files)?;

    Ok(directory)
}

fn cache_key_of_column(index_path: &str, index_column_name: &str) -> String {
    format!("{index_path}-{index_column_name}")
}

pub(crate) fn cache_key_of_index_columns(index_path: &str) -> Vec<String> {
    INDEX_COLUMN_NAMES
        .iter()
        .map(|column_name| cache_key_of_column(index_path, column_name))
        .collect()
}
