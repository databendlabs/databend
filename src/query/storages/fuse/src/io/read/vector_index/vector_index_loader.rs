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
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_milliseconds;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;

use crate::index::VectorIndexFile;
use crate::io::read::block::parquet::RowGroupImplBuilder;
use crate::io::MetaReaders;

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

/// Loads vector index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub async fn load_vector_index_meta(dal: Operator, path: &str) -> Result<Arc<VectorIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::vector_index_meta_reader(dal);
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

/// load index column data
#[fastrace::trace]
pub async fn load_vector_index_files<'a>(
    operator: Operator,
    settings: &ReadSettings,
    column_names: &'a [String],
    location: &'a str,
) -> Result<Vec<Column>> {
    let start = Instant::now();

    // 1. load index meta
    let vector_index_meta = load_vector_index_meta(operator.clone(), location).await?;

    // 2. build index schema
    let vector_index_fields: Vec<_> = vector_index_meta
        .columns
        .iter()
        .map(|col| Field::new(col.0.clone(), arrow::datatypes::DataType::Binary, false))
        .collect();
    let vector_index_schema = Schema::new(Fields::from(vector_index_fields));

    let vector_index_schema_desc =
        Arc::new(ArrowSchemaConverter::new().convert(&vector_index_schema)?);

    // 3. collect column metas that needed to build vector index
    let vector_column_chunk_metas = &vector_index_meta.columns;

    let mut column_indices = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        let column_index = vector_index_schema.index_of(column_name)?;
        column_indices.push(column_index);
    }

    let projected_column_metas: Vec<_> = vector_column_chunk_metas
        .iter()
        .enumerate()
        .filter(|(i, _)| column_indices.contains(i))
        .map(|(_, meta)| meta)
        .collect();

    // 4. read column data, first try to read from cache,
    // if not exists, fetch from object storage
    let mut ranges = Vec::new();
    let mut names_map = HashMap::new();
    let mut column_data = HashMap::new();
    let vector_index_file_cache = CacheManager::instance().get_vector_index_file_cache();
    for (i, (name, col_meta)) in column_indices
        .iter()
        .zip(projected_column_metas.into_iter())
    {
        let cache_key = cache_key_of_column(location, name);
        if let Some(cache_file) = vector_index_file_cache.get_sized(&cache_key, col_meta.len) {
            column_data.insert(*i, cache_file);
            continue;
        }

        // if cache missed, prepare the ranges to be read
        let col_range = col_meta.offset..(col_meta.offset + col_meta.len);

        ranges.push((*i as u32, col_range));
        names_map.insert(*i as u32, (name, cache_key));
    }

    if !ranges.is_empty() {
        let merge_io_result =
            MergeIOReader::merge_io_read(settings, operator.clone(), location, &ranges).await?;

        // merge column data fetched from object storage
        for (i, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
            let chunk = merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
            let data = chunk.slice(range.clone());

            let (name, cache_key) = names_map.remove(i).unwrap();
            let file = VectorIndexFile::create(name.clone(), data.to_vec().into());

            // add index file to cache
            vector_index_file_cache.insert(cache_key, file.clone());
            column_data.insert(*i as usize, Arc::new(file));
        }
    }

    // 5. deserialize raw data to vector index data
    let mut builder =
        RowGroupImplBuilder::new(1, &vector_index_schema_desc, TableCompression::Zstd.into());

    for (i, column_data) in column_data {
        builder.add_column_chunk(i, column_data.data.clone().into());
    }
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        vector_index_schema_desc.as_ref(),
        ProjectionMask::leaves(&vector_index_schema_desc, column_indices),
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

    let mut vector_bytes_len = 0;
    let mut vector_columns = Vec::with_capacity(4);
    for i in 0..record.num_columns() {
        let vector_binary = record.column(i).clone();
        let column = Column::from_arrow_rs(
            vector_binary,
            &databend_common_expression::types::DataType::Binary,
        )?;
        vector_bytes_len += column.memory_size(false);
        vector_columns.push(column);
    }

    let elapsed = start.elapsed().as_millis() as u64;
    // Perf.
    {
        metrics_inc_block_vector_index_read_bytes(vector_bytes_len as u64);
        metrics_inc_block_vector_index_read_milliseconds(elapsed);
    }

    Ok(vector_columns)
}

fn cache_key_of_column(index_path: &str, index_column_name: &str) -> String {
    format!("{index_path}-{index_column_name}")
}
