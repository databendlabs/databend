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
use bytes::Bytes;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::DataType;
use databend_storages_common_cache::CacheRef;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Reader;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::parquet_to_arrow_field_levels;

use crate::io::FuseCaches;
use crate::io::IndexMetaCache;
use crate::io::IndexMetaKey;
use crate::io::IndexMetaReader;
use crate::io::read::block::parquet::RowGroupImplBuilder;

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
            .spawn(self)
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

pub trait IndexLoadSpec {
    type Meta: CachedItem;
    type File: CachedItem;

    fn meta_cache(caches: &IndexMetaCache) -> &MemCache<Arc<Self::Meta>>;
    /// `None`: the file bytes are not cached.
    fn file_cache(caches: &FuseCaches) -> Option<&MemCache<Self::File>>;
    fn meta_columns(meta: &Self::Meta) -> &Vec<(String, SingleColumnMeta)>;
    fn make_file(name: String, data: Bytes) -> Self::File;
    fn file_data(file: &Self::File) -> &Bytes;
    fn arrow_data_type(column_name: &str) -> arrow::datatypes::DataType;
    fn column_data_type(column_name: &str) -> DataType;
    fn record_metrics(bytes: u64, ms: u64);
}

#[fastrace::trace]
pub async fn load_index_meta<S: IndexLoadSpec>(dal: Operator, path: &str) -> Result<Arc<S::Meta>>
where IndexMetaReader<S::Meta>: Reader<Key = IndexMetaKey, Item = Arc<S::Meta>> {
    let cache = S::meta_cache(&FuseCaches::instance().index_metas).clone();
    let key = (path.to_owned(), None);
    async move {
        let reader = IndexMetaReader::<S::Meta>::new(dal);
        let meta = cache.get(&reader, key).await?;
        Ok(Arc::clone(&meta))
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

#[fastrace::trace]
pub async fn load_index_files<S: IndexLoadSpec>(
    operator: Operator,
    settings: &ReadSettings,
    column_names: &[String],
    location: &str,
) -> Result<Vec<Column>>
where
    IndexMetaReader<S::Meta>: Reader<Key = IndexMetaKey, Item = Arc<S::Meta>>,
{
    let start = Instant::now();

    // 1. load index meta
    let index_meta = load_index_meta::<S>(operator.clone(), location).await?;

    // 2. build index schema
    let index_fields: Vec<_> = S::meta_columns(&index_meta)
        .iter()
        .map(|col| Field::new(col.0.clone(), S::arrow_data_type(&col.0), false))
        .collect();
    let index_schema = Schema::new(Fields::from(index_fields));

    let index_schema_desc = Arc::new(ArrowSchemaConverter::new().convert(&index_schema)?);

    // 3. collect column metas that needed to build index
    let column_chunk_metas = S::meta_columns(&index_meta);

    let mut column_indices = Vec::with_capacity(column_names.len());
    let mut projected_column_metas = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        let column_index = index_schema.index_of(column_name)?;
        let meta = column_chunk_metas
            .get(column_index)
            .ok_or_else(|| ErrorCode::Internal("Invalid index column"))?;
        column_indices.push(column_index);
        projected_column_metas.push(meta);
    }

    // 4. read column data, first try to read from cache,
    // if not exists, fetch from object storage
    let mut ranges = Vec::new();
    let mut names_map = HashMap::new();
    let mut column_data = HashMap::new();
    let caches = FuseCaches::instance();
    let index_file_cache = S::file_cache(&caches);
    for (i, (name, col_meta)) in column_indices
        .iter()
        .zip(projected_column_metas.into_iter())
    {
        let cache_key = cache_key_of_column(location, name);
        let hit = index_file_cache.and_then(|cache| cache.lookup_sized(&cache_key, col_meta.len));
        if let Some(cache_file) = hit {
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
            let file = S::make_file(name.clone(), data.to_vec().into());

            // add index file to cache
            let file = match index_file_cache {
                Some(cache) => cache.fill(&cache_key, file),
                None => CacheRef::new(file),
            };
            column_data.insert(*i as usize, file);
        }
    }

    let mut sorted_indices = column_indices.clone();
    sorted_indices.sort_unstable();

    // 5. deserialize raw data to index data
    let mut builder =
        RowGroupImplBuilder::new(1, &index_schema_desc, TableCompression::Zstd.into());

    for (i, column_data) in column_data {
        builder.add_column_chunk(i, S::file_data(&column_data).clone().into());
    }
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        index_schema_desc.as_ref(),
        ProjectionMask::leaves(&index_schema_desc, sorted_indices.clone()),
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

    let mut index_bytes_len = 0;
    let mut index_columns = Vec::with_capacity(column_indices.len());
    let mut index_to_pos = HashMap::new();
    for (pos, schema_index) in sorted_indices.iter().enumerate() {
        index_to_pos.insert(*schema_index, pos);
    }

    for schema_index in &column_indices {
        let Some(record_pos) = index_to_pos.get(schema_index) else {
            return Err(ErrorCode::Internal("Invalid index data"));
        };
        let index_binary = record.column(*record_pos).clone();
        let data_type = S::column_data_type(&column_chunk_metas[*schema_index].0);
        let column = Column::from_arrow_rs(index_binary, &data_type)?;
        index_bytes_len += column.memory_size(false);
        index_columns.push(column);
    }

    let elapsed = start.elapsed().as_millis() as u64;
    S::record_metrics(index_bytes_len as u64, elapsed);

    Ok(index_columns)
}

fn cache_key_of_column(index_path: &str, index_column_name: &str) -> String {
    format!("{index_path}-{index_column_name}")
}
