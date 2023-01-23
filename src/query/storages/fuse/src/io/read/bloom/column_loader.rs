// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use futures_util::future::try_join_all;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::caches::BloomIndexMeta;
use storages_common_table_meta::meta::BlockFilter;
use storages_common_table_meta::meta::ColumnId;
use xorfilter::Xor8;

use crate::io::read::bloom::BloomIndexColumnReader;
use crate::io::MetaReaders;

/// load index column data
#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_bloom_filter_by_columns<'a>(
    dal: Operator,
    column_needed: &'a [String],
    index_path: &'a str,
    index_length: u64,
) -> Result<BlockFilter> {
    // 1. load index meta
    let bloom_index_meta = load_index_meta(dal.clone(), index_path, index_length).await?;
    let file_meta = &bloom_index_meta.0;
    if file_meta.row_groups.len() != 1 {
        return Err(ErrorCode::StorageOther(format!(
            "invalid bloom filter index, number of row group should be 1, but found {} row groups",
            file_meta.row_groups.len()
        )));
    }

    let cols = file_meta.row_groups[0].columns();

    // 2. filter out columns that needed and exist in the index
    let arrow_schema = infer_schema(file_meta)?;
    let col_metas: Vec<_> = column_needed
        .iter()
        .enumerate()
        .filter_map(|(idx, col_name)| {
            cols.iter()
                .find(|c| &c.descriptor().path_in_schema[0] == col_name)
                .map(|col| (idx, col, arrow_schema.fields[idx].clone()))
        })
        .collect();

    // 3. load filters
    let futs = col_metas
        .iter()
        .map(|(idx, col_chunk_meta, field)| {
            load_column_xor8(*idx as ColumnId, field, col_chunk_meta, index_path, &dal)
        })
        .collect::<Vec<_>>();

    let cols_data = try_join_all(futs).await?;
    let filter_block: Vec<Arc<Xor8>> = cols_data.into_iter().map(|(x, _)| x).collect();

    // 4. build index schema
    let fields = col_metas
        .iter()
        .map(|(_, col_chunk_mea, _)| {
            TableField::new(
                &col_chunk_mea.descriptor().path_in_schema[0],
                TableDataType::String,
            )
        })
        .collect();

    let filter_schema = TableSchema::new(fields);

    Ok(BlockFilter {
        filter_schema: Arc::new(filter_schema),
        filters: filter_block,
    })
}

/// Loads bytes and index of the given column.
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_column_xor8<'a>(
    idx: ColumnId,
    field: &'a common_arrow::arrow::datatypes::Field,
    col_chunk_meta: &'a ColumnChunkMetaData,
    index_path: &'a str,
    dal: &'a Operator,
) -> Result<(Arc<Xor8>, ColumnId)> {
    let storage_runtime = GlobalIORuntime::instance();
    let bytes = {
        let column_data_reader = BloomIndexColumnReader::new(
            index_path.to_owned(),
            idx,
            col_chunk_meta,
            dal.clone(),
            field,
        );
        async move { column_data_reader.read().await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok((bytes, idx))
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_index_meta(dal: Operator, path: &str, length: u64) -> Result<Arc<BloomIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::file_meta_data_reader(dal);
        // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
        // so we just pass a zero to reader
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: Some(length),
            ver: version,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
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
