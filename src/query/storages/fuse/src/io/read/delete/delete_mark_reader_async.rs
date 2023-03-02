// Copyright 2023 Datafuse Labs.
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
//

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_base::runtime::GlobalIORuntime;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;
use storages_common_cache::CacheKey;
use storages_common_cache::InMemoryItemCacheReader;
use storages_common_cache::LoadParams;
use storages_common_cache::Loader;
use storages_common_cache_manager::CachedObject;
use storages_common_cache_manager::DeleteMarkMeta;
use storages_common_table_meta::meta::DeleteMaskVersion;
use storages_common_table_meta::meta::Location;

use crate::io::read::delete::delete_mark_reader::deserialize_bitmap;
use crate::io::read::InRuntime;
use crate::io::DeleteMarkReader;
use crate::io::MetaReaders;

type CachedReader = InMemoryItemCacheReader<Bitmap, DeleteMetaLoader>;

#[async_trait::async_trait]
impl DeleteMarkReader for Location {
    async fn read_delete_mark(&self, dal: Operator, size: u64) -> Result<Arc<Bitmap>> {
        let (path, ver) = &self;
        let mask_version = DeleteMaskVersion::try_from(*ver)?;
        if matches!(mask_version, DeleteMaskVersion::V0(_)) {
            let res = load_delete_mark(dal, path, size).await?;
            Ok(res)
        } else {
            unreachable!()
        }
    }
}

/// load delete mark data
#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_delete_mark(dal: Operator, path: &str, size: u64) -> Result<Arc<Bitmap>> {
    // 1. load delete mark meta
    let delete_mark_meta = load_mark_meta(dal.clone(), path, size).await?;
    let file_meta = &delete_mark_meta.0;
    if file_meta.row_groups.len() != 1 {
        return Err(ErrorCode::StorageOther(format!(
            "invalid delete mark meta, number of row group should be 1, but found {} row groups",
            file_meta.row_groups.len()
        )));
    }

    // 2. load delete mark data
    let index_column_chunk_metas = file_meta.row_groups[0].columns();
    assert_eq!(index_column_chunk_metas.len(), 1);
    let column_meta = &index_column_chunk_metas[0];
    let num_rows = column_meta.num_values();
    let marks = load_delete_mark_data(column_meta, path, &dal, num_rows as usize).await?;

    Ok(marks)
}

/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_delete_mark_data<'a>(
    col_chunk_meta: &'a ColumnChunkMetaData,
    path: &'a str,
    dal: &'a Operator,
    num_rows: usize,
) -> Result<Arc<Bitmap>> {
    let storage_runtime = GlobalIORuntime::instance();
    let bytes = {
        let meta = col_chunk_meta.metadata();
        let location = path.to_string();
        let loader = DeleteMetaLoader {
            num_rows,
            offset: meta.data_page_offset as u64,
            len: meta.total_compressed_size as u64,
            cache_key: location.clone(),
            operator: dal.clone(),
            column_descriptor: col_chunk_meta.descriptor().clone(),
        };

        let cached_reader = CachedReader::new(Bitmap::cache(), loader);

        let param = LoadParams {
            location,
            len_hint: None,
            ver: 0,
            put_cache: true,
        };
        async move { cached_reader.read(&param).await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(bytes)
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
async fn load_mark_meta(dal: Operator, path: &str, size: u64) -> Result<Arc<DeleteMarkMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::delete_mark_meta_reader(dal);
        // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
        // so we just pass a zero to reader
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: Some(size),
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

pub struct DeleteMetaLoader {
    pub num_rows: usize,
    pub offset: u64,
    pub len: u64,
    pub cache_key: String,
    pub operator: Operator,
    pub column_descriptor: ColumnDescriptor,
}

#[async_trait::async_trait]
impl Loader<Bitmap> for DeleteMetaLoader {
    async fn load(&self, params: &LoadParams) -> Result<Bitmap> {
        let reader = self.operator.object(&params.location);
        let bytes = reader
            .range_read(self.offset..self.offset + self.len)
            .await?;

        deserialize_bitmap(&bytes, self.num_rows, &self.column_descriptor)
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
