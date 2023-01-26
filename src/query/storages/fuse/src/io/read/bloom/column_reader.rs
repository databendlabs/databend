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
//

use std::sync::Arc;

use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_exception::Result;
use opendal::Operator;
use storages_common_cache::CacheKey;
use storages_common_cache::InMemoryBytesCacheReader;
use storages_common_cache::LoadParams;
use storages_common_cache::LoaderWithCacheKey;
use storages_common_table_meta::caches::CacheManager;
use storages_common_table_meta::meta::ColumnId;

type CachedReader = InMemoryBytesCacheReader<Vec<u8>, ColumnDataLoader>;

/// An wrapper of [InMemoryBytesCacheReader], uses [ColumnDataLoader] to
/// load the data of a given bloom index column. Also
/// - takes cares of getting the correct cache instance from [CacheManager]
/// - generates the proper cache key
///
/// this could be generified to be the template of cached data block column reader as well
pub struct BloomIndexColumnReader {
    cached_reader: CachedReader,
    param: LoadParams,
}

impl BloomIndexColumnReader {
    pub fn new(
        path: String,
        column_id: ColumnId,
        colum_chunk_meta: &ColumnChunkMetaData,
        operator: Operator,
    ) -> Self {
        let meta = colum_chunk_meta.metadata();
        let cache_key = format!("{path}-{column_id}");
        let loader = ColumnDataLoader {
            offset: meta.data_page_offset as u64,
            len: meta.total_compressed_size as u64,
            cache_key,
            operator,
        };

        let cached_reader = CachedReader::new(
            CacheManager::instance().get_bloom_index_cache(),
            "bloom_index_data_cache".to_owned(),
            loader,
        );

        let param = LoadParams {
            location: path,
            len_hint: None,
            ver: 0,
        };

        BloomIndexColumnReader {
            cached_reader,
            param,
        }
    }

    pub async fn read(&self) -> Result<Arc<Vec<u8>>> {
        self.cached_reader.read(&self.param).await
    }
}

/// Loader that fetch range of the target object with customized cache key
struct ColumnDataLoader {
    pub offset: u64,
    pub len: u64,
    pub cache_key: String,
    pub operator: Operator,
}

#[async_trait::async_trait]
impl LoaderWithCacheKey<Vec<u8>> for ColumnDataLoader {
    async fn load_with_cache_key(&self, params: &LoadParams) -> common_exception::Result<Vec<u8>> {
        let column_reader = self.operator.object(&params.location);
        let bytes = column_reader
            .range_read(self.offset..self.offset + self.len)
            .await?;
        Ok(bytes)
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
