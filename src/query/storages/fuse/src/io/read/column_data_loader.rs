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
use opendal::Operator;
use storages_common_cache::CacheKey;
use storages_common_cache::LoadParams;
use storages_common_cache::LoaderWithCacheKey;

/// Loader that fetch range of the target object with customized cache key
pub struct ColumnDataLoader {
    pub offset: u64,
    pub len: u64,
    pub cache_key: String,
    pub operator: Operator,
}

#[async_trait::async_trait]
impl LoaderWithCacheKey<Vec<u8>> for ColumnDataLoader {
    async fn load_with_cache_key(
        &self,
        params: &LoadParams,
    ) -> common_exception::Result<(Vec<u8>, CacheKey)> {
        let column_reader = self.operator.object(&params.location);
        let bytes = column_reader
            .range_read(self.offset..self.offset + self.len)
            .await?;
        Ok((bytes, self.cache_key.clone()))
    }
}
