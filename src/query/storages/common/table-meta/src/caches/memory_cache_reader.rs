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
use std::time::Instant;

use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::read::read_metadata_async;
use common_cache::Cache;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

use super::cache::LoadParams;
use crate::caches::cache_metrics::*;
use crate::caches::LabeledItemCache;
use crate::caches::Loader;

/// A "cache-aware" reader
pub struct MemoryCacheReader<T, L> {
    cache: Option<LabeledItemCache<T>>,
    name: String,
    dal: L,
}

impl<T, L> MemoryCacheReader<T, L>
where L: Loader<T>
{
    pub fn new(cache: Option<LabeledItemCache<T>>, name: impl Into<String>, dal: L) -> Self {
        Self {
            cache,
            name: name.into(),
            dal,
        }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    pub async fn read(&self, params: &LoadParams) -> Result<Arc<T>> {
        match &self.cache {
            None => self.load(params).await,
            Some(labeled_cache) => {
                // Perf.
                {
                    metrics_inc_memory_cache_access_count(1);
                }

                match self.get_by_cache(params.location.as_ref(), labeled_cache) {
                    Some(item) => {
                        // Perf.
                        {
                            metrics_inc_memory_cache_hit_count(1);
                        }

                        Ok(item)
                    }
                    None => {
                        let start = Instant::now();

                        let item = self.load(params).await?;

                        // Perf.
                        {
                            metrics_inc_memory_cache_miss_count(1);
                            metrics_inc_memory_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                            );
                        }

                        let mut cache_guard = labeled_cache.write();
                        cache_guard.put(params.location.clone(), item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get_by_cache(&self, key: &str, cache: &LabeledItemCache<T>) -> Option<Arc<T>> {
        cache.write().get(key).cloned()
    }

    async fn load(&self, params: &LoadParams) -> Result<Arc<T>> {
        let val = self.dal.load(params).await?;
        let item = Arc::new(val);
        Ok(item)
    }
}

/// Loader for parquet FileMetaData
#[async_trait::async_trait]
impl Loader<FileMetaData> for Operator {
    async fn load(&self, params: &LoadParams) -> Result<FileMetaData> {
        let object = self.object(&params.location);
        let mut reader = if let Some(len) = params.len_hint {
            object.range_reader(0..len).await?
        } else {
            object.reader().await?
        };
        read_metadata_async(&mut reader).await.map_err(|err| {
            ErrorCode::Internal(format!(
                "read file meta failed, {}, {:?}",
                params.location, err
            ))
        })
    }
}
