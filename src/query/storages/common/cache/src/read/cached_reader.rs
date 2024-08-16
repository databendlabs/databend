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

use std::sync::Arc;
use std::time::Instant;

use databend_common_cache::CountableMeter;
use databend_common_exception::Result;
use databend_common_metrics::cache::*;

use super::loader::LoadParams;
use crate::CacheAccessor;
use crate::InMemoryItemCacheHolder;
use crate::Loader;

/// A cache-aware reader
pub struct CachedReader<L, C> {
    cache: Option<C>,
    loader: L,
}

impl<V, L, M> CachedReader<L, InMemoryItemCacheHolder<V, M>>
where
    L: Loader<V> + Sync,
    M: CountableMeter<String, Arc<V>>,
{
    pub fn new(cache: Option<InMemoryItemCacheHolder<V, M>>, loader: L) -> Self {
        Self { cache, loader }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    #[async_backtrace::framed]
    pub async fn read(&self, params: &LoadParams) -> Result<Arc<V>> {
        match &self.cache {
            None => Ok(Arc::new(self.loader.load(params).await?)),
            Some(cache) => {
                let cache_key = self.loader.cache_key(params);
                match cache.get(cache_key.as_str()) {
                    Some(item) => Ok(item),
                    None => {
                        let start = Instant::now();

                        let v = self.loader.load(params).await?;
                        let item = Arc::new(v);

                        // Perf.
                        {
                            metrics_inc_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                                cache.name(),
                            );
                        }

                        if params.put_cache {
                            cache.put(cache_key, item.clone());
                        }
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.cache.as_ref().map(|c| c.name()).unwrap_or("")
    }
}
