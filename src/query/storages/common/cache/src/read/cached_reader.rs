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

use databend_common_exception::Result;
use databend_common_metrics::cache::*;

use super::loader::LoadParams;
use crate::caches::CacheValue;
use crate::CacheAccessor;
use crate::InMemoryLruCache;
use crate::Loader;

/// A cache-aware reader
pub struct CachedReader<L, C> {
    cache: Option<C>,
    loader: L,
}

impl<V: Into<CacheValue<V>>, L> CachedReader<L, InMemoryLruCache<V>>
where L: Loader<V> + Sync
{
    pub fn new(cache: Option<InMemoryLruCache<V>>, loader: L) -> Self {
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

                        // Perf.
                        {
                            metrics_inc_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                                cache.name(),
                            );
                        }

                        match params.put_cache {
                            true => Ok(cache.insert(cache_key, v)),
                            false => Ok(Arc::new(v)),
                        }
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.cache.as_ref().map(|c| c.name()).unwrap_or("")
    }
}
