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

use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::Instant;

use common_cache::CountableMeter;
use common_cache::LruCache;
use common_exception::Result;
use parking_lot::RwLock;

use super::loader::LoadParams;
use crate::metrics::metrics_inc_cache_access_count;
use crate::metrics::metrics_inc_cache_hit_count;
use crate::metrics::metrics_inc_cache_miss_count;
use crate::metrics::metrics_inc_cache_miss_load_millisecond;
use crate::CacheAccessor;
use crate::Loader;

/// A cache-aware reader
pub struct CachedReader<L, C> {
    cache: Option<C>,
    loader: L,
    cache_name: String,
}

pub type CacheHolder<V, S, M> = Arc<RwLock<LruCache<String, Arc<V>, S, M>>>;

impl<V, L, S, M> CachedReader<L, CacheHolder<V, S, M>>
where
    L: Loader<V> + Sync,
    S: BuildHasher,
    M: CountableMeter<String, Arc<V>>,
{
    pub fn new(cache: Option<CacheHolder<V, S, M>>, name: impl Into<String>, loader: L) -> Self {
        Self {
            cache,
            cache_name: name.into(),
            loader,
        }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    pub async fn read(&self, params: &LoadParams) -> Result<Arc<V>> {
        match &self.cache {
            None => Ok(Arc::new(self.loader.load(params).await?)),
            Some(cache) => {
                // Perf.
                {
                    metrics_inc_cache_access_count(1, &self.cache_name);
                }

                let cache_key = self.loader.cache_key(params);
                match cache.get(cache_key.as_str()) {
                    Some(item) => {
                        // Perf.
                        {
                            metrics_inc_cache_hit_count(1, &self.cache_name);
                        }

                        Ok(item)
                    }
                    None => {
                        let start = Instant::now();

                        let v = self.loader.load(params).await?;
                        let item = Arc::new(v);

                        // Perf.
                        {
                            metrics_inc_cache_miss_count(1, &self.cache_name);
                            metrics_inc_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                                &self.cache_name,
                            );
                        }

                        cache.put(cache_key, item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.cache_name.as_str()
    }
}
