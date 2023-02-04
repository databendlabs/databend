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

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use common_exception::Result;

use super::loader::LoadParams;
use super::loader::Loader;
use crate::cache::StorageCache;
use crate::metrics::metrics_inc_cache_access_count;
use crate::metrics::metrics_inc_cache_hit_count;
use crate::metrics::metrics_inc_cache_miss_count;
use crate::metrics::metrics_inc_cache_miss_load_millisecond;

/// A generic cache-aware reader
///
/// Given an impl of [StorageCache], e.g. `ItemCache` or `DiskCache` and a proper impl
/// [LoaderWithCacheKey], which is able to load `T`, `CachedReader` will load the `T`
/// by using [LoaderWithCacheKey], and populate the cache item into [StorageCache] by using
/// the loaded `T` and the key that [LoaderWithCacheKey] provides.
pub struct CachedReader<T, L, C> {
    cache: Option<C>,
    loader: L,
    /// name of this cache instance
    name: String,
    _p: PhantomData<T>,
}

impl<T, L, C> CachedReader<T, L, C>
where
    L: Loader<T> + Sync,
    C: StorageCache<String, T>,
{
    pub fn new(cache: Option<C>, name: impl Into<String>, loader: L) -> Self {
        Self {
            cache,
            name: name.into(),
            loader,
            _p: PhantomData,
        }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    pub async fn read(&self, params: &LoadParams) -> Result<Arc<T>> {
        match &self.cache {
            None => Ok(Arc::new(self.loader.load(params).await?)),
            Some(labeled_cache) => {
                // Perf.
                {
                    metrics_inc_cache_access_count(1, &self.name);
                }

                let cache_key = self.loader.cache_key(params);
                match labeled_cache.get(cache_key.as_str()) {
                    Some(item) => {
                        // Perf.
                        {
                            metrics_inc_cache_hit_count(1, &self.name);
                        }

                        Ok(item)
                    }
                    None => {
                        let start = Instant::now();

                        let v = self.loader.load(params).await?;
                        let item = Arc::new(v);

                        // Perf.
                        {
                            metrics_inc_cache_miss_count(1, &self.name);
                            metrics_inc_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                                &self.name,
                            );
                        }

                        labeled_cache.put(cache_key, item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}
