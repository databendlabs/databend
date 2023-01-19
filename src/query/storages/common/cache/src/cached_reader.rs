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

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use common_cache::BytesMeter;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;
use parking_lot::RwLock;

use crate::cache::StorageCache;
use crate::metrics::metrics_inc_cache_access_count;
use crate::metrics::metrics_inc_cache_hit_count;
use crate::metrics::metrics_inc_cache_miss_count;
use crate::metrics::metrics_inc_cache_miss_load_millisecond;
use crate::providers::DiskCache;

pub struct LoadParams {
    pub location: String,
    pub len_hint: Option<u64>,
    pub ver: u64,
}

pub type CacheKey = String;
/// Loads an object from storage
#[async_trait::async_trait]
pub trait LoaderWithKey<T> {
    /// Loads object of type T, located by `LoadParams`
    /// If Some(CacheKey) returns, it will be used as the key of cache item,
    /// otherwise, the LoadParams::location will be used
    async fn load_with_cache_key(&self, params: &LoadParams) -> Result<(T, CacheKey)>;
}

/// Loads an object from storage
#[async_trait::async_trait]
pub trait Loader<T> {
    /// Loads object of type T, located by `LoadParams`
    async fn load(&self, params: &LoadParams) -> Result<T>;
}

#[async_trait::async_trait]
impl<L, T> LoaderWithKey<T> for L
where L: Loader<T> + Send + Sync
{
    async fn load_with_cache_key(&self, params: &LoadParams) -> Result<(T, CacheKey)> {
        let v = self.load(params).await?;
        Ok((v, params.location.clone()))
    }
}

/// A "cache-aware" reader
pub struct CachedReader<T, L, C> {
    cache: Option<Arc<RwLock<C>>>,
    loader: L,
    name: String,
    _p: PhantomData<T>,
}

pub type MemoryCacheReader<T, L, M> =
    CachedReader<T, L, LruCache<String, Arc<T>, DefaultHashBuilder, M>>;

pub type InMemoryItemCacheReader<T, L> = MemoryCacheReader<T, L, Count>;
pub type BytesMemoryCacheReader<T, L> = MemoryCacheReader<T, L, BytesMeter>;

// NOTE: not usable yet, just testing api
#[allow(dead_code)]
pub type DiskCacheReader<T, L> = CachedReader<T, L, DiskCache>;

impl<T, L, C, M> CachedReader<T, L, C>
where
    L: LoaderWithKey<T> + Sync,
    C: StorageCache<String, T, Meter = M>,
{
    pub fn new(cache: Option<Arc<RwLock<C>>>, name: impl Into<String>, loader: L) -> Self {
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
            None => Ok(Arc::new(self.loader.load_with_cache_key(params).await?.0)),
            Some(labeled_cache) => {
                // Perf.
                {
                    metrics_inc_cache_access_count(1, &self.name);
                }

                match self.get_cached(params.location.as_ref(), labeled_cache) {
                    Some(item) => {
                        // Perf.
                        {
                            metrics_inc_cache_hit_count(1, &self.name);
                        }

                        Ok(item)
                    }
                    None => {
                        let start = Instant::now();

                        let (v, cache_key) = self.loader.load_with_cache_key(params).await?;
                        let item = Arc::new(v);

                        // Perf.
                        {
                            metrics_inc_cache_miss_count(1, &self.name);
                            metrics_inc_cache_miss_load_millisecond(
                                start.elapsed().as_millis() as u64,
                                &self.name,
                            );
                        }

                        let mut cache_guard = labeled_cache.write();
                        cache_guard.put(cache_key, item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get_cached(&self, key: &str, cache: &RwLock<C>) -> Option<Arc<T>> {
        cache.write().get(key).cloned()
    }
}
