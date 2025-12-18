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
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_metrics::cache::*;

use super::loader::LoadParams;
use crate::CacheAccessor;
use crate::Loader;
use crate::caches::CacheValue;

pub trait TTLValue {
    fn is_expired(&self, ttl: &Duration) -> bool;
    fn is_refreshing(&self) -> bool;
    fn set_refreshing(&self);
}

/// A cache-aware reader with ttl
pub struct CacheTTLReader<L, C> {
    cache: Option<Arc<C>>,
    loader: Arc<L>,
    ttl: Duration,
}

// InMemoryLruCache<V>
impl<V: Into<CacheValue<V>> + TTLValue, L, C> CacheTTLReader<L, C>
where
    L: Loader<V> + Sync + Send + 'static,
    C: CacheAccessor<V = V> + Sync + Send + 'static,
{
    pub fn new(cache: Option<C>, loader: L, ttl: Duration) -> Self {
        Self {
            cache: cache.map(|c| Arc::new(c)),
            loader: Arc::new(loader),
            ttl,
        }
    }
    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    /// If the value is expired, it triggers an asynchronous refresh logic and returns the expired data.
    #[async_backtrace::framed]
    pub async fn read(&self, params: &LoadParams) -> Result<Arc<V>> {
        match &self.cache {
            None => Ok(Arc::new(self.loader.load(params).await?)),
            Some(cache) => {
                let cache_key = self.loader.cache_key(params);
                match cache.get(cache_key.as_str()) {
                    Some(item) => {
                        if item.is_expired(&self.ttl) {
                            // Trigger async refresh logic
                            if !item.is_refreshing() {
                                item.set_refreshing();

                                let params = params.clone();
                                let loader = self.loader.clone();
                                let cache = cache.clone();

                                databend_common_base::runtime::spawn(async move {
                                    let start = Instant::now();
                                    let v = loader.load(&params).await;
                                    metrics_inc_cache_miss_load_millisecond(
                                        start.elapsed().as_millis() as u64,
                                        cache.name(),
                                    );

                                    match v {
                                        Ok(v) => {
                                            if params.put_cache {
                                                cache.insert(cache_key, v);
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "fail to refresh ttl value for key {}, {}",
                                                cache_key,
                                                e
                                            );
                                        }
                                    }
                                });
                            }
                        }
                        Ok(item.clone())
                    }
                    None => {
                        let start = Instant::now();
                        // Trigger sync refresh logic
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

    #[async_backtrace::framed]
    pub async fn refresh(&self, params: &LoadParams) -> Result<()> {
        match &self.cache {
            None => {
                let _ = self.loader.load(params).await?;
                Ok(())
            }
            Some(cache) => {
                let v = self.loader.load(params).await?;
                let cache_key = self.loader.cache_key(params);
                match params.put_cache {
                    true => {
                        let _ = cache.insert(cache_key, v);
                        Ok(())
                    }
                    false => Ok(()),
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub fn remove(&self, params: &LoadParams) -> bool {
        match &self.cache {
            None => false,
            Some(cache) => {
                let cache_key = self.loader.cache_key(params);
                cache.evict(cache_key.as_str())
            }
        }
    }

    pub fn name(&self) -> &str {
        self.cache.as_ref().map(|c| c.name()).unwrap_or("")
    }
}

impl TTLValue for (Arc<dyn Table>, AtomicBool, Instant) {
    fn is_expired(&self, ttl: &Duration) -> bool {
        Instant::now() > self.2 + *ttl
    }

    fn is_refreshing(&self) -> bool {
        // Check the value of the AtomicBool
        self.1.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_refreshing(&self) {
        // Set the AtomicBool to true
        self.1.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
