//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;

use crate::storages::fuse::cache::metrics::CacheDeferMetrics;
use crate::storages::fuse::cache::metrics::TenantLabel;

/// Loads an object from a source
#[async_trait::async_trait]
pub trait Loader<T> {
    /// Loads object of type T, located at `location`
    async fn load(&self, location: &str, len_hint: Option<u64>) -> Result<T>;
}

pub trait HasTenantLabel {
    fn tenant_label(&self) -> TenantLabel;
}

type LaCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;
pub type MemoryCache<V> = Arc<RwLock<LaCache<String, Arc<V>>>>;

pub fn new_memory_cache<V>(capacity: u64) -> MemoryCache<V> {
    Arc::new(RwLock::new(LruCache::new(capacity)))
}

/// A "cache-aware" reader
pub struct CachedReader<V, L> {
    cache: Option<MemoryCache<V>>,
    loader: L,
    name: String,
}

impl<V, L> CachedReader<V, L>
where L: Loader<V> + HasTenantLabel
{
    pub fn new(cache: Option<MemoryCache<V>>, loader: L, name: impl Into<String>) -> Self {
        Self {
            cache,
            loader,
            name: name.into(),
        }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    pub async fn read(&self, location: impl AsRef<str>) -> Result<Arc<V>> {
        match &self.cache {
            None => self.load(location.as_ref()).await,
            Some(cache) => {
                let tenant_label = self.loader.tenant_label();

                // in PR #3798, the cache is degenerated to metered by count of cached item,
                // later, when the size of BlockMeta could be acquired (needs some enhancements of crate `parquet2`)
                // 1) the `read_bytes` metric should be re-enabled
                // 2) the metrics need to be labeled by the name of cache as well

                let mut metrics = CacheDeferMetrics {
                    tenant_label,
                    cache_hit: false,
                    read_bytes: 0,
                };
                let cache = &mut cache.write().await;
                match cache.get(location.as_ref()) {
                    Some(item) => {
                        metrics.cache_hit = true;
                        metrics.read_bytes = 0u64;
                        Ok(item.clone())
                    }
                    None => {
                        let item = self.load(location.as_ref()).await?;
                        cache.put(location.as_ref().to_owned(), item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    async fn load(&self, loc: &str) -> Result<Arc<V>> {
        let val = self.loader.load(loc, None).await?;
        let item = Arc::new(val);
        Ok(item)
    }
}
