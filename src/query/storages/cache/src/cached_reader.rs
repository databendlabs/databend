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

use std::sync::Arc;

use common_cache::Cache;
use common_exception::Result;
use common_storages_table_meta::caches::CacheDeferMetrics;
use common_storages_table_meta::caches::LabeledItemCache;

/// Loads an object from a source
#[async_trait::async_trait]
pub trait Loader<T> {
    /// Loads object of type T, located at `location`
    async fn load(&self, location: &str, len_hint: Option<u64>, ver: u64) -> Result<T>;
}

/// A "cache-aware" reader
pub struct CachedReader<T, L> {
    cache: Option<LabeledItemCache<T>>,
    name: String,
    dal: L,
}

impl<T, L> CachedReader<T, L>
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
    pub async fn read(
        &self,
        path: impl AsRef<str>,
        len_hint: Option<u64>,
        version: u64,
    ) -> Result<Arc<T>> {
        match &self.cache {
            None => self.load(path.as_ref(), len_hint, version).await,
            Some(labeled_cache) => {
                // in PR #3798, the cache is degenerated to metered by count of cached item,
                // later, when the size of BlockMeta could be acquired (needs some enhancements of crate `parquet2`)
                // 1) the `read_bytes` metric should be re-enabled
                // 2) the metrics need to be labeled by the name of cache as well

                let mut metrics = CacheDeferMetrics {
                    tenant_label: labeled_cache.label(),
                    name: &self.name,
                    cache_hit: false,
                    read_bytes: 0,
                };

                match self.get_by_cache(path.as_ref(), labeled_cache) {
                    Some(item) => {
                        metrics.cache_hit = true;
                        metrics.read_bytes = 0u64;
                        Ok(item)
                    }
                    None => {
                        let item = self.load(path.as_ref(), len_hint, version).await?;
                        let mut cache_guard = labeled_cache.write();
                        cache_guard.put(path.as_ref().to_owned(), item.clone());
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

    async fn load(&self, loc: &str, len_hint: Option<u64>, version: u64) -> Result<Arc<T>> {
        let val = self.dal.load(loc, len_hint, version).await?;
        let item = Arc::new(val);
        Ok(item)
    }
}
