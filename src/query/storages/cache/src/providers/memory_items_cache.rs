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

use std::sync::Arc;

use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;
use opendal::Object;
use parking_lot::RwLock;

use crate::CacheSettings;
use crate::CachedObject;
use crate::ObjectCacheProvider;

type ItemCache<T> = RwLock<LruCache<String, Arc<T>, DefaultHashBuilder, Count>>;

/// Memory LRU cache for item.
pub struct MemoryItemsCache<T> {
    lru: ItemCache<T>,
    settings: CacheSettings,
}

impl<T> MemoryItemsCache<T> {
    pub fn create(settings: &CacheSettings) -> MemoryItemsCache<T> {
        Self {
            lru: RwLock::new(LruCache::new(settings.memory_items_cache_capacity)),
            settings: settings.clone(),
        }
    }

    fn get_cache(&self, key: &str) -> Option<Arc<T>> {
        self.lru.write().get(key).cloned()
    }
}

#[async_trait::async_trait]
impl<T> ObjectCacheProvider<T> for MemoryItemsCache<T>
where T: CachedObject + Send + Sync
{
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Arc<T>> {
        let key = object.path().to_string();
        let try_get_val = self.get_cache(&key);

        Ok(match try_get_val {
            None => {
                let data = object.range_read(start..end).await?;
                let v = T::from_bytes(data)?;
                // Write to cache.
                self.lru.write().put(key, v.clone());

                v
            }
            Some(v) => v,
        })
    }

    async fn write_object(&self, object: &Object, v: Arc<T>) -> Result<()> {
        if self.settings.cache_on_write {
            let key = object.path().to_string();
            self.lru.write().put(key, v.clone());
        }

        let data = v.to_bytes()?;

        object.write(data).await?;
        Ok(())
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        let key = object.path();

        // Try to remove from the cache.
        self.lru.write().pop(key);

        object.delete().await?;
        Ok(())
    }
}
