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
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use opendal::Object;
use parking_lot::RwLock;

use crate::CacheSettings;
use crate::ObjectCacheProvider;

type ItemCache<T> = RwLock<LruCache<String, Arc<T>, DefaultHashBuilder, Count>>;

/// Memory LRU item cache.
pub struct MemoryItemCache<T> {
    lru: ItemCache<T>,
    settings: CacheSettings,
}

impl<T> MemoryItemCache<T> {
    pub fn create(settings: &CacheSettings) -> MemoryItemCache<T> {
        Self {
            lru: RwLock::new(LruCache::new(settings.memory_item_capacity)),
            settings: settings.clone(),
        }
    }

    fn get_cache(&self, key: &str) -> Option<Arc<T>> {
        self.lru.write().get(key).cloned()
    }
}

#[async_trait::async_trait]
impl<T> ObjectCacheProvider<T> for MemoryItemCache<T>
where T: serde::Serialize + for<'a> serde::Deserialize<'a> + Sync + Send
{
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Arc<T>> {
        let key = object.path().to_string();
        let try_get_val = self.get_cache(&key);

        let val = match try_get_val {
            None => {
                let data = object.range_read(start..end).await?;
                let v: Arc<T> = Arc::new(
                    bincode::deserialize(&data)
                        .map_err_to_code(ErrorCode::BadBytes, || "read_object deserialize error")?,
                );

                // Write to cache.
                self.lru.write().put(key, v.clone());

                v
            }
            Some(v) => v,
        };

        Ok(val)
    }

    async fn write_object(&self, object: &Object, v: Arc<T>) -> Result<()> {
        if self.settings.cache_on_write {
            let key = object.path().to_string();
            self.lru.write().put(key, v.clone());
        }

        let data = bincode::serialize(v.as_ref())
            .map_err_to_code(ErrorCode::BadBytes, || "write object serialize error")?;
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
