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

use common_cache::BytesMeter;
use common_cache::Cache;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;
use opendal::Object;
use parking_lot::RwLock;

use crate::CacheSettings;
use crate::ObjectCacheProvider;

type BytesCache = RwLock<LruCache<String, Arc<Vec<u8>>, DefaultHashBuilder, BytesMeter>>;

/// Memory LRU cache for bytes(Vec<u8>).
pub struct MemoryBytesCache {
    lru: BytesCache,
    settings: CacheSettings,
}

impl MemoryBytesCache {
    pub fn create(settings: &CacheSettings) -> MemoryBytesCache {
        Self {
            lru: RwLock::new(LruCache::with_meter_and_hasher(
                settings.memory_bytes_cache_capacity,
                BytesMeter,
                DefaultHashBuilder::new(),
            )),
            settings: settings.clone(),
        }
    }

    fn get_cache(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.lru.write().get(key).cloned()
    }
}

#[async_trait::async_trait]
impl ObjectCacheProvider<Vec<u8>> for MemoryBytesCache {
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Arc<Vec<u8>>> {
        let key = object.path().to_string();
        let try_get_val = self.get_cache(&key);
        Ok(match try_get_val {
            None => {
                let data = object.range_read(start..end).await?;
                let v = Arc::new(data);

                self.lru.write().put(key, v.clone());

                v
            }
            Some(v) => v,
        })
    }

    async fn write_object(&self, object: &Object, v: Arc<Vec<u8>>) -> Result<()> {
        if self.settings.cache_on_write {
            let key = object.path().to_string();
            self.lru.write().put(key, v.clone());
        }

        object.write(v.as_slice()).await?;

        Ok(())
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        object.delete().await?;
        Ok(())
    }
}
