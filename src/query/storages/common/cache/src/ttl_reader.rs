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
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_miss_load_millisecond;

use crate::cache_ref::CacheRef;
use crate::caches::IcebergTableCacheValue;
use crate::mem_cache::MemCache;
use crate::reader::Reader;

/// A cached value that goes stale after a while.
pub trait TTLValue {
    fn is_expired(&self, ttl: &Duration) -> bool;

    /// An expired value that must not be served while it is refreshed.
    fn requires_sync_refresh(&self) -> bool {
        false
    }

    fn is_refreshing(&self) -> bool;

    fn set_refreshing(&self);
}

/// Serves cached values until they expire, then refreshes them in the background, or before
/// returning when `requires_sync_refresh`.
pub struct TTLReader<R: Reader<Key = String>>
where R::Item: TTLValue
{
    cache: MemCache<R::Item>,
    reader: Arc<R>,
    ttl: Duration,
}

impl<R> TTLReader<R>
where
    R: Reader<Key = String> + 'static,
    R::Item: TTLValue,
{
    pub fn new(cache: MemCache<R::Item>, reader: R, ttl: Duration) -> Self {
        TTLReader {
            cache,
            reader: Arc::new(reader),
            ttl,
        }
    }

    #[async_backtrace::framed]
    pub async fn read(&self, key: &str) -> Result<CacheRef<R::Item>> {
        let Some(item) = self.cache.lookup(key) else {
            return self.load(key).await;
        };
        if !item.is_expired(&self.ttl) {
            return Ok(item);
        }
        if item.requires_sync_refresh() {
            return self.load(key).await;
        }
        if !item.is_refreshing() {
            item.set_refreshing();
            let cache = self.cache.clone();
            let reader = self.reader.clone();
            let key = key.to_string();
            databend_common_base::runtime::spawn(async move {
                let start = Instant::now();
                match reader.read(&key).await {
                    Ok(value) => {
                        metrics_inc_cache_miss_load_millisecond(
                            start.elapsed().as_millis() as u64,
                            cache.name(),
                        );
                        cache.fill(&key, value);
                    }
                    Err(e) => log::warn!("fail to refresh ttl value for key {}, {}", key, e),
                }
            });
        }
        Ok(item)
    }

    /// Reloads the value now and caches it.
    #[async_backtrace::framed]
    pub async fn refresh(&self, key: &str) -> Result<()> {
        self.load(key).await.map(|_| ())
    }

    pub fn remove(&self, key: &str) -> bool {
        self.cache.evict(key)
    }

    async fn load(&self, key: &str) -> Result<CacheRef<R::Item>> {
        let start = Instant::now();
        let value = self.reader.read(&key.to_string()).await?;
        if self.cache.is_enabled() {
            metrics_inc_cache_miss_load_millisecond(
                start.elapsed().as_millis() as u64,
                self.cache.name(),
            );
        }
        Ok(self.cache.fill(key, value))
    }
}

impl TTLValue for IcebergTableCacheValue {
    fn is_expired(&self, ttl: &Duration) -> bool {
        Instant::now() > self.loaded_at() + *ttl || self.requires_sync_refresh()
    }

    fn requires_sync_refresh(&self) -> bool {
        self.credential_refresh_at()
            .is_some_and(|refresh_at| Instant::now() >= refresh_at)
    }

    fn is_refreshing(&self) -> bool {
        self.is_refreshing()
    }

    fn set_refreshing(&self) {
        self.set_refreshing();
    }
}
