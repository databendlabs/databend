// Copyright 2026 Datafuse Labs
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

use crate::MemCache;
use crate::Reader;
use crate::handle::cache_ref::CacheRef;

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
            return self.load(key, false).await;
        };
        if !item.is_expired(&self.ttl) {
            return Ok(item);
        }
        if item.requires_sync_refresh() {
            return self.load(key, true).await;
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
                        // A refresh replaces the entry, so it bypasses admission. If the
                        // cache still rejects it, drop the stale entry: it is marked refreshing
                        // and would otherwise be served forever.
                        if cache.is_enabled() && !cache.put(&key, value).is_charged() {
                            cache.evict(&key);
                        }
                    }
                    Err(e) => {
                        log::warn!("fail to refresh ttl value for key {}, {}", key, e);
                        // Not refreshed: the stale entry is dropped, so the next read loads
                        // it again instead of serving it forever.
                        cache.evict(&key);
                    }
                }
            });
        }
        Ok(item)
    }

    /// Reloads the value now and caches it.
    #[async_backtrace::framed]
    pub async fn refresh(&self, key: &str) -> Result<()> {
        self.load(key, true).await.map(|_| ())
    }

    pub fn remove(&self, key: &str) -> bool {
        self.cache.evict(key)
    }

    /// With `replace`, the loaded value supersedes a cached one: it bypasses admission, and if
    /// the cache still rejects it the old value is dropped, so it is not served afterwards.
    async fn load(&self, key: &str, replace: bool) -> Result<CacheRef<R::Item>> {
        let start = Instant::now();
        let value = self.reader.read(&key.to_string()).await?;
        if !self.cache.is_enabled() {
            return Ok(self.cache.fill(key, value));
        }
        metrics_inc_cache_miss_load_millisecond(
            start.elapsed().as_millis() as u64,
            self.cache.name(),
        );
        if !replace {
            return Ok(self.cache.fill(key, value));
        }
        let item = self.cache.put(key, value);
        if !item.is_charged() {
            self.cache.evict(key);
        }
        Ok(item)
    }
}
