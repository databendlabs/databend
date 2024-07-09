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

use std::hash::BuildHasher;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_cache::BytesMeter;
use databend_common_cache::Cache;
use databend_common_cache::Count;
use databend_common_cache::CountableMeter;
use databend_common_cache::DefaultHashBuilder;
use databend_common_cache::LruCache;
use parking_lot::RwLock;

pub type InMemoryCache<V, S, M> = LruCache<String, Arc<V>, S, M>;
pub type BytesCache = LruCache<String, Arc<Bytes>, DefaultHashBuilder, BytesMeter>;

pub type InMemoryItemCacheHolder<T, S = DefaultHashBuilder, M = Count> =
    Arc<RwLock<InMemoryCache<T, S, M>>>;
pub type InMemoryBytesCacheHolder = Arc<RwLock<BytesCache>>;

pub struct InMemoryCacheBuilder;
impl InMemoryCacheBuilder {
    // new cache that cache `V`, and metered by the given `meter`
    pub fn new_in_memory_cache<V, M>(
        capacity: u64,
        meter: M,
    ) -> InMemoryItemCacheHolder<V, DefaultHashBuilder, M>
    where
        M: CountableMeter<String, Arc<V>>,
    {
        let cache = LruCache::with_meter_and_hasher(capacity, meter, DefaultHashBuilder::default());
        Arc::new(RwLock::new(cache))
    }

    // new cache that caches `V` and meter by counting
    pub fn new_item_cache<V>(capacity: u64) -> InMemoryItemCacheHolder<V> {
        let cache = LruCache::new(capacity);
        Arc::new(RwLock::new(cache))
    }

    // new cache that cache `Vec<u8>`, and metered by byte size
    pub fn new_bytes_cache(capacity: u64) -> InMemoryBytesCacheHolder {
        let cache =
            LruCache::with_meter_and_hasher(capacity, BytesMeter, DefaultHashBuilder::default());
        Arc::new(RwLock::new(cache))
    }
}

// default impls
mod impls {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use super::*;
    use crate::cache::CacheAccessor;

    // Wrap a Cache with RwLock, and impl CacheAccessor for it
    impl<V, C, S, M> CacheAccessor<String, V, S, M> for Arc<RwLock<C>>
    where
        C: Cache<String, Arc<V>, S, M>,
        M: CountableMeter<String, Arc<V>>,
        S: BuildHasher,
    {
        fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<V>> {
            let mut guard = self.write();
            guard.get(k.as_ref()).cloned()
        }

        fn put(&self, k: String, v: Arc<V>) {
            let mut guard = self.write();
            guard.put(k, v);
        }

        fn evict(&self, k: &str) -> bool {
            let mut guard = self.write();
            guard.pop(k).is_some()
        }

        fn contains_key(&self, k: &str) -> bool {
            let guard = self.read();
            guard.contains(k)
        }

        fn size(&self) -> u64 {
            let guard = self.read();
            guard.size()
        }

        fn capacity(&self) -> u64 {
            let guard = self.read();
            guard.capacity()
        }

        fn len(&self) -> usize {
            let guard = self.read();
            guard.len()
        }
    }

    // Wrap an Option<CacheAccessor>, and impl CacheAccessor for it
    impl<V, C, S, M> CacheAccessor<String, V, S, M> for Option<C>
    where
        C: CacheAccessor<String, V, S, M>,
        M: CountableMeter<String, Arc<V>>,
        S: BuildHasher,
    {
        fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<V>> {
            self.as_ref().and_then(|cache| cache.get(k))
        }

        fn put(&self, k: String, v: Arc<V>) {
            if let Some(cache) = self {
                cache.put(k, v);
            }
        }

        fn evict(&self, k: &str) -> bool {
            if let Some(cache) = self {
                cache.evict(k)
            } else {
                false
            }
        }

        fn contains_key(&self, k: &str) -> bool {
            if let Some(cache) = self {
                cache.contains_key(k)
            } else {
                false
            }
        }

        fn size(&self) -> u64 {
            if let Some(cache) = self {
                cache.size()
            } else {
                0
            }
        }

        fn capacity(&self) -> u64 {
            if let Some(cache) = self {
                cache.capacity()
            } else {
                0
            }
        }

        fn len(&self) -> usize {
            if let Some(cache) = self {
                cache.len()
            } else {
                0
            }
        }
    }
}
