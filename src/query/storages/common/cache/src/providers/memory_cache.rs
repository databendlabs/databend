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

use databend_common_cache::Cache;
use databend_common_cache::Count;
use databend_common_cache::CountableMeter;
use databend_common_cache::LruCache;
use parking_lot::RwLock;

use crate::Unit;

pub struct InMemoryLruCache<V, M: CountableMeter<String, Arc<V>> = Count> {
    unit: Unit,
    name: String,
    inner: Arc<RwLock<LruCache<String, Arc<V>, M>>>,
}

impl<V, M: CountableMeter<String, Arc<V>>> Clone for InMemoryLruCache<V, M> {
    fn clone(&self) -> Self {
        Self {
            unit: self.unit,
            name: self.name.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<V, M: CountableMeter<String, Arc<V>>> InMemoryLruCache<V, M> {
    pub fn create(name: String, unit: Unit, cache: LruCache<String, Arc<V>, M>) -> Self {
        Self {
            unit,
            name,
            inner: Arc::new(RwLock::new(cache)),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn unit(&self) -> Unit {
        self.unit
    }
}

// default impls
mod impls {
    use std::sync::Arc;

    use databend_common_metrics::cache::metrics_inc_cache_access_count;
    use databend_common_metrics::cache::metrics_inc_cache_hit_count;
    use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
    use databend_common_metrics::cache::metrics_inc_cache_miss_count;

    use super::*;
    use crate::cache::CacheAccessor;

    // Wrap a Cache with RwLock, and impl CacheAccessor for it
    impl<V, M: CountableMeter<String, Arc<V>>> CacheAccessor for InMemoryLruCache<V, M> {
        type V = V;
        type M = M;

        fn name(&self) -> &str {
            &self.name
        }

        fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<V>> {
            metrics_inc_cache_access_count(1, self.name());
            let mut guard = self.inner.write();
            match guard.get(k.as_ref()).cloned() {
                None => {
                    metrics_inc_cache_miss_count(1, &self.name);
                    None
                }
                Some(cached_value) => {
                    metrics_inc_cache_hit_count(1, &self.name);
                    Some(cached_value)
                }
            }
        }

        fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
            let Some(cached_value) = self.get(k) else {
                metrics_inc_cache_miss_bytes(len, &self.name);
                return None;
            };

            Some(cached_value)
        }

        fn put(&self, k: String, v: Arc<V>) {
            let mut guard = self.inner.write();
            guard.insert(k, v);
        }

        fn evict(&self, k: &str) -> bool {
            let mut guard = self.inner.write();
            guard.pop(k).is_some()
        }

        fn contains_key(&self, k: &str) -> bool {
            let guard = self.inner.read();
            guard.contains(k)
        }

        fn size(&self) -> u64 {
            let guard = self.inner.read();
            guard.size()
        }

        fn capacity(&self) -> u64 {
            let guard = self.inner.read();
            guard.capacity()
        }

        fn len(&self) -> usize {
            let guard = self.inner.read();
            guard.len()
        }
    }

    // Wrap an Option<CacheAccessor>, and impl CacheAccessor for it
    // impl<K, V, M> CacheAccessor<K, V, M>
    impl<T: CacheAccessor> CacheAccessor for Option<T> {
        type V = T::V;
        type M = T::M;

        fn name(&self) -> &str {
            match self.as_ref() {
                None => "Unknown",
                Some(v) => v.name(),
            }
        }

        fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Self::V>> {
            let Some(inner_cache) = self.as_ref() else {
                metrics_inc_cache_access_count(1, self.name());
                metrics_inc_cache_miss_count(1, self.name());
                return None;
            };

            inner_cache.get(k)
        }

        fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
            let Some(inner_cache) = self.as_ref() else {
                metrics_inc_cache_access_count(1, self.name());
                metrics_inc_cache_miss_count(1, self.name());
                metrics_inc_cache_miss_bytes(len, self.name());
                return None;
            };

            inner_cache.get_sized(k, len)
        }

        fn put(&self, k: String, v: Arc<Self::V>) {
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
            match self.as_ref() {
                None => 0,
                Some(cache) => cache.len(),
            }
        }
    }
}
