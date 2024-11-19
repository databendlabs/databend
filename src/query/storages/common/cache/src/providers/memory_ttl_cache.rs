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

use databend_common_cache::Cache;
use databend_common_cache::TtlCache;
use parking_lot::RwLock;

use crate::caches::CacheValue;
use crate::Unit;

pub struct InMemoryTtlCache<V: Into<CacheValue<V>>> {
    unit: Unit,
    name: String,
    inner: Arc<RwLock<TtlCache<String, CacheValue<V>>>>,
}

impl<V: Into<CacheValue<V>>> Clone for InMemoryTtlCache<V> {
    fn clone(&self) -> Self {
        Self {
            unit: self.unit,
            name: self.name.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<V: Into<CacheValue<V>>> InMemoryTtlCache<V> {
    pub fn with_items_capacity(name: String, items_capacity: usize, ttl: Duration) -> Self {
        Self {
            name,
            unit: Unit::Count,
            inner: Arc::new(RwLock::new(TtlCache::with_items_capacity(
                items_capacity,
                ttl,
            ))),
        }
    }

    pub fn with_bytes_capacity(name: String, bytes_capacity: usize, ttl: Duration) -> Self {
        Self {
            unit: Unit::Bytes,
            name,
            inner: Arc::new(RwLock::new(TtlCache::with_bytes_capacity(
                bytes_capacity,
                ttl,
            ))),
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
    impl<V: Into<CacheValue<V>>> CacheAccessor for InMemoryTtlCache<V> {
        type V = V;

        fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<V>> {
            metrics_inc_cache_access_count(1, self.name());
            let mut guard = self.inner.write();
            match guard.get(k.as_ref()) {
                None => {
                    metrics_inc_cache_miss_count(1, &self.name);
                    None
                }
                Some(cached_value) => {
                    metrics_inc_cache_hit_count(1, &self.name);
                    Some(cached_value.get_inner())
                }
            }
        }

        fn get_batch<Q: AsRef<str>>(&self, keys: Vec<Q>) -> Vec<Option<Arc<V>>> {
            metrics_inc_cache_access_count(keys.len() as u64, self.name());

            let mut values = Vec::with_capacity(keys.len());
            let mut guard = self.inner.write();
            for k in keys {
                match guard.get(k.as_ref()) {
                    None => {
                        metrics_inc_cache_miss_count(1, &self.name);
                        values.push(None);
                    }
                    Some(cached_value) => {
                        metrics_inc_cache_hit_count(1, &self.name);
                        values.push(Some(cached_value.get_inner()));
                    }
                }
            }
            values
        }

        fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
            let Some(cached_value) = self.get(k) else {
                metrics_inc_cache_miss_bytes(len, &self.name);
                return None;
            };

            Some(cached_value)
        }

        fn insert(&self, k: String, v: V) -> Arc<V> {
            let cache_value = v.into();
            let res = cache_value.get_inner();
            let mut guard = self.inner.write();
            guard.insert(k, cache_value);
            res
        }

        fn insert_batch(&self, entries: Vec<(String, V)>) -> Vec<Arc<V>> {
            let mut results = Vec::with_capacity(entries.len());
            let mut guard = self.inner.write();
            for (k, v) in entries {
                let cache_value = v.into();
                let res = cache_value.get_inner();
                guard.insert(k, cache_value);
                results.push(res);
            }
            results
        }

        fn evict(&self, k: &str) -> bool {
            let mut guard = self.inner.write();
            guard.pop(k).is_some()
        }

        fn contains_key(&self, k: &str) -> bool {
            let guard = self.inner.read();
            guard.contains(k)
        }

        fn items_capacity(&self) -> u64 {
            let guard = self.inner.read();
            guard.items_capacity()
        }

        fn bytes_capacity(&self) -> u64 {
            let guard = self.inner.read();
            guard.bytes_capacity()
        }

        fn len(&self) -> usize {
            let guard = self.inner.read();
            guard.len()
        }

        fn bytes_size(&self) -> u64 {
            let guard = self.inner.read();
            guard.bytes_size()
        }

        fn name(&self) -> &str {
            &self.name
        }
    }
}
