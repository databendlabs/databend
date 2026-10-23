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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_metrics::cache::metrics_inc_cache_access_count;
use databend_common_metrics::cache::metrics_inc_cache_hit_count;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use databend_common_metrics::cache::metrics_inc_cache_miss_count;
use databend_storages_common_index::HNSWIndex;
use parking_lot::RwLock;

use crate::CacheAccessor;
use crate::CacheValue;

// Keep the existing name for monitoring and runtime capacity controls; its memory tier now
// retains decoded indexes rather than serialized file bytes.
pub const MEMORY_CACHE_VECTOR_INDEX: &str = "memory_cache_vector_index_file";

/// A loaded index and, when admitted to the cache, its lifetime-based memory charge.
/// Keeping this wrapper outside HNSWIndex leaves the search implementation cache-independent.
pub struct CachedVectorIndex {
    index: HNSWIndex,
    memory_size: usize,
    _reservation: Option<IndexReservation>,
}

impl CachedVectorIndex {
    pub fn new(index: HNSWIndex) -> Self {
        let memory_size = index.memory_size() + std::mem::size_of::<Self>()
            - std::mem::size_of::<HNSWIndex>()
            + 2 * std::mem::size_of::<usize>(); // Arc reference counts.
        Self {
            index,
            memory_size,
            _reservation: None,
        }
    }

    pub fn index(&self) -> &HNSWIndex {
        &self.index
    }
}

struct IndexReservation {
    bytes: usize,
    resident_bytes: Arc<AtomicUsize>,
}

impl Drop for IndexReservation {
    fn drop(&mut self) {
        self.resident_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// A byte-limited cache of decoded indexes. Its budget includes admitted objects still held
/// by readers after eviction or clearance. If readers pin the budget, new indexes are returned
/// uncached, just as on the original query-local loading path.
#[derive(Clone)]
pub struct VectorIndexCache {
    inner: Arc<RwLock<LruCache<String, CacheValue<CachedVectorIndex>>>>,
    resident_bytes: Arc<AtomicUsize>,
}

impl VectorIndexCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LruCache::with_bytes_capacity(capacity))),
            resident_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set_bytes_capacity(&self, capacity: usize) {
        // Shrinking can evict reader-held objects, but their charges live until the last reader.
        self.inner.write().set_bytes_capacity(capacity);
    }
}

impl CacheAccessor for VectorIndexCache {
    type V = CachedVectorIndex;

    fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Arc<Self::V>> {
        metrics_inc_cache_access_count(1, self.name());
        let value = self
            .inner
            .write()
            .get(key.as_ref())
            .map(CacheValue::get_inner);
        if value.is_some() {
            metrics_inc_cache_hit_count(1, self.name());
        } else {
            metrics_inc_cache_miss_count(1, self.name());
        }
        value
    }

    fn get_sized<Q: AsRef<str>>(&self, key: Q, len: u64) -> Option<Arc<Self::V>> {
        let value = self.get(key);
        if value.is_none() {
            metrics_inc_cache_miss_bytes(len, self.name());
        }
        value
    }

    fn insert(&self, key: String, mut value: Self::V) -> Arc<Self::V> {
        let mut cache = self.inner.write();
        if let Some(existing) = cache.get(key.as_str()) {
            return existing.get_inner();
        }

        let size = value.memory_size;
        let capacity = cache.bytes_capacity() as usize;
        // Reject oversized entries before evicting any useful cached indexes.
        if size > capacity {
            return Arc::new(value);
        }

        while self.resident_bytes.load(Ordering::Relaxed) > capacity - size {
            // Evict the least-recently-used unpinned entry. Dropping a reader-held entry cannot
            // make room, and would needlessly discard a useful cache reference.
            let victim = cache
                .iter()
                .find(|(_, entry)| !entry.is_shared())
                .map(|(key, _)| key.clone());
            let Some(victim) = victim else {
                return Arc::new(value);
            };
            cache.pop(victim.as_str());
        }

        self.resident_bytes.fetch_add(size, Ordering::Relaxed);
        value._reservation = Some(IndexReservation {
            bytes: size,
            resident_bytes: self.resident_bytes.clone(),
        });
        let value = CacheValue::new(value, size);
        let result = value.get_inner();
        cache.insert(key, value);
        result
    }

    fn evict(&self, key: &str) -> bool {
        self.inner.write().pop(key).is_some()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.inner.read().contains(key)
    }

    fn bytes_size(&self) -> u64 {
        self.resident_bytes.load(Ordering::Relaxed) as u64
    }

    fn items_capacity(&self) -> u64 {
        self.inner.read().items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        self.inner.read().bytes_capacity()
    }

    fn len(&self) -> usize {
        self.inner.read().len()
    }

    fn name(&self) -> &str {
        MEMORY_CACHE_VECTOR_INDEX
    }

    fn clear(&self) {
        self.inner.write().clear();
    }
}

#[cfg(test)]
mod tests;
