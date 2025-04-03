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

use databend_common_exception::ErrorCode;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use log::warn;
use parquet::data_type::AsBytes;

use crate::CacheAccessor;
use crate::CacheValue;
use crate::InMemoryLruCache;
use crate::TableDataCache;

pub struct HybridCache<V: Into<CacheValue<V>>> {
    name: String,
    memory_cache: InMemoryLruCache<V>,
    disk_cache: Option<TableDataCache>,
}

impl<T: Into<CacheValue<T>>> HybridCache<T> {
    pub fn build_in_memory_cache_name(name: &str) -> String {
        format!("memory_{name}")
    }
    pub fn build_on_disk_cache_name(name: &str) -> String {
        format!("disk_{name}")
    }
}

impl<V: Into<CacheValue<V>>> Clone for HybridCache<V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            memory_cache: self.memory_cache.clone(),
            disk_cache: self.disk_cache.clone(),
        }
    }
}

impl<V: Into<CacheValue<V>>> HybridCache<V> {
    pub fn new(
        name: String,
        memory_cache: InMemoryLruCache<V>,
        disk_cache: Option<TableDataCache>,
    ) -> Self {
        Self {
            name,
            memory_cache,
            disk_cache,
        }
    }

    pub fn in_memory_cache(&self) -> &InMemoryLruCache<V> {
        &self.memory_cache
    }

    pub fn on_disk_cache(&self) -> &Option<TableDataCache> {
        &self.disk_cache
    }
}

impl<T: Into<CacheValue<T>>> HybridCache<T>
where
    Vec<u8>: for<'a> TryFrom<&'a T, Error = ErrorCode>,
    T: for<'a> TryFrom<&'a [u8], Error = ErrorCode>,
{
    pub fn insert_to_disk_cache_if_necessary(&self, k: &str, v: &T) {
        // `None` is also a CacheAccessor, avoid serialization for it
        if let Some(cache) = &self.disk_cache {
            // check before serialization, `contains_key` is a light operation, will not hit disk
            if !cache.contains_key(k) {
                match TryInto::<Vec<u8>>::try_into(v) {
                    Ok(bytes) => {
                        cache.insert(k.to_owned(), bytes.into());
                    }
                    Err(e) => {
                        warn!("failed to encode cache value key {k}, {}", e);
                    }
                }
            }
        }
    }
}

impl<T> CacheAccessor for HybridCache<T>
where
    CacheValue<T>: From<T>,
    Vec<u8>: for<'a> TryFrom<&'a T, Error = ErrorCode>,
    T: for<'a> TryFrom<&'a [u8], Error = ErrorCode>,
{
    type V = T;

    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Self::V>> {
        if let Some(item) = self.memory_cache.get(k.as_ref()) {
            // try putting it bach to on-disk cache if necessary
            self.insert_to_disk_cache_if_necessary(k.as_ref(), item.as_ref());
            Some(item)
        } else if let Some(bytes) = self.disk_cache.get(k.as_ref()) {
            let bytes = bytes.as_bytes();
            match bytes.try_into() {
                Ok(v) => Some(self.memory_cache.insert(k.as_ref().to_owned(), v)),
                Err(e) => {
                    let key = k.as_ref();
                    // Disk cache crc is correct, but failed to deserialize.
                    // Likely the serialization format has been changed, evict it.
                    warn!("failed to decode cache value, key {key}, {}", e);
                    self.disk_cache.evict(key);
                    None
                }
            }
        } else {
            // Cache Miss
            None
        }
    }

    fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
        let Some(cached_value) = self.get(k) else {
            // Both in-memory and on-disk cache are missed, record it
            let in_memory_cache_name = self.memory_cache.name();
            let on_disk_cache_name = self.disk_cache.name();
            metrics_inc_cache_miss_bytes(len, in_memory_cache_name);
            metrics_inc_cache_miss_bytes(len, on_disk_cache_name);
            return None;
        };
        Some(cached_value)
    }

    fn insert(&self, key: String, value: Self::V) -> Arc<Self::V> {
        let v = self.memory_cache.insert(key.clone(), value);
        self.insert_to_disk_cache_if_necessary(&key, v.as_ref());
        v
    }

    fn evict(&self, k: &str) -> bool {
        self.memory_cache.evict(k) && self.disk_cache.evict(k)
    }

    fn contains_key(&self, k: &str) -> bool {
        self.memory_cache.contains_key(k) && self.disk_cache.contains_key(k)
    }

    fn bytes_size(&self) -> u64 {
        self.memory_cache.bytes_size() + self.disk_cache.bytes_size()
    }

    fn items_capacity(&self) -> u64 {
        self.disk_cache.items_capacity() + self.disk_cache.items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        self.disk_cache.bytes_capacity() + self.disk_cache.bytes_capacity()
    }

    fn len(&self) -> usize {
        self.disk_cache.len() + self.disk_cache.len()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}
