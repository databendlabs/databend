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

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use log::warn;

use crate::CacheAccessor;
use crate::CacheValue;
use crate::DiskCacheAccessor;
use crate::InMemoryLruCache;

pub struct HybridCache<V: Into<CacheValue<V>>> {
    name: String,
    memory_cache: Option<InMemoryLruCache<V>>,
    disk_cache: Option<DiskCacheAccessor>,
}

impl<T: Into<CacheValue<T>>> HybridCache<T> {
    pub fn in_memory_cache_name(name: &str) -> String {
        format!("memory_{name}")
    }
    pub fn on_disk_cache_name(name: &str) -> String {
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
        memory_cache: Option<InMemoryLruCache<V>>,
        disk_cache: Option<DiskCacheAccessor>,
    ) -> Option<Self> {
        if memory_cache.is_none() && disk_cache.is_none() {
            None
        } else {
            Some(Self {
                name,
                memory_cache,
                disk_cache,
            })
        }
    }

    fn toggle_off_disk_cache_impl(self) -> Self {
        if self.disk_cache.is_some() {
            Self {
                name: self.name,
                memory_cache: self.memory_cache,
                disk_cache: None,
            }
        } else {
            self
        }
    }
    pub fn in_memory_cache(&self) -> Option<&InMemoryLruCache<V>> {
        self.memory_cache.as_ref()
    }

    pub fn on_disk_cache(&self) -> Option<&DiskCacheAccessor> {
        self.disk_cache.as_ref()
    }
}

// Crate internal helper methods
pub trait HybridCacheExt<V: Into<CacheValue<V>>> {
    fn toggle_off_disk_cache(self) -> Self;
    fn in_memory_cache(&self) -> Option<&InMemoryLruCache<V>>;
    fn on_disk_cache(&self) -> Option<&DiskCacheAccessor>;
}

impl<V: Into<CacheValue<V>>> HybridCacheExt<V> for Option<HybridCache<V>> {
    fn toggle_off_disk_cache(self) -> Self {
        self.map(|cache| cache.toggle_off_disk_cache_impl())
    }

    fn in_memory_cache(&self) -> Option<&InMemoryLruCache<V>> {
        match self {
            None => None,
            Some(v) => match &v.memory_cache {
                None => None,
                Some(v) => Some(v),
            },
        }
    }

    fn on_disk_cache(&self) -> Option<&DiskCacheAccessor> {
        match self {
            None => None,
            Some(v) => match &v.disk_cache {
                None => None,
                Some(v) => Some(v),
            },
        }
    }
}

impl<T: Into<CacheValue<T>>> HybridCache<T>
where
    Vec<u8>: for<'a> TryFrom<&'a T, Error = ErrorCode>,
    T: TryFrom<Bytes, Error = ErrorCode>,
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
    T: TryFrom<Bytes, Error = ErrorCode>,
{
    type V = T;

    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Self::V>> {
        if let Some(item) = self.memory_cache.get(k.as_ref()) {
            // Record memory cache hit
            // Note: The actual size recording happens in memory_cache.get()
            // try putting it bach to on-disk cache if necessary
            self.insert_to_disk_cache_if_necessary(k.as_ref(), item.as_ref());
            Some(item)
        } else if let Some(bytes) = self.disk_cache.get(k.as_ref()) {
            // Record disk cache hit
            // Note: The actual size recording happens in disk_cache.get()
            let bytes = bytes.as_ref().clone();
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
            // Cache Miss - will need to read from remote
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
        self.memory_cache.contains_key(k) || self.disk_cache.contains_key(k)
    }

    fn bytes_size(&self) -> u64 {
        self.memory_cache.bytes_size() + self.disk_cache.bytes_size()
    }

    fn items_capacity(&self) -> u64 {
        self.memory_cache.items_capacity() + self.disk_cache.items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        self.memory_cache.bytes_capacity() + self.disk_cache.bytes_capacity()
    }

    fn len(&self) -> usize {
        self.memory_cache.len() + self.disk_cache.len()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn clear(&self) {
        // Only clear the in-memory cache
        self.memory_cache.clear()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_cache::MemSized;
    use databend_common_config::DiskCacheKeyReloadPolicy;
    use databend_common_exception::ErrorCode;
    use parquet::data_type::AsBytes;
    use tempfile::TempDir;

    use crate::providers::hybrid_cache::HybridCacheExt;
    use crate::CacheAccessor;
    use crate::CacheValue;
    use crate::DiskCacheBuilder;
    use crate::HybridCache;
    use crate::InMemoryLruCache;

    // Custom test data type for serialization/deserialization testing
    #[derive(Clone, Debug, PartialEq)]
    struct TestData {
        id: u32,
        value: String,
    }

    // Implement TryFrom for serialization
    impl TryFrom<&TestData> for Vec<u8> {
        type Error = ErrorCode;
        fn try_from(data: &TestData) -> Result<Self, Self::Error> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&data.id.to_le_bytes());
            buf.extend_from_slice(data.value.as_bytes());
            Ok(buf)
        }
    }

    // Implement TryFrom for deserialization
    impl TryFrom<Bytes> for TestData {
        type Error = ErrorCode;
        fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
            let bytes = bytes.as_bytes();
            if bytes.len() < 4 {
                return Err(ErrorCode::BadBytes(
                    "Not enough bytes for TestData".to_string(),
                ));
            }

            let id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let value = String::from_utf8(bytes[4..].to_vec())
                .map_err(|e| ErrorCode::BadBytes(format!("Invalid UTF-8: {}", e)))?;

            Ok(TestData { id, value })
        }
    }

    // Implement CacheValue creation for TestData
    impl From<TestData> for CacheValue<TestData> {
        fn from(data: TestData) -> Self {
            // Calculate approximate size: size of structure + size of string data
            let mem_bytes = std::mem::size_of::<TestData>() + data.value.len();
            CacheValue::new(data, mem_bytes)
        }
    }

    // Implement MemSized for TestData to track memory usage
    impl MemSized for TestData {
        fn mem_bytes(&self) -> usize {
            std::mem::size_of::<TestData>() + self.value.len()
        }
    }

    #[test]
    fn test_hybrid_cache_creation() {
        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create hybrid cache without disk cache
        let cache = HybridCache::new("test_hybrid".to_string(), Some(memory_cache.clone()), None);

        assert_eq!(cache.name(), "test_hybrid");
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_hybrid_cache_with_disk_cache() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create disk cache
        let table_cache = DiskCacheBuilder::try_build_disk_cache(
            "test_disk".to_string(),
            &cache_path,
            100,         // population_queue_size
            1024 * 1024, // disk_cache_bytes_size
            DiskCacheKeyReloadPolicy::Fuzzy,
            false, // sync_data
        )
        .unwrap();

        // Create hybrid cache
        let cache = HybridCache::new(
            "test_hybrid".to_string(),
            Some(memory_cache),
            Some(table_cache),
        );

        assert_eq!(cache.name(), "test_hybrid");
        assert!(cache.is_empty());
    }

    #[test]
    fn test_hybrid_cache_insert_get() {
        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create hybrid cache without disk cache
        let cache = HybridCache::new("test_hybrid".to_string(), Some(memory_cache), None);

        // Insert data
        let test_data = TestData {
            id: 1,
            value: "test value".to_string(),
        };
        let key = "test_key".to_string();
        let _inserted = cache.insert(key.clone(), test_data.clone());

        // Retrieve data
        let retrieved = cache.get(&key).expect("Failed to retrieve data");

        assert_eq!(*retrieved, test_data);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key));
    }

    #[test]
    fn test_hybrid_cache_memory_to_disk_fallback() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create memory cache (small capacity for testing eviction)
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 2);

        // Create disk cache
        let table_cache = DiskCacheBuilder::try_build_disk_cache(
            "test_disk".to_string(),
            &cache_path,
            100,         // population_queue_size
            1024 * 1024, // disk_cache_bytes_size
            DiskCacheKeyReloadPolicy::Fuzzy,
            false, // sync_data
        )
        .unwrap();

        // Create hybrid cache
        let cache = HybridCache::new(
            "test_hybrid".to_string(),
            Some(memory_cache),
            Some(table_cache),
        );

        // Insert data
        let test_data1 = TestData {
            id: 1,
            value: "value1".to_string(),
        };
        let test_data2 = TestData {
            id: 2,
            value: "value2".to_string(),
        };
        let test_data3 = TestData {
            id: 3,
            value: "value3".to_string(),
        };

        cache.insert("key1".to_string(), test_data1.clone());
        cache.insert("key2".to_string(), test_data2.clone());

        // This will cause key1 to be evicted from memory cache, but it should exist in disk cache
        cache.insert("key3".to_string(), test_data3.clone());
        assert!(!cache.in_memory_cache().unwrap().contains_key("key1"));

        let disk_cache = cache.on_disk_cache().unwrap();
        // Since the on-disk cache is populated asynchronously, we need to
        // wait until the populate-queue is empty. At that time, at least
        // the first key `test_key` should be written into the on-disk cache.
        disk_cache.till_no_pending_items_in_queue();

        // key1 should be retrieved from disk cache
        let retrieved1 = cache
            .get("key1")
            .expect("Failed to retrieve data from disk");
        assert_eq!(*retrieved1, test_data1);

        // key3 should be in memory cache
        let retrieved3 = cache
            .get("key3")
            .expect("Failed to retrieve data from memory");
        assert_eq!(*retrieved3, test_data3);
    }

    #[test]
    fn test_disk_cache_hit_populates_memory_cache() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create memory cache with very small capacity (1 item)
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 1);

        // Create disk cache
        let table_cache = DiskCacheBuilder::try_build_disk_cache(
            "test_disk".to_string(),
            &cache_path,
            100,         // population_queue_size
            1024 * 1024, // disk_cache_bytes_size
            DiskCacheKeyReloadPolicy::Fuzzy,
            false, // sync_data
        )
        .unwrap();

        // Create hybrid cache
        let cache = HybridCache::new(
            "test_hybrid".to_string(),
            Some(memory_cache.clone()),
            Some(table_cache),
        );

        // Create test data
        let test_data = TestData {
            id: 42,
            value: "disk_cache_test".to_string(),
        };

        // Insert data using standard API - this will put it in both caches
        cache.insert("test_key".to_string(), test_data.clone());

        // Add a filler item to potentially evict the first item from memory
        cache.insert("filler_key".to_string(), TestData {
            id: 99,
            value: "filler".to_string(),
        });

        // Verify the memory cache doesn't have our test key anymore
        assert!(!cache.in_memory_cache().unwrap().contains_key("test_key"));

        // Since the on-disk cache is populated asynchronously, we need to
        // wait until the populate-queue is empty. At that time, at least
        // the first key `test_key` should be written into the on-disk cache.
        cache
            .on_disk_cache()
            .as_ref()
            .unwrap()
            .till_no_pending_items_in_queue();

        // First access - should hit disk cache and populate memory cache
        let first_access = cache
            .get("test_key")
            .expect("Failed to retrieve from disk cache");
        assert_eq!(first_access.as_ref().id, 42);

        // Verify it's now in memory cache after the first access
        assert!(cache.in_memory_cache().unwrap().contains_key("test_key"));

        // Second access - should now hit memory cache
        let second_access = cache
            .get("test_key")
            .expect("Failed to retrieve from memory cache");
        assert_eq!(second_access.as_ref().id, 42);
    }

    #[test]
    fn test_hybrid_cache_evict() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create disk cache
        let table_cache = DiskCacheBuilder::try_build_disk_cache(
            "test_disk".to_string(),
            &cache_path,
            100,         // population_queue_size
            1024 * 1024, // disk_cache_bytes_size
            DiskCacheKeyReloadPolicy::Fuzzy,
            false, // sync_data
        )
        .unwrap();

        // Create hybrid cache
        let cache = HybridCache::new(
            "test_hybrid".to_string(),
            Some(memory_cache),
            Some(table_cache),
        );

        // Insert data
        let test_data = TestData {
            id: 1,
            value: "test value".to_string(),
        };
        let key = "test_key".to_string();
        cache.insert(key.clone(), test_data);

        // A padding item, it will be pushed into the populate-queue after "test_key"
        cache.insert("padding".to_string(), TestData {
            id: 2,
            value: "does not matter".to_string(),
        });

        // Although the on-disk cache is populated asynchronously, items queued in the populate-queue
        // are processed one by one. To ensure that key "test_key" has been processed, we could wait
        // until the populate-queue is empty. At that time, at least the first key `test_key`
        // should have been written into the on-disk cache.
        cache
            .on_disk_cache()
            .as_ref()
            .unwrap()
            .till_no_pending_items_in_queue();

        // Now the on-disk cache item of "test_key" is stabilized: no item of it being processed
        // asynchronously, e.g., there should be no DISK cache of key "test_key" being created
        // while the HYBRID cache has evicted it.

        // Verify data exists
        assert!(cache.contains_key(&key));

        // Evict data
        let evicted = cache.evict(&key);
        assert!(evicted);

        // Verify data doesn't exist
        assert!(!cache.contains_key(&key));
        assert_eq!(cache.get(&key), None);
    }

    #[test]
    fn test_hybrid_cache_metrics() {
        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create hybrid cache without disk cache
        let cache = HybridCache::new("test_hybrid".to_string(), Some(memory_cache), None);

        // Insert multiple items
        for i in 0..5 {
            let test_data = TestData {
                id: i,
                value: format!("value{}", i),
            };
            cache.insert(format!("key{}", i), test_data);
        }

        // Verify cache metrics
        assert_eq!(cache.len(), 5);
        assert!(!cache.is_empty());
        assert!(cache.bytes_size() > 0);
    }

    #[test]
    fn test_toggle_off_disk_cache() {
        // Case 1: Test with disk cache present

        // Create temporary directory for disk cache
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create memory cache
        let memory_cache: InMemoryLruCache<TestData> =
            InMemoryLruCache::with_items_capacity("test_memory".to_string(), 100);

        // Create disk cache
        let disk_cache = DiskCacheBuilder::try_build_disk_cache(
            "test_disk".to_string(),
            &cache_path,
            100,         // population_queue_size
            1024 * 1024, // disk_cache_bytes_size
            DiskCacheKeyReloadPolicy::Fuzzy,
            false, // sync_data
        )
        .unwrap();

        // Create hybrid cache with disk cache
        let cache_with_disk = HybridCache::new(
            "test_hybrid_with_disk".to_string(),
            Some(memory_cache.clone()),
            Some(disk_cache),
        );

        // Verify disk cache is present
        assert!(cache_with_disk.on_disk_cache().is_some());

        // Toggle off disk cache
        let cache_without_disk = cache_with_disk.toggle_off_disk_cache();

        // Verify disk cache is now None
        assert!(cache_without_disk.on_disk_cache().is_none());
        // Verify other properties remain the same
        assert_eq!(cache_without_disk.name(), "test_hybrid_with_disk");

        // Case 2: Test when disk cache is already None

        // Create hybrid cache without disk cache
        let cache_no_disk =
            HybridCache::new("test_hybrid_no_disk".to_string(), Some(memory_cache), None);

        // Verify disk cache is None
        assert!(cache_no_disk.on_disk_cache().is_none());

        // Toggle off disk cache (should be no-op)
        let unchanged_cache = cache_no_disk.toggle_off_disk_cache();

        // Verify disk cache is still None
        assert!(unchanged_cache.on_disk_cache().is_none());
        // Verify other properties remain the same
        assert_eq!(unchanged_cache.name(), "test_hybrid_no_disk");
    }
}
