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
use std::sync::atomic::AtomicBool;

use bytes::Bytes;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::Codec;
use databend_storages_common_cache::DiskCacheAccessor;
use databend_storages_common_cache::DiskCacheBuilder;
use databend_storages_common_cache::DiskCodec;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::VectorIndexFile;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::vector;

use crate::fakes::shared_store;
use crate::fakes::wait_for;

/// Bloom filter stand-in with a disk encoding; an empty payload fails to decode.
struct Blob(Vec<u8>);

impl CachedItem for Blob {
    fn mem_bytes(&self) -> usize {
        self.0.len()
    }
}

impl DiskCodec for Blob {
    fn encode(&self) -> databend_common_exception::Result<Vec<u8>> {
        Ok(self.0.clone())
    }

    fn decode(bytes: Bytes) -> databend_common_exception::Result<Self> {
        if bytes.is_empty() {
            return Err(ErrorCode::Internal("empty"));
        }
        Ok(Blob(bytes.to_vec()))
    }
}

fn allowed() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(true))
}

fn disk(dir: &tempfile::TempDir) -> Arc<DiskCacheAccessor> {
    Arc::new(
        DiskCacheBuilder::try_build_disk_cache(
            "disk_cache_test".to_string(),
            &dir.path().to_path_buf(),
            16,
            1 << 20,
            DiskCacheKeyReloadPolicy::Reset,
            false,
        )
        .unwrap(),
    )
}

#[test]
fn test_disk_tier_refills_memory() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let store = shared_store(0, 1 << 20, 0);
    let cache = MemCache::<Blob>::new(store.clone(), CacheKind::of::<bloom::Filter>()).with_disk(
        disk.clone(),
        allowed(),
        Codec::of(),
    );

    cache.put("k", Blob(vec![1; 10]));
    wait_for(|| disk.contains_key("k"));

    store.evict(CacheKind::of::<bloom::Filter>(), "k");
    let hit = cache.lookup("k").unwrap();
    assert_eq!(hit.0, vec![1; 10]);
    assert!(hit.is_charged());
}

#[test]
fn test_disk_decode_failure_evicts_disk_entry() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let cache = MemCache::<Blob>::new(
        shared_store(0, 1 << 20, 0),
        CacheKind::of::<bloom::Filter>(),
    )
    .with_disk(disk.clone(), allowed(), Codec::of());

    disk.insert("bad".to_string(), Bytes::new());
    wait_for(|| disk.contains_key("bad"));
    assert!(cache.lookup("bad").is_none());
    assert!(!disk.contains_key("bad"));
}

/// As before the refactor, a memory capacity of 0 leaves the disk tier working: the default
/// column data cache is disk only.
#[test]
fn test_disk_tier_works_without_memory_tier() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let store = shared_store(0, 1 << 20, 0);
    let cache = MemCache::<Blob>::new(store.clone(), CacheKind::of::<bloom::Filter>()).with_disk(
        disk.clone(),
        allowed(),
        Codec::of(),
    );
    cache.put("k", Blob(vec![1; 10]));
    wait_for(|| disk.contains_key("k"));

    CacheManager::new(store.clone(), Default::default())
        .set_cache_capacity("memory_cache_bloom_index_filter", 0)
        .unwrap();
    assert!(cache.is_enabled());
    assert_eq!(cache.lookup("k").unwrap().0, vec![1; 10]);
    assert!(cache.contains("k"));
    assert!(!cache.put("other", Blob(vec![2; 10])).is_charged());
    wait_for(|| disk.contains_key("other"));
    assert_eq!(store.kind_stats(CacheKind::of::<bloom::Filter>()).items, 0);

    let memory_only = MemCache::<Blob>::new(store, CacheKind::of::<bloom::Filter>());
    assert!(!memory_only.is_enabled());
}

/// With disk tiers switched off at runtime, a memory miss does not fall back to disk.
#[test]
fn test_disk_tiers_switched_off() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let store = shared_store(0, 1 << 20, 0);
    let kind = CacheKind::of::<vector::Refine>();
    let manager = CacheManager::new(store.clone(), [(kind, disk.clone())].into());
    let cache = manager.cache::<vector::Refine>();
    let page = || VectorIndexFile::create("p".to_string(), Bytes::from_static(b"page"));
    cache.put("k", page());
    wait_for(|| disk.contains_key("k"));

    manager.set_disk_allowed(false);
    assert!(!manager.rows().iter().any(|r| r.name == "disk_cache_test"));
    store.evict(kind, "k");
    assert!(cache.lookup("k").is_none());
    cache.put("other", page());
    assert!(!disk.contains_key("other"));
    manager.set_disk_allowed(true);
    assert_eq!(cache.lookup("k").unwrap().data, page().data);

    let rows = manager.rows();
    let disk_row = rows.iter().find(|r| r.name == "disk_cache_test").unwrap();
    assert_eq!((disk_row.kind, disk_row.num_items), (Some(kind), 1));
    assert!(disk_row.capacity.unwrap() > 0);
}
