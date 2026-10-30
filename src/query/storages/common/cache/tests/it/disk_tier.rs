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

use bytes::Bytes;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::DiskCacheAccessor;
use databend_storages_common_cache::DiskCacheBuilder;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::cache_manager::CacheManager;

use crate::fakes::shared_store;

/// Bloom filter stand-in with a disk encoding; an empty payload fails to decode.
struct Blob(Vec<u8>);

impl CachedItem for Blob {
    fn mem_bytes(&self) -> usize {
        self.0.len()
    }
}

impl TryFrom<&Blob> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &Blob) -> Result<Self, ErrorCode> {
        Ok(value.0.clone())
    }
}

impl TryFrom<Bytes> for Blob {
    type Error = ErrorCode;

    fn try_from(bytes: Bytes) -> Result<Self, ErrorCode> {
        if bytes.is_empty() {
            return Err(ErrorCode::Internal("empty"));
        }
        Ok(Blob(bytes.to_vec()))
    }
}

fn wait_for(cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "disk cache population timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
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
    let cache =
        MemCache::<Blob>::new(store.clone(), CacheKind::BloomFilter).with_disk(disk.clone());

    cache.put("k", Blob(vec![1; 10]));
    wait_for(|| disk.contains_key("k"));

    store.evict(CacheKind::BloomFilter, "k");
    let hit = cache.lookup("k").unwrap();
    assert_eq!(hit.0, vec![1; 10]);
    assert!(hit.is_charged());
}

#[test]
fn test_disk_decode_failure_evicts_disk_entry() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let cache = MemCache::<Blob>::new(shared_store(0, 1 << 20, 0), CacheKind::BloomFilter)
        .with_disk(disk.clone());

    disk.insert("bad".to_string(), Bytes::new());
    wait_for(|| disk.contains_key("bad"));
    assert!(cache.lookup("bad").is_none());
    assert!(!disk.contains_key("bad"));
}

#[test]
fn test_disabled_kind_skips_disk() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let store = shared_store(0, 1 << 20, 0);
    let cache =
        MemCache::<Blob>::new(store.clone(), CacheKind::BloomFilter).with_disk(disk.clone());
    cache.put("k", Blob(vec![1; 10]));
    wait_for(|| disk.contains_key("k"));

    CacheManager::new(store, Default::default())
        .set_capacity_by_name("bloom_filter", 0)
        .unwrap();
    assert!(cache.lookup("k").is_none());
    assert!(!cache.contains("k"));
    cache.put("other", Blob(vec![1; 10]));
    assert!(!disk.contains_key("other"));
}

#[test]
fn test_manager_attaches_disk_and_reports_it() {
    let dir = tempfile::tempdir().unwrap();
    let disk = disk(&dir);
    let manager = CacheManager::new(
        shared_store(0, 1 << 20, 0),
        [(CacheKind::BloomFilter, disk.clone())].into(),
    );
    let cache = manager.hybrid_cache::<Blob>(CacheKind::BloomFilter);
    cache.put("k", Blob(vec![1; 10]));
    wait_for(|| disk.contains_key("k"));

    let rows = manager.rows();
    let disk_row = rows.iter().find(|r| r.name == "disk_cache_test").unwrap();
    assert_eq!(
        (disk_row.kind, disk_row.num_items),
        (Some(CacheKind::BloomFilter), 1)
    );
    assert!(disk_row.capacity.unwrap() > 0);
}
