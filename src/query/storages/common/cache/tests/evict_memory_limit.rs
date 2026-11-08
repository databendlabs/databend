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

//! Dropping the cached copies of deleted objects is not held to the memory limit of the query
//! that deleted them. Own binary for the tracking allocator and the allocation error hook.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::set_alloc_error_hook;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::kinds::column;
use opendal::Operator;
use opendal::services::Memory;

#[global_allocator]
static ALLOCATOR: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

const PATH: &str = "1/2/_b/block.parquet";
const LIMIT: i64 = 256 << 20;

#[tokio::test(flavor = "current_thread")]
async fn test_evict_objects_at_the_memory_limit_of_the_query() {
    set_alloc_error_hook();
    ThreadTracker::init();
    let dir = tempfile::TempDir::new().unwrap();
    let disk = Arc::new(DiskCache::new(dir.path(), 1 << 30, [1 << 20; 3], false).unwrap());
    let op = Operator::new(Memory::default()).unwrap().finish();
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let kind = CacheKind::of::<column::Data>();
    let key = DiskKey::whole(PATH);
    assert!(disk.put(kind, &key, Bytes::from_static(b"deleted")).await);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !disk.contains(kind, &key) {
        assert!(Instant::now() < deadline, "the entry was not written");
        std::thread::sleep(Duration::from_millis(5));
    }

    // Enough paths for their keys to take one allocation past the tracker's 4 MiB buffer, which
    // is then charged to the query; the cached one last.
    let mut paths: Vec<String> = (0..200_000).map(|i| format!("{i:0>32}")).collect();
    paths.push(PATH.to_string());

    // A query already at its limit: any allocation charged to it fails.
    let query = MemStat::create("deleting-query".to_string());
    query.set_limit(LIMIT, false);
    {
        let _unlimited = LimitMemGuard::enter_unlimited();
        query.record_memory::<false>(LIMIT, 0).unwrap();
    }
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(query.clone());
    let evicted = {
        let _tracking = ThreadTracker::tracking(payload);
        ThreadTracker::tracking_future(storage.evict_objects(&paths))
    };
    evicted.await.unwrap();
    assert!(!disk.contains(kind, &key));

    let _unlimited = LimitMemGuard::enter_unlimited();
    query.rollback(LIMIT);
}
