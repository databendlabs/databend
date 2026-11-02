// Copyright 2022 Datafuse Labs.
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
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::spawn;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::split_caps;
use tempfile::TempDir;

use super::files_under;
use crate::fakes::wait_for;

const MAX_BYTES: u64 = 10_000;
/// Meta, index and data pools.
const CAPS: [u64; 3] = [1_000, 2_000, 4_000];

fn data() -> CacheKind {
    CacheKind::of::<column::Data>()
}

fn cache(dir: &TempDir) -> DiskCache {
    DiskCache::new(dir.path(), MAX_BYTES, CAPS, false).unwrap()
}

fn key(i: usize) -> DiskKey {
    DiskKey::range(format!("1/2/_b/block_{i}.parquet"), 0..900)
}

fn value(i: usize) -> Bytes {
    Bytes::from(vec![i as u8; 900])
}

async fn put_and_wait(cache: &DiskCache, kind: CacheKind, key: &DiskKey, value: Bytes) {
    assert!(cache.put(kind, key, value, &cache.begin_fill()).await);
    wait_for(|| cache.contains(kind, key));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_then_get() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    assert_eq!(cache.get(data(), &key(0)).await, None);

    put_and_wait(&cache, data(), &key(0), value(0)).await;
    assert_eq!(cache.get(data(), &key(0)).await, Some(value(0)));
    // A whole object and a range of it are different entries.
    assert_eq!(
        cache.get(data(), &DiskKey::whole(key(0).path())).await,
        None
    );

    let stats = cache.pool_stats();
    let (_, data_pool) = stats.iter().find(|(p, _)| *p == Pool::Data).unwrap();
    assert_eq!(data_pool.items, 1);
    assert_eq!(data_pool.bytes, 904, "the value and its crc");
    assert_eq!(cache.kind_usage(data()), (1, 904));
    assert_eq!(cache.metric_name(data()), "disk_cache_column_data");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kinds_go_to_their_pools() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    let meta = CacheKind::of::<segment::Info>();
    let index = CacheKind::of::<bloom::Filter>();
    put_and_wait(&cache, meta, &key(0), Bytes::from(vec![1; 200])).await;
    put_and_wait(&cache, index, &key(0), Bytes::from(vec![2; 200])).await;
    put_and_wait(&cache, data(), &key(0), value(3)).await;

    for (pool, kind) in [("meta", meta), ("index", index), ("data", data())] {
        let files = files_under(&dir.path().join("v2").join(pool));
        assert_eq!(files.len(), 1, "{pool}: {files:?}");
        assert_eq!(cache.kind_usage(kind).0, 1);
    }
    assert_eq!(
        cache.get(meta, &key(0)).await,
        Some(Bytes::from(vec![1; 200]))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lru_eviction_within_the_pool() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    // 4 entries of 904 bytes fit in the 4000 byte data pool.
    for i in 0..4 {
        put_and_wait(&cache, data(), &key(i), value(i)).await;
    }
    // Touch the oldest, so the next one is the least recently used.
    assert!(cache.get(data(), &key(0)).await.is_some());
    put_and_wait(&cache, data(), &key(4), value(4)).await;

    assert!(!cache.contains(data(), &key(1)));
    for i in [0, 2, 3, 4] {
        assert!(cache.contains(data(), &key(i)), "entry {i}");
    }
    wait_for(|| files_under(&dir.path().join("v2/data")).len() == 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_eviction_keeps_per_kind_usage() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    // Two kinds of the 2000 byte index pool, entries of 454 bytes with their crc.
    let (filters, lookups) = (
        CacheKind::of::<bloom::Filter>(),
        CacheKind::of::<inverted::Lookup>(),
    );
    let small = |i: usize| Bytes::from(vec![i as u8; 450]);
    put_and_wait(&cache, filters, &key(0), small(0)).await;
    for i in 1..4 {
        put_and_wait(&cache, lookups, &key(i), small(i)).await;
    }
    assert_eq!(cache.kind_usage(filters), (1, 454));
    assert_eq!(cache.kind_usage(lookups), (3, 3 * 454));
    // The fifth entry evicts the oldest, the filter.
    put_and_wait(&cache, lookups, &key(4), small(4)).await;
    assert_eq!(cache.kind_usage(filters), (0, 0));
    assert_eq!(cache.kind_usage(lookups), (4, 4 * 454));
}

/// A second put of a key queued or being written is not queued again.
#[tokio::test(flavor = "multi_thread")]
async fn test_put_of_a_pending_key_is_dropped() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    let key = key(0);
    let ticket = cache.begin_fill();
    let puts =
        futures::future::join_all((0..8).map(|_| cache.put(data(), &key, value(0), &ticket)));
    let queued = puts.await.into_iter().filter(|q| *q).count();
    assert_eq!(queued, 1);
    wait_for(|| cache.contains(data(), &key));
    assert_eq!(cache.kind_usage(data()), (1, 904));
    // Once on disk, a put of the same key is dropped as well.
    assert!(!cache.put(data(), &key, value(0), &cache.begin_fill()).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_entry_too_large_for_the_pool_is_rejected() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    // More than a quarter of the 4000 byte pool.
    let big = Bytes::from(vec![0; 1_100]);
    assert!(
        cache.put(data(), &key(0), big, &cache.begin_fill()).await,
        "queued"
    );
    wait_for(|| cache.stats().dropped_rejected == 1);
    assert!(!cache.contains(data(), &key(0)));
    assert_eq!(cache.stats().queued_bytes, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reload_after_restart() {
    let dir = TempDir::new().unwrap();
    {
        let cache = cache(&dir);
        put_and_wait(&cache, data(), &key(0), value(0)).await;
        put_and_wait(&cache, data(), &key(1), value(1)).await;
    }
    // A tmp file left by a crashed writer is removed.
    let tmp = dir.path().join("v2/data/leftover_1_2_tmp");
    std::fs::write(&tmp, b"partial").unwrap();

    let cache = cache(&dir);
    assert!(!cache.contains(data(), &key(0)), "the index starts empty");
    cache.reload_now();
    assert_eq!(cache.get(data(), &key(0)).await, Some(value(0)));
    assert_eq!(cache.get(data(), &key(1)).await, Some(value(1)));
    assert_eq!(cache.kind_usage(data()), (2, 2 * 904));
    assert!(!tmp.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reload_trims_to_a_smaller_capacity() {
    let dir = TempDir::new().unwrap();
    // 8 entries of 404 bytes with their crc.
    let small = |i: usize| Bytes::from(vec![i as u8; 400]);
    {
        let cache = cache(&dir);
        for i in 0..8 {
            put_and_wait(&cache, data(), &key(i), small(i)).await;
        }
    }
    // A 2000 byte data pool holds 4 of them.
    let cache = DiskCache::new(dir.path(), MAX_BYTES, [1_000, 2_000, 2_000], false).unwrap();
    cache.reload_now();
    assert_eq!(cache.kind_usage(data()), (4, 4 * 404));
    assert_eq!(files_under(&dir.path().join("v2/data")).len(), 4);
    let mut hits = 0;
    for i in 0..8 {
        if let Some(bytes) = cache.get(data(), &key(i)).await {
            assert_eq!(bytes, small(i));
            hits += 1;
        }
    }
    assert_eq!(hits, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_entry_is_a_miss_and_dropped() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    put_and_wait(&cache, data(), &key(0), value(0)).await;
    let files = files_under(&dir.path().join("v2/data"));
    assert_eq!(files.len(), 1);
    let mut bytes = std::fs::read(&files[0]).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&files[0], bytes).unwrap();

    assert_eq!(cache.get(data(), &key(0)).await, None);
    assert!(!cache.contains(data(), &key(0)));
    wait_for(|| !files[0].exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_old_layouts_are_removed() {
    let dir = TempDir::new().unwrap();
    let old = dir.path().join("disk_cache_table_data");
    std::fs::create_dir_all(old.join("abc")).unwrap();
    std::fs::write(old.join("abc/entry"), b"old").unwrap();

    let cache = Arc::new(cache(&dir));
    cache.clone().start_reload(dir.path().to_path_buf());
    wait_for(|| !old.exists());
    assert!(dir.path().join("v2/data").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pool_capacity_at_runtime() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    put_and_wait(&cache, data(), &key(0), value(0)).await;

    // 1000 + 2000 + 8000 > 10000.
    assert!(cache.set_pool_cap(Pool::Data, 8_000).is_err());

    cache.set_pool_cap(Pool::Data, 0).unwrap();
    assert!(!cache.is_enabled(data()));
    assert_eq!(cache.get(data(), &key(0)).await, None);
    assert!(
        !cache
            .put(data(), &key(1), value(1), &cache.begin_fill())
            .await
    );
    wait_for(|| files_under(&dir.path().join("v2/data")).is_empty());

    cache
        .set_pool_cap(Pool::Data, cache.default_pool_cap(Pool::Data))
        .unwrap();
    assert!(cache.is_enabled(data()));
    put_and_wait(&cache, data(), &key(1), value(1)).await;
}

/// Shrinking a pool below what it holds evicts from the least recently used end.
#[tokio::test(flavor = "multi_thread")]
async fn test_shrinking_a_pool_evicts_lru_first() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    for i in 0..4 {
        put_and_wait(&cache, data(), &key(i), value(i)).await;
    }
    assert!(cache.get(data(), &key(0)).await.is_some());
    cache.set_pool_cap(Pool::Data, 2 * 904).unwrap();
    assert!(cache.contains(data(), &key(0)) && cache.contains(data(), &key(3)));
    assert!(!cache.contains(data(), &key(1)) && !cache.contains(data(), &key(2)));
    assert_eq!(cache.kind_usage(data()), (2, 2 * 904));
    wait_for(|| files_under(&dir.path().join("v2/data")).len() == 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kind_switch_drops_only_that_kind() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    // Two kinds of the index pool.
    let (filters, lookups) = (
        CacheKind::of::<bloom::Filter>(),
        CacheKind::of::<inverted::Lookup>(),
    );
    let small = Bytes::from(vec![7; 100]);
    put_and_wait(&cache, filters, &key(0), small.clone()).await;
    put_and_wait(&cache, lookups, &key(0), small.clone()).await;

    cache.set_kind_enabled(filters, false);
    assert!(!cache.is_enabled(filters) && !cache.is_kind_enabled(filters));
    assert!(
        !cache
            .put(filters, &key(1), small.clone(), &cache.begin_fill())
            .await
    );
    assert_eq!(cache.get(filters, &key(0)).await, None);
    assert_eq!(cache.kind_usage(filters), (0, 0));
    assert!(cache.contains(lookups, &key(0)));
    wait_for(|| files_under(&dir.path().join("v2/index")).len() == 1);

    cache.set_kind_enabled(filters, true);
    put_and_wait(&cache, filters, &key(1), small).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_license_switch() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    put_and_wait(&cache, data(), &key(0), value(0)).await;
    cache.set_allowed(false);
    assert_eq!(cache.get(data(), &key(0)).await, None);
    assert!(
        !cache
            .put(data(), &key(1), value(1), &cache.begin_fill())
            .await
    );
    cache.set_allowed(true);
    assert_eq!(cache.get(data(), &key(0)).await, Some(value(0)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_evict() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    put_and_wait(&cache, data(), &key(0), value(0)).await;
    assert!(cache.evict(data(), &key(0)));
    assert!(!cache.evict(data(), &key(0)));
    assert_eq!(cache.get(data(), &key(0)).await, None);
    assert!(files_under(&dir.path().join("v2/data")).is_empty());
}

#[test]
fn test_split_caps_follows_the_ratios_whatever_their_sum() {
    assert_eq!(split_caps(1000, [10, 20, 70]).unwrap(), [100, 200, 700]);
    assert_eq!(split_caps(1000, [1, 2, 7]).unwrap(), [100, 200, 700]);
    assert_eq!(split_caps(1000, [20, 40, 140]).unwrap(), [100, 200, 700]);
    assert_eq!(split_caps(1000, [0, 1, 1]).unwrap(), [0, 500, 500]);
    assert!(split_caps(1000, [0, 0, 0]).is_err());
}

/// An entry whose file is gone (deleted behind the cache) is a miss and leaves the index.
#[tokio::test(flavor = "multi_thread")]
async fn test_missing_file_is_a_miss_and_dropped() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    put_and_wait(&cache, data(), &key(0), value(0)).await;
    for file in files_under(&dir.path().join("v2/data")) {
        std::fs::remove_file(file).unwrap();
    }
    assert_eq!(cache.get(data(), &key(0)).await, None);
    assert!(!cache.contains(data(), &key(0)));
    assert_eq!(cache.kind_usage(data()), (0, 0));
    // And can be cached again.
    put_and_wait(&cache, data(), &key(0), value(0)).await;
    assert_eq!(cache.get(data(), &key(0)).await, Some(value(0)));
}

fn stress_key(i: usize) -> DiskKey {
    DiskKey::range(
        format!("1/2/_b/stress_{i}.parquet"),
        0..(i as u64 % 7 + 1) * 100,
    )
}

/// The value of a key is a function of it, so any hit can be checked.
fn stress_value(i: usize) -> Bytes {
    Bytes::from(vec![i as u8; (i % 7 + 1) * 100])
}

/// Readers putting, getting, evicting and checking keys while the pool is resized and the
/// kind switched off and on: every hit has the bytes of its key, and once quiet the index,
/// the per-kind counts and the files agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_use_keeps_entries_and_files_consistent() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(cache(&dir));
    let readers: Vec<_> = (0..16usize)
        .map(|t| {
            let cache = cache.clone();
            spawn(async move {
                for n in 0..300usize {
                    let i = (t * 13 + n * 7) % 48;
                    match n % 5 {
                        0 | 1 => {
                            cache
                                .put(data(), &stress_key(i), stress_value(i), &cache.begin_fill())
                                .await;
                        }
                        2 | 3 => {
                            if let Some(bytes) = cache.get(data(), &stress_key(i)).await {
                                assert_eq!(bytes, stress_value(i), "key {i}");
                            }
                        }
                        _ => {
                            cache.evict(data(), &stress_key(i));
                        }
                    }
                }
            })
        })
        .collect();
    let control = {
        let cache = cache.clone();
        spawn(async move {
            for n in 0..30u64 {
                cache.set_pool_cap(Pool::Data, 3_000 + n * 100).unwrap();
                if n % 10 == 0 {
                    cache.set_kind_enabled(data(), false);
                    cache.set_kind_enabled(data(), true);
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
    };
    for r in readers {
        r.await.unwrap();
    }
    control.await.unwrap();

    wait_for(|| cache.stats().queued_bytes == 0);
    assert_quiet_and_consistent(&cache, &dir);
    // Entries may be rejected while an earlier file of their key is being deleted.
    assert_eq!(cache.stats().dropped_io_error, 0);
}

/// The restart scan running while entries are written: nothing is counted twice, and the
/// index matches the files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reload_while_writing() {
    let dir = TempDir::new().unwrap();
    {
        let cache = cache(&dir);
        for i in 0..24 {
            cache
                .put(data(), &stress_key(i), stress_value(i), &cache.begin_fill())
                .await;
        }
        wait_for(|| cache.stats().queued_bytes == 0);
    }
    let cache = Arc::new(cache(&dir));
    let reload = {
        let cache = cache.clone();
        Thread::spawn(move || cache.reload_now())
    };
    for i in 12..48 {
        cache
            .put(data(), &stress_key(i), stress_value(i), &cache.begin_fill())
            .await;
    }
    reload.join().unwrap();
    wait_for(|| cache.stats().queued_bytes == 0);
    assert_quiet_and_consistent(&cache, &dir);
    for i in 0..48 {
        if let Some(bytes) = cache.get(data(), &stress_key(i)).await {
            assert_eq!(bytes, stress_value(i), "key {i}");
        }
    }
}

/// Once no write is queued and background deletes are done: the pool's items and bytes match
/// the files under its directory and the kind's counts, within its capacity.
fn assert_quiet_and_consistent(cache: &DiskCache, dir: &TempDir) {
    let pool_dir = dir.path().join("v2/data");
    let stats = || {
        cache
            .pool_stats()
            .into_iter()
            .find(|(p, _)| *p == Pool::Data)
            .unwrap()
            .1
    };
    // Deletes of evicted files may still run in the background.
    wait_for(|| files_under(&pool_dir).len() as u64 == stats().items);
    let files = files_under(&pool_dir);
    let on_disk: u64 = files
        .iter()
        .map(|f| std::fs::metadata(f).unwrap().len())
        .sum();
    let stats = stats();
    assert_eq!(on_disk, stats.bytes);
    assert!(stats.bytes <= stats.cap);
    assert_eq!(cache.kind_usage(data()), (stats.items, stats.bytes));
}

/// `evict_object` drops every unit of an object, whole or ranges, in every kind and pool, and
/// leaves other objects alone.
#[tokio::test(flavor = "multi_thread")]
async fn test_evict_object_drops_all_its_units() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    let path = "1/2/_i_b_v2/index.parquet";
    let filters = CacheKind::of::<bloom::Filter>();
    let metas = CacheKind::of::<bloom::IndexMeta>();
    let units = [
        (filters, DiskKey::whole(path)),
        (filters, DiskKey::range(path, 0..100)),
        (metas, DiskKey::range(path, 100..200)),
    ];
    for (kind, key) in &units {
        put_and_wait(&cache, *kind, key, Bytes::from(vec![1; 100])).await;
    }
    let other = DiskKey::whole("1/2/_i_b_v2/other.parquet");
    put_and_wait(&cache, filters, &other, Bytes::from(vec![2; 100])).await;

    assert_eq!(cache.evict_object(path), 3);
    for (kind, key) in &units {
        assert!(!cache.contains(*kind, key), "{key}");
        assert_eq!(cache.get(*kind, key).await, None);
    }
    assert!(cache.contains(filters, &other));
    assert_eq!(files_under(&dir.path().join("v2")).len(), 1);
    assert_eq!(cache.evict_object(path), 0);
    // The object can be cached again, as rewritten.
    put_and_wait(&cache, filters, &units[0].1, Bytes::from(vec![3; 100])).await;
    assert_eq!(
        cache.get(filters, &units[0].1).await,
        Some(Bytes::from(vec![3; 100]))
    );
}

/// A unit whose fill started before `evict_object`, still queued or being written then, is not
/// published after it: the bytes were read before the rewrite. A fill started after is.
#[tokio::test(flavor = "multi_thread")]
async fn test_evict_object_rejects_fills_started_before() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    let path = "1/2/_sg/segment.mpk";
    let kind = CacheKind::of::<segment::Info>();
    let key = DiskKey::whole(path);
    let read_before = cache.begin_fill();
    // The object is rewritten and evicted before the old bytes are queued.
    assert_eq!(cache.evict_object(path), 0);
    assert!(
        cache
            .put(kind, &key, Bytes::from(vec![1; 100]), &read_before)
            .await
    );
    drop(read_before);
    wait_for(|| cache.stats().queued_bytes == 0);
    wait_for(|| cache.stats().dropped_rejected == 1);
    assert!(!cache.contains(kind, &key));
    assert!(files_under(&dir.path().join("v2")).is_empty());

    put_and_wait(&cache, kind, &key, Bytes::from(vec![2; 100])).await;
    assert_eq!(cache.get(kind, &key).await, Some(Bytes::from(vec![2; 100])));
}

/// Pools resized at once from many threads: each resize checks the pools' sum and updates its
/// pool together, so the pools never hold more than `max_bytes`, whichever resizes win.
#[test]
fn test_concurrent_pool_resizes_stay_within_max_bytes() {
    let dir = TempDir::new().unwrap();
    let cache =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 30_000], false).unwrap());
    let threads: Vec<_> = Pool::ALL
        .into_iter()
        .map(|pool| {
            let cache = cache.clone();
            Thread::spawn(move || {
                for i in 0..2_000u64 {
                    let _ = cache.set_pool_cap(pool, 30_000 + (i * 7919) % 30_000);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let total: u64 = cache.pool_stats().iter().map(|(_, s)| s.cap).sum();
    assert!(total <= 100_000, "{total}");
}

/// A kind turned on and off from two threads at once ends consistent: when it reads as on, its
/// entries are written; when off, they are not.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_kind_toggles_stay_consistent() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(cache(&dir));
    let threads: Vec<_> = [true, false]
        .into_iter()
        .map(|first| {
            let cache = cache.clone();
            Thread::spawn(move || {
                for i in 0..2_000 {
                    cache.set_kind_enabled(data(), (i % 2 == 0) == first);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let key = key(0);
    let queued = cache.put(data(), &key, value(0), &cache.begin_fill()).await;
    wait_for(|| cache.stats().queued_bytes == 0);
    assert_eq!(cache.is_kind_enabled(data()), queued);
    assert_eq!(cache.is_kind_enabled(data()), cache.contains(data(), &key));
}

/// Invalidations kept while an old fill runs are dropped when it finishes, without waiting
/// for another invalidation.
#[test]
fn test_invalidations_are_dropped_when_the_oldest_fill_finishes() {
    let dir = TempDir::new().unwrap();
    let cache = cache(&dir);
    cache.reload_now();
    let kind = CacheKind::of::<segment::Info>();
    let paths: Vec<_> = (0..1_000).map(|i| format!("1/2/_sg/{i}.mpk")).collect();
    let old = cache.begin_fill();
    let newer = cache.begin_fill();
    for path in &paths {
        assert_eq!(cache.evict_object(path), 0);
    }
    assert!(paths.iter().all(|p| cache.invalidation_of(kind, p) > 0));
    // A fill that is not the oldest finishing changes nothing.
    drop(newer);
    assert!(paths.iter().all(|p| cache.invalidation_of(kind, p) > 0));
    drop(old);
    assert!(paths.iter().all(|p| cache.invalidation_of(kind, p) == 0));
}
