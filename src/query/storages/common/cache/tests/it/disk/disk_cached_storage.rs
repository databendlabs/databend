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
use databend_storages_common_cache::CacheFill;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;
use opendal::services::Memory;
use tempfile::TempDir;

use crate::fakes::wait_for;

const PATH: &str = "1/2/_b/block.parquet";

fn data() -> CacheKind {
    CacheKind::of::<column::Data>()
}

fn object() -> Bytes {
    Bytes::from((0..=255u8).cycle().take(3_000).collect::<Vec<_>>())
}

/// A memory operator holding `object()` at `PATH`, behind a disk cache.
async fn setup(dir: &TempDir) -> (Operator, Arc<DiskCache>, DiskCachedStorage) {
    let op = Operator::new(Memory::default()).unwrap().finish();
    op.write(PATH, object()).await.unwrap();
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op.clone(), Some(disk.clone()));
    (op, disk, storage)
}

/// A read that must not fill the cache queues nothing: `put` is what queues, and it has
/// returned by the time `read` returns.
fn assert_not_filled(disk: &DiskCache, key: &DiskKey) {
    assert_eq!(disk.stats().queued_bytes, 0);
    assert!(!disk.contains(data(), key));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_fills_on_miss_and_hits_after() {
    let dir = TempDir::new().unwrap();
    let (op, disk, storage) = setup(&dir).await;

    let range = storage
        .read(data(), PATH, Some(10..20), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(range, object().slice(10..20));
    let whole = storage
        .read(data(), PATH, None, CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(whole, object());
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 10..20)));
    wait_for(|| disk.contains(data(), &DiskKey::whole(PATH)));

    // Served from disk once the object is gone.
    op.delete(PATH).await.unwrap();
    for fill in [CacheFill::ReadWrite, CacheFill::ReadOnly] {
        let hit = storage
            .read(data(), PATH, Some(10..20), fill)
            .await
            .unwrap();
        assert_eq!(hit, object().slice(10..20));
    }
    assert!(
        storage
            .read(data(), PATH, Some(10..20), CacheFill::Bypass)
            .await
            .is_err(),
        "bypass reads storage"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_and_bypass_do_not_fill() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    for (fill, range) in [(CacheFill::ReadOnly, 0..5), (CacheFill::Bypass, 5..10)] {
        let bytes = storage
            .read(data(), PATH, Some(range.clone()), fill)
            .await
            .unwrap();
        assert_eq!(
            bytes,
            object().slice(range.start as usize..range.end as usize)
        );
        assert_not_filled(&disk, &DiskKey::range(PATH, range));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_key_prefix_separates_objects() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let prefixed = storage.clone().with_key_prefix("s3://bucket/");
    prefixed
        .read(data(), PATH, None, CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &DiskKey::whole(format!("s3://bucket/{PATH}"))));
    assert!(!disk.contains(data(), &DiskKey::whole(PATH)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_units_mixes_disk_and_storage() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let units = [(0u32, 0..100u64), (1, 100..300), (2, 1_000..1_500)];
    let settings = ReadSettings {
        max_gap_size: 1_000,
        max_range_size: 10_000,
        parquet_fast_read_bytes: 0,
    };

    // Unit 1 is on disk first.
    storage
        .read(data(), PATH, Some(100..300), CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 100..300)));

    let read = storage
        .read_units(data(), PATH, &units, CacheFill::ReadWrite, &settings)
        .await
        .unwrap();
    assert_eq!(read.cached.keys().collect::<Vec<_>>(), vec![&1]);
    assert!(!read.fetched.columns_chunk_offsets.contains_key(&1));
    for (id, range) in &units {
        let expected = object().slice(range.start as usize..range.end as usize);
        assert_eq!(read.unit(*id), Some(expected), "unit {id}");
    }
    // Each unit is its own entry, whatever the merged reads were.
    for (_, range) in &units {
        wait_for(|| disk.contains(data(), &DiskKey::range(PATH, range.clone())));
    }

    // All on disk now.
    let read = storage
        .read_units(data(), PATH, &units, CacheFill::ReadOnly, &settings)
        .await
        .unwrap();
    assert_eq!(read.cached.len(), 3);
    assert!(read.fetched.columns_chunk_offsets.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_units_without_disk() {
    let dir = TempDir::new().unwrap();
    let (op, disk, _) = setup(&dir).await;
    let units = [(0u32, 0..100u64), (1, 2_000..2_100)];
    let settings = ReadSettings {
        max_gap_size: 0,
        max_range_size: 10_000,
        parquet_fast_read_bytes: 0,
    };
    for storage in [
        DiskCachedStorage::with_disk(op.clone(), None),
        DiskCachedStorage::with_disk(op.clone(), Some(disk.clone())),
    ] {
        let read = storage
            .read_units(data(), PATH, &units, CacheFill::Bypass, &settings)
            .await
            .unwrap();
        assert!(read.cached.is_empty());
        assert_eq!(read.unit(1), Some(object().slice(2_000..2_100)));
    }
    assert_not_filled(&disk, &DiskKey::range(PATH, 0..100));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_disabled_kind_reads_storage() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    disk.set_kind_enabled(data(), false);
    assert!(!storage.uses_disk(data(), CacheFill::ReadWrite));
    let bytes = storage
        .read(data(), PATH, Some(0..8), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(bytes, object().slice(0..8));
    assert_not_filled(&disk, &DiskKey::range(PATH, 0..8));
}

/// Concurrent misses of one unit read storage once and share the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_misses_read_storage_once() {
    let dir = TempDir::new().unwrap();
    let op = Operator::new(Memory::default()).unwrap().finish();
    op.write(PATH, object()).await.unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = op.layer(CountingLayer(reads.clone()));
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));

    let results = futures::future::join_all((0..16).map(|_| {
        let storage = storage.clone();
        async move {
            storage
                .read(data(), PATH, Some(0..100), CacheFill::ReadWrite)
                .await
                .unwrap()
        }
    }))
    .await;
    assert!(results.iter().all(|b| *b == object().slice(0..100)));
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..100)));
}

/// Concurrent misses of the same units of an object, as scans of the same columns at once make
/// them, read storage once with merge IO and share the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_unit_misses_read_storage_once() {
    let dir = TempDir::new().unwrap();
    let op = Operator::new(Memory::default()).unwrap().finish();
    op.write(PATH, object()).await.unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = op.layer(CountingLayer(reads.clone()));
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let units = [(0u32, 0..100u64), (1, 200..300)];
    let settings = ReadSettings {
        max_gap_size: 1_000,
        max_range_size: 10_000,
        parquet_fast_read_bytes: 0,
    };

    let results = futures::future::join_all((0..16).map(|_| {
        let storage = storage.clone();
        let units = units.clone();
        async move {
            let read = storage
                .read_units(data(), PATH, &units, CacheFill::ReadWrite, &settings)
                .await
                .unwrap();
            (read.unit(0).unwrap(), read.unit(1).unwrap())
        }
    }))
    .await;
    assert!(
        results
            .iter()
            .all(|r| *r == (object().slice(0..100), object().slice(200..300)))
    );
    // One merged read of both units.
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 200..300)));
}

/// A failed storage read is returned to every caller sharing it and fills nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_storage_error_is_shared_and_not_cached() {
    let dir = TempDir::new().unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = Operator::new(Memory::default())
        .unwrap()
        .finish()
        .layer(CountingLayer(reads.clone()));
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let results = futures::future::join_all((0..8).map(|_| {
        let storage = storage.clone();
        async move {
            storage
                .read(data(), "missing", Some(0..10), CacheFill::ReadWrite)
                .await
        }
    }))
    .await;
    assert!(results.iter().all(|r| r.is_err()));
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_not_filled(&disk, &DiskKey::range("missing", 0..10));
    // The next miss reads storage again.
    assert!(
        storage
            .read(data(), "missing", Some(0..10), CacheFill::ReadWrite)
            .await
            .is_err()
    );
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 2);
}

/// A unit whose file is damaged is read from storage and written again.
#[tokio::test(flavor = "multi_thread")]
async fn test_damaged_entry_falls_back_to_storage_and_refills() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let key = DiskKey::range(PATH, 0..50);
    storage
        .read(data(), PATH, Some(0..50), CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &key));
    for file in super::files_under(&dir.path().join("v2/data")) {
        std::fs::write(file, b"garbage").unwrap();
    }
    let bytes = storage
        .read(data(), PATH, Some(0..50), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(bytes, object().slice(0..50));
    wait_for(|| disk.contains(data(), &key));
    assert_eq!(disk.get(data(), &key).await, Some(object().slice(0..50)));
}

/// `read_ranges` reads each range as its own unit.
#[tokio::test(flavor = "multi_thread")]
async fn test_read_ranges_are_units() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let ranges = vec![0..10, 100..300, 2_000..3_000];
    let read = storage
        .read_ranges(data(), PATH, ranges.clone(), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(read.len(), 3);
    for range in ranges {
        let expected = object().slice(range.start as usize..range.end as usize);
        assert_eq!(read[&range], expected);
        wait_for(|| disk.contains(data(), &DiskKey::range(PATH, range.clone())));
    }
}

/// Counts reads reaching the operator, each taking 50 ms after it has the bytes.
#[derive(Clone)]
struct CountingLayer(Arc<std::sync::atomic::AtomicUsize>);

impl<A: opendal::raw::Access> opendal::raw::Layer<A> for CountingLayer {
    type LayeredAccess = Counting<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        Counting {
            inner,
            reads: self.0.clone(),
        }
    }
}

#[derive(Debug)]
struct Counting<A> {
    inner: A,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl<A: opendal::raw::Access> opendal::raw::LayeredAccess for Counting<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(
        &self,
        path: &str,
        args: opendal::raw::OpRead,
    ) -> opendal::Result<(opendal::raw::RpRead, Self::Reader)> {
        let read = self.inner.read(path, args).await;
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Slow enough that concurrent misses find the first read in flight, and that the
        // object can be rewritten before the bytes read reach the cache.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        read
    }

    async fn write(
        &self,
        path: &str,
        args: opendal::raw::OpWrite,
    ) -> opendal::Result<(opendal::raw::RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(
        &self,
        path: &str,
        args: opendal::raw::OpList,
    ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }
}

/// An object rewritten in place is read as rewritten once it is evicted; until then the disk
/// cache keeps serving the old bytes, which is what writers in place must prevent.
#[tokio::test(flavor = "multi_thread")]
async fn test_eviction_after_rewrite_reads_the_new_bytes() {
    let dir = TempDir::new().unwrap();
    let (op, disk, storage) = setup(&dir).await;
    let prefixed = storage.clone().with_key_prefix("root/");
    for storage in [&storage, &prefixed] {
        storage
            .read(data(), PATH, None, CacheFill::ReadWrite)
            .await
            .unwrap();
        storage
            .read(data(), PATH, Some(0..10), CacheFill::ReadWrite)
            .await
            .unwrap();
    }
    wait_for(|| disk.contains(data(), &DiskKey::range(format!("root/{PATH}"), 0..10)));
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..10)));

    let rewritten = Bytes::from(vec![9u8; 3_000]);
    op.write(PATH, rewritten.clone()).await.unwrap();
    let stale = storage
        .read(data(), PATH, Some(0..10), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(stale, object().slice(0..10));

    storage.evict_objects(&[PATH.to_string()]).await.unwrap();
    for (range, expected) in [
        (None, rewritten.clone()),
        (Some(0..10), rewritten.slice(0..10)),
    ] {
        let bytes = storage
            .read(data(), PATH, range, CacheFill::ReadWrite)
            .await
            .unwrap();
        assert_eq!(bytes, expected);
    }
    // The prefixed object is another object; it is dropped through its own storage.
    assert!(disk.contains(data(), &DiskKey::range(format!("root/{PATH}"), 0..10)));
    prefixed.evict_objects(&[PATH.to_string()]).await.unwrap();
    assert!(!disk.contains(data(), &DiskKey::range(format!("root/{PATH}"), 0..10)));
}

/// A reader cancelled while its storage read is in flight does not take the others down: a
/// waiting reader takes the load over and gets the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_cancelled_reader_hands_the_load_over() {
    let dir = TempDir::new().unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = Operator::new(Memory::default())
        .unwrap()
        .finish()
        .layer(CountingLayer(reads.clone()));
    op.write(PATH, object()).await.unwrap();
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let first = {
        let storage = storage.clone();
        databend_common_base::runtime::spawn(async move {
            storage
                .read(data(), PATH, Some(0..10), CacheFill::ReadWrite)
                .await
        })
    };
    wait_for(|| reads.load(std::sync::atomic::Ordering::Relaxed) == 1);
    let second = {
        let storage = storage.clone();
        databend_common_base::runtime::spawn(async move {
            storage
                .read(data(), PATH, Some(0..10), CacheFill::ReadWrite)
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    first.abort();
    assert_eq!(second.await.unwrap().unwrap(), object().slice(0..10));
    assert!(reads.load(std::sync::atomic::Ordering::Relaxed) <= 2);
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..10)));
}

/// Bypassing reads, as those of maintenance statements (`CachePolicy::MAINTENANCE`), whole
/// ranges and units alike: they fill nothing, and what queries cached is neither read nor
/// refreshed by them. A filling read fills.
#[tokio::test(flavor = "multi_thread")]
async fn test_bypass_reads_neither_look_up_nor_fill_the_disk_cache() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let key = DiskKey::range(PATH, 0..10);
    let bypass = |storage: DiskCachedStorage| async move {
        let range = storage
            .read(data(), PATH, Some(0..10), CacheFill::Bypass)
            .await
            .unwrap();
        let units = storage
            .read_units(
                data(),
                PATH,
                &[(1, 20..30)],
                CacheFill::Bypass,
                &ReadSettings {
                    max_gap_size: 0,
                    max_range_size: 1_000,
                    parquet_fast_read_bytes: 0,
                },
            )
            .await
            .unwrap();
        (range, units.unit(1).unwrap())
    };
    let (range, unit) = bypass(storage.clone()).await;
    assert_eq!(
        (range, unit),
        (object().slice(0..10), object().slice(20..30))
    );
    wait_for(|| disk.stats().queued_bytes == 0);
    assert!(!disk.contains(data(), &key));
    assert!(!disk.contains(data(), &DiskKey::range(PATH, 20..30)));

    storage
        .read(data(), PATH, Some(0..10), CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &key));
    // Cached now, yet not read by the bypassing reads, and still nothing filled.
    let accesses = || {
        disk.pool_stats()
            .iter()
            .map(|(_, s)| s.hit + s.miss)
            .sum::<u64>()
    };
    let before = accesses();
    bypass(storage).await;
    assert_eq!(accesses(), before);
    assert!(!disk.contains(data(), &DiskKey::range(PATH, 20..30)));
}

/// A memory operator holding `object()` at `PATH`, counting and delaying its reads, behind a
/// disk cache: for the shared reads of units.
async fn counted_setup(
    dir: &TempDir,
) -> (
    Operator,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<DiskCache>,
    DiskCachedStorage,
) {
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = Operator::new(Memory::default())
        .unwrap()
        .finish()
        .layer(CountingLayer(reads.clone()));
    op.write(PATH, object()).await.unwrap();
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op.clone(), Some(disk.clone()));
    (op, reads, disk, storage)
}

const UNITS: [(u32, std::ops::Range<u64>); 2] = [(0, 0..100), (1, 200..300)];

fn merged() -> ReadSettings {
    ReadSettings {
        max_gap_size: 1_000,
        max_range_size: 10_000,
        parquet_fast_read_bytes: 0,
    }
}

async fn read_units(
    storage: &DiskCachedStorage,
    path: &str,
    units: &[(u32, std::ops::Range<u64>)],
) -> databend_common_exception::Result<Vec<Bytes>> {
    let read = storage
        .read_units(data(), path, units, CacheFill::ReadWrite, &merged())
        .await?;
    Ok(units
        .iter()
        .map(|(id, _)| read.unit(*id).unwrap())
        .collect())
}

fn spawn_read_units(
    storage: &DiskCachedStorage,
    units: &'static [(u32, std::ops::Range<u64>)],
) -> databend_common_base::runtime::JoinHandle<databend_common_exception::Result<Vec<Bytes>>> {
    let storage = storage.clone();
    databend_common_base::runtime::spawn(async move { read_units(&storage, PATH, units).await })
}

fn reads_of(reads: &std::sync::atomic::AtomicUsize) -> usize {
    reads.load(std::sync::atomic::Ordering::Relaxed)
}

/// Concurrent misses of different units of one object do not share a read: each reader gets
/// its own units.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_misses_of_different_units_read_their_own() {
    let dir = TempDir::new().unwrap();
    let (_op, reads, _disk, storage) = counted_setup(&dir).await;
    static FIRST: [(u32, std::ops::Range<u64>); 1] = [(0, 0..100)];
    let (one, both) = futures::future::join(
        read_units(&storage, PATH, &FIRST),
        read_units(&storage, PATH, &UNITS),
    )
    .await;
    assert_eq!(one.unwrap(), vec![object().slice(0..100)]);
    assert_eq!(both.unwrap(), vec![
        object().slice(0..100),
        object().slice(200..300)
    ]);
    assert_eq!(reads_of(&reads), 2);
}

/// A failed storage read of units is returned to every reader sharing it and fills nothing;
/// the next miss reads storage again.
#[tokio::test(flavor = "multi_thread")]
async fn test_unit_read_error_is_shared_and_not_cached() {
    let dir = TempDir::new().unwrap();
    let (_op, reads, disk, storage) = counted_setup(&dir).await;
    let results =
        futures::future::join_all((0..8).map(|_| read_units(&storage, "missing", &UNITS))).await;
    assert!(results.iter().all(|r| r.is_err()));
    assert_eq!(reads_of(&reads), 1);
    assert_not_filled(&disk, &DiskKey::range("missing", 0..100));
    assert!(read_units(&storage, "missing", &UNITS).await.is_err());
    assert_eq!(reads_of(&reads), 2);
}

/// When the reader running a shared read of units is cancelled, a reader waiting on it takes
/// the read over and gets the units, which are cached.
#[tokio::test(flavor = "multi_thread")]
async fn test_cancelled_unit_reader_hands_the_read_over() {
    let dir = TempDir::new().unwrap();
    let (_op, reads, disk, storage) = counted_setup(&dir).await;
    let first = spawn_read_units(&storage, &UNITS);
    wait_for(|| reads_of(&reads) == 1);
    let second = spawn_read_units(&storage, &UNITS);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    first.abort();
    assert_eq!(second.await.unwrap().unwrap(), vec![
        object().slice(0..100),
        object().slice(200..300)
    ]);
    assert!(reads_of(&reads) <= 2);
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 200..300)));
}

/// Many readers of the units of a few objects at once, as the scans of one query and of
/// concurrent queries make them: each object is read from storage once, whatever the number of
/// readers, and every reader gets its units.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_readers_of_many_objects_read_each_once() {
    let dir = TempDir::new().unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let op = Operator::new(Memory::default())
        .unwrap()
        .finish()
        .layer(CountingLayer(reads.clone()));
    let paths: Vec<String> = (0..8)
        .map(|i| format!("1/2/_b/block_{i}.parquet"))
        .collect();
    for path in &paths {
        op.write(path, object()).await.unwrap();
    }
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let results = futures::future::join_all((0..64).map(|i| {
        let storage = storage.clone();
        let path = paths[i % paths.len()].clone();
        async move { read_units(&storage, &path, &UNITS).await.unwrap() }
    }))
    .await;
    let expected = vec![object().slice(0..100), object().slice(200..300)];
    assert!(results.iter().all(|r| *r == expected));
    assert_eq!(reads_of(&reads), paths.len());
    // All on disk now: the same reads again touch no storage.
    wait_for(|| {
        paths
            .iter()
            .all(|p| disk.contains(data(), &DiskKey::range(p.as_str(), 200..300)))
    });
    for path in &paths {
        assert_eq!(read_units(&storage, path, &UNITS).await.unwrap(), expected);
    }
    assert_eq!(reads_of(&reads), paths.len());
}

/// Readers of overlapping units race with rewrites of the object in place, each followed by an
/// eviction, as queries race with ANALYZE. Every unit read is whole and of one version of the
/// object, never newer than the last one written. Eviction is best effort: a read in flight
/// across a rewrite may cache what it read before, so once the readers are done one more
/// eviction leaves every read with the last version.
#[tokio::test(flavor = "multi_thread")]
async fn test_unit_reads_racing_rewrites_return_whole_units() {
    let dir = TempDir::new().unwrap();
    let op = Operator::new(Memory::default()).unwrap().finish();
    let version = |v: u8| Bytes::from(vec![v; 3_000]);
    op.write(PATH, version(0)).await.unwrap();
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage = DiskCachedStorage::with_disk(op.clone(), Some(disk.clone()));
    let written = Arc::new(std::sync::atomic::AtomicU8::new(0));
    const VERSIONS: u8 = 20;
    let sets: [&[(u32, std::ops::Range<u64>)]; 3] = [
        &[(0, 0..100), (1, 200..300)],
        &[(1, 200..300), (2, 1_000..1_500)],
        &[(0, 0..100), (2, 1_000..1_500), (3, 2_000..3_000)],
    ];
    let readers: Vec<_> = (0..8)
        .map(|r| {
            let (storage, written) = (storage.clone(), written.clone());
            databend_common_base::runtime::spawn(async move {
                for i in 0..200 {
                    let units = sets[(r + i) % sets.len()];
                    let read = read_units(&storage, PATH, units).await.unwrap();
                    for (bytes, (_, range)) in read.iter().zip(units) {
                        assert_eq!(bytes.len() as u64, range.end - range.start);
                        let v = bytes[0];
                        assert!(bytes.iter().all(|b| *b == v), "a torn unit");
                        // Never a version not written yet.
                        assert!(v <= written.load(std::sync::atomic::Ordering::SeqCst));
                    }
                }
            })
        })
        .collect();
    for v in 1..VERSIONS {
        // As a fuse rewrite: written, then evicted. The version is announced before it can be
        // read.
        written.store(v, std::sync::atomic::Ordering::SeqCst);
        op.write(PATH, version(v)).await.unwrap();
        storage.evict_objects(&[PATH.to_string()]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    for reader in readers {
        reader.await.unwrap();
    }
    wait_for(|| disk.stats().queued_bytes == 0);
    storage.evict_objects(&[PATH.to_string()]).await.unwrap();
    let last = VERSIONS - 1;
    for units in sets {
        for _ in 0..2 {
            let read = read_units(&storage, PATH, units).await.unwrap();
            assert!(
                read.iter().all(|b| b.iter().all(|x| *x == last)),
                "stale bytes after the rewrites"
            );
        }
    }
}

/// Bypassing reads (maintenance) and filling reads (queries) of the same objects at once, on
/// the same runtime: each read keeps its own fill, so the bypassing reads never touch the disk
/// cache while the filling reads fill and hit it. Every disk cache lookup is a filling read's.
#[tokio::test(flavor = "multi_thread")]
async fn test_bypass_and_filling_reads_at_once_keep_their_own_fill() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    let query_reads = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tasks: Vec<_> = (0..16)
        .map(|t| {
            let (storage, query_reads) = (storage.clone(), query_reads.clone());
            databend_common_base::runtime::spawn(async move {
                for i in 0..50u64 {
                    let range = Some((i % 10) * 100..(i % 10) * 100 + 100);
                    if t % 2 == 0 {
                        storage
                            .read(data(), PATH, range, CacheFill::Bypass)
                            .await
                            .unwrap();
                    } else {
                        storage
                            .read(data(), PATH, range, CacheFill::ReadWrite)
                            .await
                            .unwrap();
                        query_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let stats = disk.kind_stats(data());
    assert_eq!(
        stats.hits + stats.misses,
        query_reads.load(std::sync::atomic::Ordering::Relaxed)
    );
}

/// Two storages sharing the disk cache under different key prefixes read different objects at
/// one relative path: their concurrent unit reads do not share bytes, and a rewrite of one is
/// seen by the reads after it, prefix included.
#[tokio::test(flavor = "multi_thread")]
async fn test_unit_reads_of_prefixed_storages_are_their_own() {
    let dir = TempDir::new().unwrap();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let disk =
        Arc::new(DiskCache::new(dir.path(), 100_000, [10_000, 20_000, 70_000], false).unwrap());
    let storage_of = |value: u8| {
        let reads = reads.clone();
        let disk = disk.clone();
        async move {
            let op = Operator::new(Memory::default())
                .unwrap()
                .finish()
                .layer(CountingLayer(reads));
            op.write(PATH, Bytes::from(vec![value; 3_000]))
                .await
                .unwrap();
            let storage = DiskCachedStorage::with_disk(op.clone(), Some(disk))
                .with_key_prefix(format!("tenant_{value}/"));
            (op, storage)
        }
    };
    let (op_a, a) = storage_of(1).await;
    let (_op_b, b) = storage_of(2).await;
    let (read_a, read_b) =
        futures::future::join(read_units(&a, PATH, &UNITS), read_units(&b, PATH, &UNITS)).await;
    assert!(read_a.unwrap().iter().all(|u| u.iter().all(|x| *x == 1)));
    assert!(read_b.unwrap().iter().all(|u| u.iter().all(|x| *x == 2)));
    assert_eq!(reads_of(&reads), 2);

    // `a` rewritten and evicted: its reads get the new bytes, and `b` keeps its own entries.
    wait_for(|| disk.stats().queued_bytes == 0);
    op_a.write(PATH, Bytes::from(vec![3u8; 3_000]))
        .await
        .unwrap();
    a.evict_objects(&[PATH.to_string()]).await.unwrap();
    let after = read_units(&a, PATH, &UNITS).await.unwrap();
    assert!(after.iter().all(|u| u.iter().all(|x| *x == 3)));
    assert_eq!(reads_of(&reads), 3);
    let read_b = read_units(&b, PATH, &UNITS).await.unwrap();
    assert!(read_b.iter().all(|u| u.iter().all(|x| *x == 2)));
    assert_eq!(reads_of(&reads), 3, "b read from the disk cache");
}

/// A cold read of more units than the write queue holds returns every unit, and fills what
/// the queue takes: each unit is written or counted as dropped for a full queue.
#[tokio::test(flavor = "multi_thread")]
async fn test_unit_reads_larger_than_the_write_queue() {
    use databend_storages_common_cache::disk::disk_writer::WriterOptions;
    let dir = TempDir::new().unwrap();
    let op = Operator::new(Memory::default()).unwrap().finish();
    op.write(PATH, Bytes::from(vec![5u8; 3_000])).await.unwrap();
    let disk = Arc::new(
        DiskCache::with_writer(
            dir.path(),
            100_000,
            [10_000, 20_000, 70_000],
            WriterOptions {
                queue_bytes: 250,
                queue_wait: std::time::Duration::ZERO,
                ..WriterOptions::new(false)
            },
        )
        .unwrap(),
    );
    let storage = DiskCachedStorage::with_disk(op, Some(disk.clone()));
    let units: Vec<(u32, std::ops::Range<u64>)> = (0..20)
        .map(|i| (i, i as u64 * 150..i as u64 * 150 + 100))
        .collect();
    let read = read_units(&storage, PATH, &units).await.unwrap();
    assert!(
        read.iter()
            .all(|u| u.len() == 100 && u.iter().all(|x| *x == 5))
    );
    wait_for(|| disk.stats().queued_bytes == 0);
    let stats = disk.kind_stats(data());
    assert_eq!(stats.events.written + stats.dropped.iter().sum::<u64>(), 20);
    assert!(stats.events.written >= 1);
}

/// A path written with a leading slash names the same object as without one, as for the
/// operator: a read either way hits what the other cached, and an eviction of the object
/// either way drops it.
#[tokio::test(flavor = "multi_thread")]
async fn test_leading_slashes_name_the_same_object() {
    let dir = TempDir::new().unwrap();
    let (_op, reads, disk, storage) = counted_setup(&dir).await;
    let slashed = format!("/{PATH}");
    storage
        .read(data(), &slashed, Some(0..100), CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..100)));
    let bytes = storage
        .read(data(), PATH, Some(0..100), CacheFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(bytes, object().slice(0..100));
    assert_eq!(reads_of(&reads), 1);
    storage.evict_objects(&[format!("//{PATH}")]).await.unwrap();
    assert!(!disk.contains(data(), &DiskKey::range(PATH, 0..100)));
}

/// `evict_objects` drops the units of every object it is given, and does nothing without a
/// disk cache.
#[tokio::test(flavor = "multi_thread")]
async fn test_evict_objects_drops_the_units() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    storage
        .read(data(), PATH, Some(0..100), CacheFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..100)));
    storage
        .evict_objects(&[PATH.to_string(), "1/2/_b/none.parquet".to_string()])
        .await
        .unwrap();
    assert!(!disk.contains(data(), &DiskKey::range(PATH, 0..100)));
    assert_eq!(disk.kind_usage(data()), (0, 0));
    let without = DiskCachedStorage::with_disk(storage.operator().clone(), None);
    without.evict_objects(&[PATH.to_string()]).await.unwrap();
}
