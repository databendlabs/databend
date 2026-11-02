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
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskFill;
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
        .read(data(), PATH, Some(10..20), DiskFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(range, object().slice(10..20));
    let whole = storage
        .read(data(), PATH, None, DiskFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(whole, object());
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 10..20)));
    wait_for(|| disk.contains(data(), &DiskKey::whole(PATH)));

    // Served from disk once the object is gone.
    op.delete(PATH).await.unwrap();
    for fill in [DiskFill::ReadWrite, DiskFill::ReadOnly] {
        let hit = storage
            .read(data(), PATH, Some(10..20), fill)
            .await
            .unwrap();
        assert_eq!(hit, object().slice(10..20));
    }
    assert!(
        storage
            .read(data(), PATH, Some(10..20), DiskFill::Bypass)
            .await
            .is_err(),
        "bypass reads storage"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_and_bypass_do_not_fill() {
    let dir = TempDir::new().unwrap();
    let (_op, disk, storage) = setup(&dir).await;
    for (fill, range) in [(DiskFill::ReadOnly, 0..5), (DiskFill::Bypass, 5..10)] {
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
        .read(data(), PATH, None, DiskFill::ReadWrite)
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
        .read(data(), PATH, Some(100..300), DiskFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 100..300)));

    let read = storage
        .read_units(data(), PATH, &units, DiskFill::ReadWrite, &settings)
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
        .read_units(data(), PATH, &units, DiskFill::ReadOnly, &settings)
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
            .read_units(data(), PATH, &units, DiskFill::Bypass, &settings)
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
    assert!(!storage.uses_disk(data(), DiskFill::ReadWrite));
    let bytes = storage
        .read(data(), PATH, Some(0..8), DiskFill::ReadWrite)
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
                .read(data(), PATH, Some(0..100), DiskFill::ReadWrite)
                .await
                .unwrap()
        }
    }))
    .await;
    assert!(results.iter().all(|b| *b == object().slice(0..100)));
    assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 1);
    wait_for(|| disk.contains(data(), &DiskKey::range(PATH, 0..100)));
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
                .read(data(), "missing", Some(0..10), DiskFill::ReadWrite)
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
            .read(data(), "missing", Some(0..10), DiskFill::ReadWrite)
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
        .read(data(), PATH, Some(0..50), DiskFill::ReadWrite)
        .await
        .unwrap();
    wait_for(|| disk.contains(data(), &key));
    for file in super::files_under(&dir.path().join("v2/data")) {
        std::fs::write(file, b"garbage").unwrap();
    }
    let bytes = storage
        .read(data(), PATH, Some(0..50), DiskFill::ReadWrite)
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
        .read_ranges(data(), PATH, ranges.clone(), DiskFill::ReadWrite)
        .await
        .unwrap();
    assert_eq!(read.len(), 3);
    for range in ranges {
        let expected = object().slice(range.start as usize..range.end as usize);
        assert_eq!(read[&range], expected);
        wait_for(|| disk.contains(data(), &DiskKey::range(PATH, range.clone())));
    }
}

/// Counts reads reaching the operator, each taking 50 ms.
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
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Slow enough that concurrent misses find the first read in flight.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        self.inner.read(path, args).await
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
