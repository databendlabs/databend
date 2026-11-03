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

//! `DiskWriter`: the byte-bounded queue and its wait, dedup of pending entries, the reasons
//! entries are dropped, panics, eviction under disk pressure, and concurrent enqueues.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use databend_common_base::runtime::spawn;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::disk::disk_file::TMP_SUFFIX;
use databend_storages_common_cache::disk::disk_file::read_entry;
use databend_storages_common_cache::disk::disk_pool::DiskPool;
use databend_storages_common_cache::disk::disk_writer::DiskWriter;
use databend_storages_common_cache::disk::disk_writer::DropReason;
use databend_storages_common_cache::disk::disk_writer::WriteJob;
use databend_storages_common_cache::disk::disk_writer::WriterOptions;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::segment;
use parking_lot::Condvar;
use parking_lot::Mutex;
use tempfile::TempDir;

use super::files_under;
use crate::fakes::wait_for;

/// Holds writers at the start of `write` while closed: a job taken off the channel stays
/// queued and pending. Each test has its own, as tests run in parallel.
struct Gate {
    closed: Mutex<bool>,
    cv: Condvar,
    entered: AtomicUsize,
}

impl Gate {
    const fn new() -> Self {
        Gate {
            closed: Mutex::new(false),
            cv: Condvar::new(),
            entered: AtomicUsize::new(0),
        }
    }

    fn pass(&self) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let mut closed = self.closed.lock();
        while *closed {
            self.cv.wait(&mut closed);
        }
    }

    fn close(&self) {
        *self.closed.lock() = true;
    }

    fn open(&self) {
        *self.closed.lock() = false;
        self.cv.notify_all();
    }

    fn wait_entered(&self, n: usize) {
        wait_for(|| self.entered.load(Ordering::SeqCst) >= n);
    }
}

fn never_full(_: &Path, _: u64) -> bool {
    false
}

fn options(writers: usize, queue_bytes: u64, queue_wait: Duration) -> WriterOptions {
    WriterOptions {
        writers,
        queue_bytes,
        queue_wait,
        sync_data: false,
        watermark_ratio: 90,
        over_watermark: never_full,
    }
}

fn data_kind() -> CacheKind {
    CacheKind::of::<column::Data>()
}

fn pool_at(root: PathBuf, pool: Pool, cap: u64) -> Arc<DiskPool> {
    std::fs::create_dir_all(&root).unwrap();
    Arc::new(DiskPool::new(pool, root, cap))
}

fn job(pool: &Arc<DiskPool>, kind: CacheKind, name: &str, len: usize) -> WriteJob {
    WriteJob {
        pool: pool.clone(),
        rel: DiskKey::whole(name).rel_path(kind),
        value: value_of(name, len),
    }
}

/// Bytes that tell entries apart, so a file read back is checked against its key.
fn value_of(name: &str, len: usize) -> Bytes {
    let seed = crc32fast::hash(name.as_bytes()) as u8;
    Bytes::from(
        (0..len)
            .map(|i| seed.wrapping_add(i as u8))
            .collect::<Vec<_>>(),
    )
}

fn idle(writer: &DiskWriter) -> bool {
    writer.queued_bytes() == 0 && writer.pending_entries() == 0
}

fn drops(writer: &DiskWriter) -> (u64, u64, u64) {
    let d = writer.drops();
    (
        d.total(DropReason::QueueFull),
        d.total(DropReason::Rejected),
        d.total(DropReason::IoError),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_writes_and_publishes() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 10_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(2, 1 << 20, Duration::from_millis(50)),
    );
    let job = job(&pool, data_kind(), "a", 100);
    let rel = job.rel.clone();
    assert!(writer.enqueue(job).await);
    wait_for(|| idle(&writer));

    let (path, size) = pool.lookup(&rel).unwrap();
    assert_eq!(size, 104, "value and crc");
    assert_eq!(read_entry(path, 104).unwrap(), value_of("a", 100));
    assert_eq!(pool.stats().items, 1);
    assert_eq!(pool.kind_usage(data_kind()), (1, 104));
    assert_eq!(drops(&writer), (0, 0, 0));
    // No tmp file is left behind.
    assert!(
        files_under(dir.path())
            .iter()
            .all(|p| !p.to_string_lossy().ends_with(TMP_SUFFIX))
    );
}

static DEDUP: Gate = Gate::new();
fn dedup_gate(_: &Path, _: u64) -> bool {
    DEDUP.pass();
    false
}

/// An entry queued or being written is not queued again; once done it can be.
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_entry_is_not_queued_twice() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 10_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: dedup_gate,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    DEDUP.close();
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    DEDUP.wait_entered(1);
    // Being written.
    assert!(!writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    // Queued behind it.
    assert!(writer.enqueue(job(&pool, data_kind(), "b", 10)).await);
    assert!(!writer.enqueue(job(&pool, data_kind(), "b", 10)).await);
    // Same key in another kind is another entry.
    assert!(
        writer
            .enqueue(job(&pool, CacheKind::of::<segment::Info>(), "a", 10))
            .await
    );
    assert_eq!(writer.queued_bytes(), 30);
    DEDUP.open();
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 3);
    assert_eq!(drops(&writer), (0, 0, 0));
    // Written: indexed, so not queued again, and not counted as a drop.
    assert!(!writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    assert!(idle(&writer));
    assert_eq!(pool.stats().items, 3);
    assert_eq!(pool.kind_usage(data_kind()), (2, 28));
    assert_eq!(drops(&writer), (0, 0, 0));
}

static FULL: Gate = Gate::new();
fn full_gate(_: &Path, _: u64) -> bool {
    FULL.pass();
    false
}

/// The queue holds `queue_bytes`; an entry that does not fit waits `queue_wait` and is
/// dropped. An entry larger than the queue goes in when the queue is empty.
#[tokio::test(flavor = "multi_thread")]
async fn test_full_queue_drops_after_waiting() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let wait = Duration::from_millis(30);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: full_gate,
            ..options(1, 100, wait)
        },
    );
    FULL.close();
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 60)).await);
    FULL.wait_entered(1);
    assert!(writer.enqueue(job(&pool, data_kind(), "b", 40)).await);
    assert_eq!(writer.queued_bytes(), 100);

    let start = Instant::now();
    assert!(!writer.enqueue(job(&pool, data_kind(), "c", 1)).await);
    assert!(start.elapsed() >= wait);
    assert!(!writer.enqueue(job(&pool, data_kind(), "big", 500)).await);
    assert_eq!(drops(&writer).0, 2);
    // Dropped entries are not left pending: only "a", being written, and "b", queued.
    assert_eq!(writer.pending_entries(), 2);

    FULL.open();
    wait_for(|| idle(&writer));
    assert!(writer.enqueue(job(&pool, data_kind(), "big", 500)).await);
    assert!(writer.enqueue(job(&pool, data_kind(), "c", 1)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 4);
    assert_eq!(drops(&writer), (2, 0, 0));
}

static ROOM: Gate = Gate::new();
fn room_gate(_: &Path, _: u64) -> bool {
    ROOM.pass();
    false
}

/// A reader waiting for room is woken when a write completes, well before its deadline.
#[tokio::test(flavor = "multi_thread")]
async fn test_waiting_entry_goes_in_when_room_frees() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: room_gate,
            ..options(1, 100, Duration::from_secs(30))
        },
    ));
    ROOM.close();
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 80)).await);
    ROOM.wait_entered(1);
    let waiting = {
        let (writer, pool) = (writer.clone(), pool.clone());
        spawn(async move { writer.enqueue(job(&pool, data_kind(), "b", 80)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    let start = Instant::now();
    ROOM.open();
    assert!(waiting.await.unwrap());
    assert!(start.elapsed() < Duration::from_secs(10));
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 2);
}

static RATIO: AtomicU64 = AtomicU64::new(0);
fn ratio_seen(_: &Path, ratio: u64) -> bool {
    RATIO.store(ratio, Ordering::Relaxed);
    false
}

/// The writer checks the file system against the configured watermark before it writes.
#[tokio::test(flavor = "multi_thread")]
async fn test_writer_checks_the_configured_watermark() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: ratio_seen,
            watermark_ratio: 75,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(RATIO.load(Ordering::Relaxed), 75);
    assert_eq!(pool.stats().items, 1);
}

/// An entry larger than a quarter of its pool is dropped as rejected, and leaves no room
/// reserved.
#[tokio::test(flavor = "multi_thread")]
async fn test_oversized_entry_is_rejected() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 1000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(1, 1 << 20, Duration::from_millis(50)),
    );
    assert!(writer.enqueue(job(&pool, data_kind(), "big", 300)).await);
    wait_for(|| idle(&writer));
    assert_eq!(drops(&writer), (0, 1, 0));
    assert_eq!(pool.stats().items, 0);
    assert_no_reservation(&pool);
}

/// A write that fails counts as an IO error and gives its reservation back.
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_write_releases_its_reservation() {
    let dir = TempDir::new().unwrap();
    // The pool root is under a regular file: its directories cannot be created.
    std::fs::write(dir.path().join("file"), b"x").unwrap();
    let pool = Arc::new(DiskPool::new(
        Pool::Data,
        dir.path().join("file/data"),
        1000,
    ));
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(1, 1 << 20, Duration::from_millis(50)),
    );
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 100)).await);
    wait_for(|| idle(&writer));
    assert_eq!(drops(&writer), (0, 0, 1));
    assert_eq!(pool.stats().items, 0);
    assert_no_reservation(&pool);
}

static PANICKED: AtomicBool = AtomicBool::new(false);
fn panic_once(_: &Path, _: u64) -> bool {
    if !PANICKED.swap(true, Ordering::SeqCst) {
        panic!("injected");
    }
    false
}

/// A panic while writing loses that entry only: the queue and the pending set are cleaned
/// up, and the writer thread goes on with the next entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_panic_in_writer_keeps_accounting_and_thread() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 10_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: panic_once,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 0);
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 1);
    assert_no_reservation(&pool);
}

static PRESSURE: AtomicUsize = AtomicUsize::new(0);
fn pressure(_: &Path, _: u64) -> bool {
    PRESSURE
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

/// Over the watermark, a writer evicts from the data pool first, then index, then meta,
/// as long as the file system stays over it.
#[tokio::test(flavor = "multi_thread")]
async fn test_disk_pressure_evicts_data_then_index_then_meta() {
    let dir = TempDir::new().unwrap();
    let data = pool_at(dir.path().join("data"), Pool::Data, 10_000);
    let index = pool_at(dir.path().join("index"), Pool::Index, 10_000);
    let meta = pool_at(dir.path().join("meta"), Pool::Meta, 10_000);
    let writer = DiskWriter::start(
        vec![data.clone(), index.clone(), meta.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: pressure,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    let meta_kind = CacheKind::of::<segment::Info>();
    for (pool, name) in [(&data, "d"), (&index, "i"), (&meta, "m")] {
        for i in 0..3 {
            assert!(
                writer
                    .enqueue(job(pool, meta_kind, &format!("{name}{i}"), 100))
                    .await
            );
        }
    }
    wait_for(|| idle(&writer));
    let items = || (data.stats().items, index.stats().items, meta.stats().items);
    assert_eq!(items(), (3, 3, 3));

    // Over the watermark for one check: the data pool goes, the others stay.
    PRESSURE.store(1, Ordering::SeqCst);
    assert!(writer.enqueue(job(&meta, meta_kind, "m3", 100)).await);
    wait_for(|| idle(&writer));
    assert_eq!(items(), (0, 3, 4));
    assert_eq!(files_under(&dir.path().join("data")).len(), 0);

    // Over it for two checks: index goes too, meta stays.
    PRESSURE.store(2, Ordering::SeqCst);
    assert!(writer.enqueue(job(&meta, meta_kind, "m4", 100)).await);
    wait_for(|| idle(&writer));
    assert_eq!(items(), (0, 0, 5));
    assert_eq!(files_under(&dir.path().join("index")).len(), 0);
    assert_eq!(files_under(&dir.path().join("meta")).len(), 5);

    // An entry too large for its pool, over a quarter of it, is refused before the pressure
    // check: nothing is evicted for it.
    PRESSURE.store(2, Ordering::SeqCst);
    assert!(writer.enqueue(job(&meta, meta_kind, "huge", 2_600)).await);
    wait_for(|| idle(&writer));
    assert_eq!(items(), (0, 0, 5));
    assert_eq!(drops(&writer).1, 1);

    // So is an entry of a kind turned off while it was queued.
    PRESSURE.store(2, Ordering::SeqCst);
    meta.set_kind_enabled(meta_kind, false);
    assert!(writer.enqueue(job(&meta, meta_kind, "off", 100)).await);
    wait_for(|| idle(&writer));
    assert_eq!(
        items(),
        (0, 0, 0),
        "the kind's own entries are taken out, no other"
    );
    assert_eq!(drops(&writer).1, 2);
    PRESSURE.store(0, Ordering::SeqCst);
}

static INDEXED: Gate = Gate::new();
fn indexed_gate(_: &Path, _: u64) -> bool {
    INDEXED.pass();
    false
}

/// An entry whose key was indexed after it was queued, as by the restart scan, is refused
/// before the pressure check: nothing is evicted for it.
#[tokio::test(flavor = "multi_thread")]
async fn test_entry_indexed_while_queued_evicts_nothing() {
    let dir = TempDir::new().unwrap();
    let meta = pool_at(dir.path().join("meta"), Pool::Meta, 10_000);
    let writer = DiskWriter::start(
        vec![meta.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: indexed_gate,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    let meta_kind = CacheKind::of::<segment::Info>();
    // The writer is held in the pressure check of "gate" while "late" waits behind it.
    INDEXED.close();
    assert!(writer.enqueue(job(&meta, meta_kind, "gate", 100)).await);
    INDEXED.wait_entered(1);
    let late = job(&meta, meta_kind, "late", 100);
    let rel = late.rel.clone();
    assert!(writer.enqueue(late).await);
    // Indexed meanwhile, as the restart scan does with a file of the previous run.
    let r = meta.reserve(&rel, 104);
    assert!(r.reserved);
    assert!(
        meta.publish(rel.clone(), 104, 104, || Ok(()))
            .unwrap()
            .indexed
    );
    INDEXED.open();
    wait_for(|| idle(&writer));
    // "gate" checked the watermark and went in; "late" was refused before the check, so the
    // watermark was checked once in all: nothing could have been evicted for "late".
    assert_eq!(meta.stats().items, 2);
    assert_eq!(drops(&writer).1, 1);
    assert_eq!(INDEXED.entered.load(Ordering::SeqCst), 1);

    // So is an entry that the writes in flight alone leave no room for: evicting every entry
    // could not make it fit, and the watermark is not checked for it either.
    // Four writes of a quarter of the pool each, in flight.
    let held: Vec<String> = (0..4)
        .map(|i| DiskKey::whole(format!("held{i}")).rel_path(meta_kind))
        .collect();
    for rel in &held {
        let r = meta.reserve(rel, 2_400);
        assert!(r.reserved);
        meta.delete(&r.victims);
    }
    assert!(
        writer
            .enqueue(job(&meta, meta_kind, "squeezed", 1_000))
            .await
    );
    wait_for(|| idle(&writer));
    assert_eq!(
        meta.stats().items,
        0,
        "the reservations evicted the entries"
    );
    assert_eq!(drops(&writer).1, 2);
    assert_eq!(INDEXED.entered.load(Ordering::SeqCst), 1);
    for rel in &held {
        meta.release(rel, 2_400);
    }
}

/// Many readers queue entries at once, with duplicates, a small queue and a pool that
/// evicts: whatever is dropped, the queue drains to zero, nothing stays pending or
/// reserved, and the index matches the files, each holding the value of its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_enqueues_keep_index_and_files_consistent() {
    let dir = TempDir::new().unwrap();
    let cap = 64 * 1024;
    let pool = pool_at(dir.path().join("data"), Pool::Data, cap);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(4, 8 * 1024, Duration::from_millis(2)),
    ));
    let tasks: Vec<_> = (0..16)
        .map(|t| {
            let (writer, pool) = (writer.clone(), pool.clone());
            spawn(async move {
                let mut queued = 0;
                for i in 0..200usize {
                    // 64 keys shared by all tasks, sizes up to a quarter of the pool.
                    let key = (t * 31 + i * 7) % 64;
                    let len = 1 + (key * 997) % (cap as usize / 4 - 4);
                    if writer
                        .enqueue(job(&pool, data_kind(), &format!("k{key}"), len))
                        .await
                    {
                        queued += 1;
                    }
                }
                queued
            })
        })
        .collect();
    let mut queued = 0;
    for t in tasks {
        queued += t.await.unwrap();
    }
    assert!(queued > 0);
    wait_for(|| idle(&writer));

    // Entries may be rejected while an earlier file of their key is being deleted.
    assert_eq!(drops(&writer).2, 0);
    let files = files_under(&dir.path().join("data"));
    let stats = pool.stats();
    assert_eq!(stats.items, files.len() as u64);
    assert!(stats.bytes <= cap);
    assert_eq!(pool.kind_usage(data_kind()), (stats.items, stats.bytes));
    let on_disk: u64 = files
        .iter()
        .map(|f| std::fs::metadata(f).unwrap().len())
        .sum();
    assert_eq!(on_disk, stats.bytes);
    for key in 0..64 {
        let name = format!("k{key}");
        if let Some((path, size)) = pool.lookup(&DiskKey::whole(&name).rel_path(data_kind())) {
            let len = 1 + (key * 997) % (cap as usize / 4 - 4);
            assert_eq!(
                read_entry(path, size as usize).unwrap(),
                value_of(&name, len)
            );
        }
    }
    assert_no_reservation(&pool);
}

fn assert_no_reservation(pool: &DiskPool) {
    assert_eq!(pool.stats().writing, 0);
}

static CANCEL: Gate = Gate::new();
fn cancel_gate(_: &Path, _: u64) -> bool {
    CANCEL.pass();
    false
}

/// A reader cancelled while it waits for room leaves nothing pending: the same entry can be
/// queued again later. An entry waiting for room is not pending yet: its bytes are made, and it
/// is listed, once it has room.
#[tokio::test(flavor = "multi_thread")]
async fn test_cancelled_enqueue_leaves_nothing_pending() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: cancel_gate,
            ..options(1, 100, Duration::from_secs(30))
        },
    ));
    CANCEL.close();
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 60)).await);
    CANCEL.wait_entered(1);
    let waiting = {
        let (writer, pool) = (writer.clone(), pool.clone());
        spawn(async move { writer.enqueue(job(&pool, data_kind(), "b", 60)).await })
    };
    // Waiting for room: listed nowhere, holding no room.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    assert_eq!(writer.pending_entries(), 1);
    assert_eq!(writer.queued_bytes(), 60);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(writer.pending_entries(), 1);
    assert_eq!(
        writer.queued_bytes(),
        60,
        "the cancelled entry took no room"
    );

    CANCEL.open();
    wait_for(|| idle(&writer));
    assert!(writer.enqueue(job(&pool, data_kind(), "b", 60)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 2);
    assert_eq!(drops(&writer), (0, 0, 0));
}

static SERVE: Gate = Gate::new();
fn serve_gate(_: &Path, _: u64) -> bool {
    SERVE.pass();
    false
}

/// An entry read and still waiting to be written is served by the disk cache, so concurrent
/// scans of the same data read it from storage once. Once published it is read from disk.
#[tokio::test(flavor = "multi_thread")]
async fn test_entries_waiting_to_be_written_are_served() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::with_writer(
            dir.path(),
            1 << 20,
            [1 << 18, 1 << 18, 1 << 18],
            WriterOptions {
                over_watermark: serve_gate,
                ..options(1, 1 << 20, Duration::from_millis(50))
            },
        )
        .unwrap(),
    );
    let kind = data_kind();
    let writing = DiskKey::range("1/2/_b/writing.parquet", 0..10);
    let queued = DiskKey::range("1/2/_b/queued.parquet", 0..10);
    let value = Bytes::from_static(b"0123456789");
    SERVE.close();
    assert!(cache.put(kind, &writing, value.clone()).await);
    SERVE.wait_entered(1);
    assert!(cache.put(kind, &queued, value.clone()).await);

    for key in [&writing, &queued] {
        assert!(!cache.contains(kind, key));
        assert_eq!(cache.get(kind, key).await, Some(value.clone()), "{key}");
    }
    assert_eq!(cache.kind_stats(kind).queued_hits, 2);

    SERVE.open();
    wait_for(|| cache.stats().queued_bytes == 0);
    for key in [&writing, &queued] {
        assert!(cache.contains(kind, key));
        assert_eq!(cache.get(kind, key).await, Some(value.clone()), "{key}");
    }
    assert_eq!(cache.kind_stats(kind).queued_hits, 2);
}

/// Readers look up entries while they are queued, written, published and evicted with their
/// objects, from many tasks: a lookup returns the bytes put under its key or nothing, never a
/// torn or foreign value, whether from the queue or from disk. Every lookup is counted once, as
/// a hit or a miss, and once the writes are done and the objects evicted, nothing is served.
#[tokio::test(flavor = "multi_thread")]
async fn test_lookups_racing_writes_and_evictions_return_whole_values() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::with_writer(
            dir.path(),
            1 << 24,
            [1 << 22, 1 << 22, 1 << 23],
            options(4, 1 << 20, Duration::from_millis(50)),
        )
        .unwrap(),
    );
    let kind = data_kind();
    const OBJECTS: usize = 64;
    let path = |i: usize| format!("1/2/_b/race_{i}.parquet");
    let value = |i: usize| Bytes::from(vec![i as u8; 1_000]);
    let lookups = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let cache = cache.clone();
            spawn(async move {
                for round in 0..20 {
                    for i in (w..OBJECTS).step_by(4) {
                        let key = DiskKey::range(path(i), 0..1_000);
                        cache.put(kind, &key, value(i)).await;
                        if (i + round) % 5 == 0 {
                            cache.evict_object(&path(i));
                        }
                    }
                }
            })
        })
        .collect();
    let readers: Vec<_> = (0..8)
        .map(|r| {
            let (cache, lookups) = (cache.clone(), lookups.clone());
            spawn(async move {
                for n in 0..2_000 {
                    let i = (r * 7 + n) % OBJECTS;
                    let got = cache.get(kind, &DiskKey::range(path(i), 0..1_000)).await;
                    lookups.fetch_add(1, Ordering::Relaxed);
                    if let Some(bytes) = got {
                        assert_eq!(bytes, value(i), "object {i}");
                    }
                }
            })
        })
        .collect();
    for task in writers.into_iter().chain(readers) {
        task.await.unwrap();
    }
    wait_for(|| cache.stats().queued_bytes == 0);
    for i in 0..OBJECTS {
        cache.evict_object(&path(i));
    }
    let stats = cache.kind_stats(kind);
    assert_eq!(stats.hits + stats.misses, lookups.load(Ordering::Relaxed));
    assert!(stats.queued_hits <= stats.hits);
    for i in 0..OBJECTS {
        let key = DiskKey::range(path(i), 0..1_000);
        assert!(!cache.contains(kind, &key));
        assert!(cache.get(kind, &key).await.is_none());
    }
    assert_eq!(cache.kind_usage(kind), (0, 0));
}

static MADE: Gate = Gate::new();
fn made_gate(_: &Path, _: u64) -> bool {
    MADE.pass();
    false
}

/// `enqueue_with` makes the bytes only once the queue has room for them: not while it waits,
/// not when the entry is dropped for a full queue, not when the entry is already queued.
#[tokio::test(flavor = "multi_thread")]
async fn test_enqueue_with_makes_the_bytes_once_there_is_room() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: made_gate,
            ..options(1, 100, Duration::from_millis(30))
        },
    ));
    let made = Arc::new(AtomicUsize::new(0));
    let enqueue = |name: &'static str, len: usize| {
        let (writer, pool, made) = (writer.clone(), pool.clone(), made.clone());
        async move {
            let WriteJob { pool, rel, .. } = job(&pool, data_kind(), name, len);
            writer
                .enqueue_with(pool, rel, len as u64, move || {
                    made.fetch_add(1, Ordering::SeqCst);
                    value_of(name, len)
                })
                .await
        }
    };
    MADE.close();
    assert!(enqueue("a", 100).await);
    MADE.wait_entered(1);
    assert_eq!(made.load(Ordering::SeqCst), 1);
    // The queue is full: dropped after waiting, never made.
    assert!(!enqueue("b", 10).await);
    assert_eq!(made.load(Ordering::SeqCst), 1);
    assert_eq!(drops(&writer).0, 1);
    // Already queued: refused at once, never made.
    assert!(!enqueue("a", 100).await);
    assert_eq!(made.load(Ordering::SeqCst), 1);
    // Waiting for room, not made until "a" is written.
    let waiting = {
        let enqueue = enqueue("c", 10);
        spawn(enqueue)
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(made.load(Ordering::SeqCst), 1);
    MADE.open();
    assert!(waiting.await.unwrap());
    assert_eq!(made.load(Ordering::SeqCst), 2);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 2);
}

/// Many tasks queue entries made on demand into a small queue at once: every entry is made
/// while it holds room, so the bytes made and not yet written never exceed `queue_bytes`;
/// each entry is written or dropped, and only those queued are made.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_enqueue_with_keeps_made_bytes_within_the_queue() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 1 << 30);
    const QUEUE: u64 = 4 * 1024;
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(2, QUEUE, Duration::from_millis(20)),
    ));
    let made = Arc::new(AtomicU64::new(0));
    let queued = Arc::new(AtomicU64::new(0));
    let tasks: Vec<_> = (0..16)
        .map(|t| {
            let (writer, pool, made, queued) =
                (writer.clone(), pool.clone(), made.clone(), queued.clone());
            spawn(async move {
                for i in 0..100usize {
                    let len = 64 + (i * 7919 + t * 31) % 1_000;
                    let name = format!("t{t}_{i}");
                    let WriteJob { pool, rel, .. } = job(&pool, data_kind(), &name, len);
                    let (writer2, made) = (writer.clone(), made.clone());
                    let ok = writer
                        .enqueue_with(pool, rel, len as u64, move || {
                            // Room is held for this entry: it is counted in the queue.
                            let queued = writer2.queued_bytes();
                            assert!(queued >= len as u64 && queued <= QUEUE, "{queued}");
                            made.fetch_add(1, Ordering::Relaxed);
                            value_of(&name, len)
                        })
                        .await;
                    if ok {
                        queued.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    wait_for(|| idle(&writer));
    let made = made.load(Ordering::Relaxed);
    assert_eq!(made, queued.load(Ordering::Relaxed));
    let (queue_full, rejected, io_error) = drops(&writer);
    assert_eq!(made + queue_full, 16 * 100);
    assert_eq!(pool.stats().items + rejected + io_error, made);
}

/// Making the bytes of an entry unwinds, as an allocation over the query's memory limit does:
/// the room the entry took is given back and nothing of it stays pending, so the queue keeps
/// its capacity.
#[tokio::test(flavor = "multi_thread")]
async fn test_unwinding_while_making_the_bytes_gives_the_room_back() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        options(1, 100, Duration::from_millis(30)),
    ));
    let unwound = {
        let (writer, pool) = (writer.clone(), pool.clone());
        spawn(async move {
            let WriteJob { pool, rel, .. } = job(&pool, data_kind(), "a", 100);
            writer
                .enqueue_with(pool, rel, 100, || panic!("over the memory limit"))
                .await
        })
    };
    assert!(unwound.await.unwrap_err().is_panic());
    assert_eq!(writer.queued_bytes(), 0);
    assert_eq!(writer.pending_entries(), 0);
    // The key is free, and the queue takes entries as before.
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 60)).await);
    assert!(writer.enqueue(job(&pool, data_kind(), "b", 40)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 2);
    assert_eq!(drops(&writer), (0, 0, 0));
}
