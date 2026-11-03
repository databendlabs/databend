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
use databend_storages_common_cache::disk::disk_writer::WriteJob;
use databend_storages_common_cache::disk::disk_writer::WriterOptions;
use databend_storages_common_cache::disk::fill_fence::FillFence;
use databend_storages_common_cache::disk::fill_fence::FillTicket;
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
    let ticket = Arc::new(FillFence::default()).begin();
    job_of(pool, kind, name, len, ticket)
}

fn job_of(
    pool: &Arc<DiskPool>,
    kind: CacheKind,
    name: &str,
    len: usize,
    ticket: FillTicket,
) -> WriteJob {
    WriteJob {
        pool: pool.clone(),
        rel: DiskKey::whole(name).rel_path(kind),
        value: value_of(name, len),
        ticket,
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
        d.queue_full.load(Ordering::Relaxed),
        d.rejected.load(Ordering::Relaxed),
        d.io_error.load(Ordering::Relaxed),
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
    // Written: the writer takes it again, but the indexed key is not reserved again.
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 3);
    assert_eq!(pool.kind_usage(data_kind()), (2, 28));
    assert_eq!(drops(&writer), (0, 1, 0));
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

static GROW: Gate = Gate::new();
fn grow_gate(_: &Path, _: u64) -> bool {
    GROW.pass();
    false
}

/// A larger queue set at runtime lets a reader waiting for room in at once, while the write
/// that fills the queue is still running.
#[tokio::test(flavor = "multi_thread")]
async fn test_larger_queue_lets_a_waiting_entry_in() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = Arc::new(DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: grow_gate,
            ..options(1, 100, Duration::from_secs(30))
        },
    ));
    GROW.close();
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 80)).await);
    GROW.wait_entered(1);
    let waiting = {
        let (writer, pool) = (writer.clone(), pool.clone());
        spawn(async move { writer.enqueue(job(&pool, data_kind(), "b", 80)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    writer.set_queue_bytes(1_000);
    let queued = tokio::time::timeout(Duration::from_secs(10), waiting).await;
    assert!(queued.expect("the entry still waits").unwrap());
    assert_eq!(writer.settings().queue_bytes(), 1_000);
    GROW.open();
    wait_for(|| idle(&writer));
    assert_eq!(pool.stats().items, 2);
}

static RATIO: AtomicU64 = AtomicU64::new(0);
fn ratio_seen(_: &Path, ratio: u64) -> bool {
    RATIO.store(ratio, Ordering::Relaxed);
    false
}

/// The writer checks the file system against the watermark as last set, and syncs entries as
/// last set; a shorter wait applies to the next reader.
#[tokio::test(flavor = "multi_thread")]
async fn test_settings_apply_to_later_entries() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: ratio_seen,
            ..options(1, 1 << 20, Duration::from_millis(50))
        },
    );
    assert!(writer.enqueue(job(&pool, data_kind(), "a", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(RATIO.load(Ordering::Relaxed), 90);

    writer.set_watermark_ratio(75);
    writer.set_sync_data(true);
    writer.set_queue_wait(Duration::ZERO);
    assert!(writer.enqueue(job(&pool, data_kind(), "b", 10)).await);
    wait_for(|| idle(&writer));
    assert_eq!(RATIO.load(Ordering::Relaxed), 75);
    let settings = writer.settings();
    assert!(settings.sync_data());
    assert_eq!(settings.queue_wait(), Duration::ZERO);
    assert_eq!(pool.stats().items, 2);
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
/// queued again later.
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
    wait_for(|| writer.pending_entries() == 2);
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

static FENCE: Gate = Gate::new();
fn fence_gate(_: &Path, _: u64) -> bool {
    FENCE.pass();
    false
}

/// An entry queued before its object is invalidated is not published: its file is deleted, it
/// counts as rejected, and the ticket of its fill is released. An entry of a fill started after
/// the invalidation is published.
#[tokio::test(flavor = "multi_thread")]
async fn test_entry_of_an_invalidated_object_is_not_published() {
    let dir = TempDir::new().unwrap();
    let pool = pool_at(dir.path().join("data"), Pool::Data, 100_000);
    let writer = DiskWriter::start(
        vec![pool.clone()],
        dir.path().to_path_buf(),
        WriterOptions {
            over_watermark: fence_gate,
            ..options(1, 100_000, Duration::from_secs(30))
        },
    );
    let fence = Arc::new(FillFence::default());
    FENCE.close();
    let stale = fence.begin();
    assert!(
        writer
            .enqueue(job_of(&pool, data_kind(), "a", 60, stale))
            .await
    );
    FENCE.wait_entered(1);
    let (at, oldest) = fence.invalidation_point();
    assert!(oldest.is_some());
    assert!(
        pool.remove_object(&DiskKey::object_id("a"), at, oldest)
            .is_empty()
    );
    FENCE.open();
    wait_for(|| idle(&writer));
    assert_eq!(drops(&writer), (0, 1, 0));
    assert_eq!(pool.stats().items, 0);
    assert!(files_under(&dir.path().join("data")).is_empty());
    // The ticket goes with the job, just after the writer is done with it.
    wait_for(|| fence.invalidation_point().1.is_none());

    assert!(
        writer
            .enqueue(job_of(&pool, data_kind(), "a", 60, fence.begin()))
            .await
    );
    wait_for(|| idle(&writer));
    assert!(pool.contains(&DiskKey::whole("a").rel_path(data_kind())));
}

static SERVE: Gate = Gate::new();
fn serve_gate(_: &Path, _: u64) -> bool {
    SERVE.pass();
    false
}

/// An entry read and still waiting to be written is served by the disk cache, so concurrent
/// scans of the same data read it from storage once; once its object changes, it is not, and
/// it is never published. An entry that is published is then read from disk.
#[tokio::test(flavor = "multi_thread")]
async fn test_entries_waiting_to_be_written_are_served_until_their_object_changes() {
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
    let changed = DiskKey::range("1/2/_b/changed.parquet", 0..10);
    let kept = DiskKey::range("1/2/_b/kept.parquet", 0..10);
    let value = Bytes::from_static(b"0123456789");
    SERVE.close();
    let fill = cache.begin_fill();
    assert!(cache.put(kind, &changed, value.clone(), &fill).await);
    SERVE.wait_entered(1);
    assert!(cache.put(kind, &kept, value.clone(), &fill).await);
    drop(fill);

    for key in [&changed, &kept] {
        assert!(!cache.contains(kind, key));
        assert_eq!(cache.get(kind, key).await, Some(value.clone()), "{key}");
    }
    let change = cache.begin_change("1/2/_b/changed.parquet").unwrap();
    assert_eq!(cache.get(kind, &changed).await, None);
    drop(change);
    assert_eq!(cache.get(kind, &changed).await, None);

    SERVE.open();
    wait_for(|| cache.stats().queued_bytes == 0);
    assert!(!cache.contains(kind, &changed));
    assert_eq!(cache.get(kind, &changed).await, None);
    assert!(cache.contains(kind, &kept));
    assert_eq!(cache.get(kind, &kept).await, Some(value));
}
