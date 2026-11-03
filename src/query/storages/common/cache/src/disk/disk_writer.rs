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

//! Writes disk cache entries in the background. The queue is bounded in bytes; a reader whose
//! entry does not fit waits a short while for room, then drops the entry and counts it. Writer
//! threads reserve room in the entry's pool, delete the victims' files, write the entry under a
//! temporary name, then publish it; the pool renames the file into place if it accepts the
//! entry. It may reject it then, when its object was invalidated meanwhile or the pool shrank,
//! and the temporary file is deleted.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use databend_common_base::runtime::catch_unwind;
use log::warn;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::CacheKind;
use crate::disk::disk_file::tmp_path;
use crate::disk::disk_file::write_tmp;
use crate::disk::disk_pool::DiskPool;
use crate::disk::fill_fence::FillTicket;

/// Bytes evicted per pool per pass when the file system is over the watermark.
const WATERMARK_EVICT_BYTES: u64 = 64 * 1024 * 1024;

/// How the writer starts, from `cache.disk`; tests shrink the queue and replace the file
/// system check. All but `writers` and `over_watermark` change at runtime (`WriterSettings`).
#[derive(Clone, Copy)]
pub struct WriterOptions {
    pub writers: usize,
    /// Bytes queued for writing, at most.
    pub queue_bytes: u64,
    /// How long a reader waits for room in the queue before dropping its entry.
    pub queue_wait: Duration,
    pub sync_data: bool,
    /// Percent of the file system in use above which the writer evicts before it writes.
    pub watermark_ratio: u64,
    /// Whether the file system under the root is over `watermark_ratio` percent in use.
    pub over_watermark: fn(&Path, u64) -> bool,
}

impl WriterOptions {
    /// The defaults of `cache.disk`: 4 writer threads, a 256 MiB queue, 50 ms of wait, and
    /// evictions above 90% of the file system in use.
    pub fn new(sync_data: bool) -> Self {
        WriterOptions {
            writers: 4,
            queue_bytes: 256 * 1024 * 1024,
            queue_wait: Duration::from_millis(50),
            sync_data,
            watermark_ratio: 90,
            over_watermark: fs_over_watermark,
        }
    }
}

/// The writer's settings that change at runtime, read at each entry.
pub struct WriterSettings {
    queue_bytes: AtomicU64,
    queue_wait_ms: AtomicU64,
    watermark_ratio: AtomicU64,
    sync_data: AtomicBool,
}

impl WriterSettings {
    fn new(options: &WriterOptions) -> Self {
        WriterSettings {
            queue_bytes: AtomicU64::new(options.queue_bytes),
            queue_wait_ms: AtomicU64::new(options.queue_wait.as_millis() as u64),
            watermark_ratio: AtomicU64::new(options.watermark_ratio),
            sync_data: AtomicBool::new(options.sync_data),
        }
    }

    pub fn queue_bytes(&self) -> u64 {
        self.queue_bytes.load(Ordering::Relaxed)
    }

    pub fn queue_wait(&self) -> Duration {
        Duration::from_millis(self.queue_wait_ms.load(Ordering::Relaxed))
    }

    pub fn watermark_ratio(&self) -> u64 {
        self.watermark_ratio.load(Ordering::Relaxed)
    }

    pub fn sync_data(&self) -> bool {
        self.sync_data.load(Ordering::Relaxed)
    }
}

pub struct WriteJob {
    pub pool: Arc<DiskPool>,
    /// Index key and path under the pool root.
    pub rel: String,
    pub value: Bytes,
    /// Taken before the value was read from storage; held until the entry is published or
    /// dropped.
    pub ticket: FillTicket,
}

/// Why an entry was not written, for `cache_disk_dropped`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// The queue stayed full for `queue_wait`.
    QueueFull,
    /// Refused by its pool: too large, the pool or kind off, no room, or its object changed.
    Rejected,
    /// The file could not be written.
    IoError,
}

impl DropReason {
    pub const ALL: [DropReason; 3] = [
        DropReason::QueueFull,
        DropReason::Rejected,
        DropReason::IoError,
    ];

    pub fn name(self) -> &'static str {
        match self {
            DropReason::QueueFull => "queue_full",
            DropReason::Rejected => "rejected",
            DropReason::IoError => "io_error",
        }
    }
}

/// Entries not written, by kind index and `DropReason`.
pub struct DropCounters {
    per_kind: Box<[[AtomicU64; 3]]>,
}

impl Default for DropCounters {
    fn default() -> Self {
        DropCounters {
            per_kind: CacheKind::all().map(|_| Default::default()).collect(),
        }
    }
}

impl DropCounters {
    fn add(&self, rel: &str, reason: DropReason) {
        if let Some(kind) = rel.split('/').next().and_then(CacheKind::from_name) {
            self.per_kind[kind.index()][reason as usize].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Entries of `kind` dropped for `reason`.
    pub fn of(&self, kind: CacheKind, reason: DropReason) -> u64 {
        self.per_kind[kind.index()][reason as usize].load(Ordering::Relaxed)
    }

    /// Entries of every kind dropped for `reason`.
    pub fn total(&self, reason: DropReason) -> u64 {
        self.per_kind
            .iter()
            .map(|counts| counts[reason as usize].load(Ordering::Relaxed))
            .sum()
    }
}

struct Shared {
    queued: AtomicU64,
    room: Notify,
    /// Entries queued or being written, by pool and key, with their bytes and the ticket of the
    /// fill that read them: a second miss of the same key while the first is pending is not
    /// queued again, and is served these bytes (see `DiskWriter::pending`). Also the number of
    /// the job, so that a job superseded by a newer one does not take the newer one off.
    pending: Mutex<PendingSet>,
    drops: DropCounters,
    settings: WriterSettings,
    over_watermark: fn(&Path, u64) -> bool,
    /// Pools in eviction order under disk pressure: data, index, meta.
    pools: Vec<Arc<DiskPool>>,
    root: PathBuf,
    seq: AtomicU64,
}

/// Entries queued or being written, by pool index and key: their bytes, the ticket of their
/// fill and the number of their job.
type PendingSet = HashMap<(usize, String), (Bytes, FillTicket, u64)>;

pub struct DiskWriter {
    tx: crossbeam_channel::Sender<(WriteJob, u64)>,
    shared: Arc<Shared>,
}

impl DiskWriter {
    /// Writers over `pools`, in eviction order under disk pressure, all under `root`.
    pub fn start(pools: Vec<Arc<DiskPool>>, root: PathBuf, options: WriterOptions) -> DiskWriter {
        let (tx, rx) = crossbeam_channel::unbounded::<(WriteJob, u64)>();
        let writers = options.writers;
        let shared = Arc::new(Shared {
            queued: AtomicU64::new(0),
            room: Notify::new(),
            pending: Mutex::new(HashMap::new()),
            drops: DropCounters::default(),
            settings: WriterSettings::new(&options),
            over_watermark: options.over_watermark,
            pools,
            root,
            seq: AtomicU64::new(0),
        });
        for i in 0..writers {
            let rx = rx.clone();
            let shared = shared.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("disk-cache-writer-{i}"))
                .spawn(move || {
                    while let Ok((job, number)) = rx.recv() {
                        // A panic loses the entry, not the thread; `write` keeps the
                        // accounting right on unwind.
                        if let Err(e) = catch_unwind(|| shared.write(job, number)) {
                            warn!("[DiskCache] writer {i} panicked writing an entry: {e}");
                        }
                    }
                });
            if let Err(e) = spawned {
                warn!("[DiskCache] failed to spawn writer thread {i}: {e}");
            }
        }
        DiskWriter { tx, shared }
    }

    pub fn drops(&self) -> &DropCounters {
        &self.shared.drops
    }

    pub fn settings(&self) -> &WriterSettings {
        &self.shared.settings
    }

    /// Bytes queued at most; readers waiting for room retry at once.
    pub fn set_queue_bytes(&self, bytes: u64) {
        self.shared
            .settings
            .queue_bytes
            .store(bytes, Ordering::Relaxed);
        self.shared.room.notify_waiters();
    }

    /// How long a reader waits for room in a full queue, from its next entry on.
    pub fn set_queue_wait(&self, wait: Duration) {
        self.shared
            .settings
            .queue_wait_ms
            .store(wait.as_millis() as u64, Ordering::Relaxed);
    }

    /// Percent of the file system in use above which entries evict before they are written.
    pub fn set_watermark_ratio(&self, percent: u64) {
        self.shared
            .settings
            .watermark_ratio
            .store(percent, Ordering::Relaxed);
    }

    /// Whether entries written from now on are synced to disk.
    pub fn set_sync_data(&self, sync: bool) {
        self.shared
            .settings
            .sync_data
            .store(sync, Ordering::Relaxed);
    }

    /// Bytes queued or being written.
    pub fn queued_bytes(&self) -> u64 {
        self.shared.queued.load(Ordering::Relaxed)
    }

    /// The bytes queued or being written under `rel` of the pool with index `pool`, and the
    /// ticket of the fill that read them. Held while the bytes are checked against the pool:
    /// the invalidations of their object since the fill started are kept as long as it is.
    pub fn pending(&self, pool: usize, rel: &str) -> Option<(Bytes, FillTicket)> {
        self.shared
            .pending
            .lock()
            .get(&(pool, rel.to_string()))
            .map(|(bytes, ticket, _)| (bytes.clone(), ticket.clone()))
    }

    /// Entries queued or being written.
    pub fn pending_entries(&self) -> usize {
        self.shared.pending.lock().len()
    }

    /// Queues `job`, waiting up to `queue_wait` for room. Returns false when dropped, or when
    /// the same entry is already queued or being written.
    pub async fn enqueue(&self, job: WriteJob) -> bool {
        let WriteJob {
            pool,
            rel,
            value,
            ticket,
        } = job;
        let len = value.len() as u64;
        self.enqueue_with(pool, rel, len, ticket, move || value)
            .await
    }

    /// `enqueue` of an entry of `len` bytes made by `value` once the queue has room for them:
    /// a caller copying them out of a larger buffer holds no copy while it waits, so the
    /// copies of all callers stay within `queue_bytes`. `value` is not called when the entry
    /// is dropped, or already queued.
    pub async fn enqueue_with(
        &self,
        pool: Arc<DiskPool>,
        rel: String,
        len: u64,
        ticket: FillTicket,
        value: impl FnOnce() -> Bytes,
    ) -> bool {
        let key = (pool.pool().index(), rel);
        // A fill its object was invalidated since is rejected now, before it waits for room or
        // makes its bytes, and so does not keep a newer fill of the key out of the queue.
        if !pool.accepts(&key.1, ticket.started()) {
            self.shared.drops.add(&key.1, DropReason::Rejected);
            return false;
        }
        if self.shared.blocks(&pool, &key) {
            return false;
        }
        let settings = &self.shared.settings;
        let deadline = Instant::now() + settings.queue_wait();
        loop {
            let room = self.shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            let queued = self.shared.queued.load(Ordering::Acquire);
            // An entry larger than the whole queue still goes in when the queue is empty.
            if queued == 0 || queued + len <= settings.queue_bytes() {
                if self
                    .shared
                    .queued
                    .compare_exchange(queued, queued + len, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                self.shared.drops.add(&key.1, DropReason::QueueFull);
                return false;
            }
            let _ = tokio::time::timeout(deadline - now, room).await;
        }
        // No await from the room taken to the hand-over: a cancelled caller holds no room.
        let give_back = || {
            self.shared.queued.fetch_sub(len, Ordering::AcqRel);
            self.shared.room.notify_waiters();
        };
        let value = value();
        debug_assert_eq!(value.len() as u64, len);
        let number = self.shared.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut pending = self.shared.pending.lock();
            // Queued by another caller while this one waited for room.
            if Shared::blocks_in(&pending, &pool, &key) {
                drop(pending);
                give_back();
                return false;
            }
            pending.insert(key.clone(), (value.clone(), ticket.clone(), number));
        }
        let (pool, rel) = (pool, key.1.clone());
        if let Err(e) = self.tx.send((
            WriteJob {
                pool,
                rel,
                value,
                ticket,
            },
            number,
        )) {
            // No writer left.
            self.shared.remove_pending(&key, number);
            give_back();
            warn!("[DiskCache] writer threads are gone: {e}");
            return false;
        }
        // The writer takes the key off when done.
        true
    }
}

impl Shared {
    /// Whether an entry pending under `key` keeps another fill of it out of the queue: one
    /// whose object was invalidated since its fill started does not, as it will be rejected.
    fn blocks(&self, pool: &DiskPool, key: &(usize, String)) -> bool {
        Self::blocks_in(&self.pending.lock(), pool, key)
    }

    fn blocks_in(pending: &PendingSet, pool: &DiskPool, key: &(usize, String)) -> bool {
        pending
            .get(key)
            .is_some_and(|(_, ticket, _)| pool.accepts(&key.1, ticket.started()))
    }

    /// Takes job `number` off the pending set, unless a newer job of its key superseded it.
    fn remove_pending(&self, key: &(usize, String), number: u64) {
        let mut pending = self.pending.lock();
        if pending.get(key).is_some_and(|(_, _, n)| *n == number) {
            pending.remove(key);
        }
    }

    fn done(&self, pending_key: &(usize, String), number: u64, len: u64) {
        self.queued.fetch_sub(len, Ordering::AcqRel);
        self.remove_pending(pending_key, number);
        self.room.notify_waiters();
    }

    fn write(&self, job: WriteJob, number: u64) {
        // Takes the entry off the queue when done, also on a panic.
        struct Queued<'a> {
            shared: &'a Shared,
            key: (usize, String),
            number: u64,
            len: u64,
        }
        impl Drop for Queued<'_> {
            fn drop(&mut self) {
                self.shared.done(&self.key, self.number, self.len);
            }
        }
        let len = job.value.len() as u64;
        let _queued = Queued {
            shared: self,
            key: (job.pool.pool().index(), job.rel.clone()),
            number,
            len,
        };
        // A fill its object was invalidated since, while it waited in the queue, is rejected
        // before it evicts anything to make room, under disk pressure or in its pool. `publish`
        // checks again, for an invalidation from here on.
        if !job.pool.accepts(&job.rel, job.ticket.started()) {
            self.drops.add(&job.rel, DropReason::Rejected);
            return;
        }
        self.relieve_disk_pressure();
        // Gives the reservation back unless the entry was published, also on a panic.
        struct Held<'a> {
            pool: &'a DiskPool,
            rel: &'a str,
            bytes: u64,
            published: bool,
        }
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                if !self.published {
                    self.pool.release(self.rel, self.bytes);
                }
            }
        }
        // The file holds the value and its 4 byte crc.
        let bytes = len + 4;
        let reservation = job.pool.reserve(&job.rel, bytes);
        job.pool.delete(&reservation.victims);
        if reservation.reserved {
            let mut held = Held {
                pool: &job.pool,
                rel: &job.rel,
                bytes,
                published: false,
            };
            let path = job.pool.root().join(&job.rel);
            let tmp = tmp_path(&path, self.seq.fetch_add(1, Ordering::Relaxed));
            let published =
                write_tmp(&tmp, &job.value, self.settings.sync_data()).and_then(|written| {
                    held.published = true;
                    let commit = || std::fs::rename(&tmp, &path);
                    let started = job.ticket.started();
                    Ok(job
                        .pool
                        .publish(job.rel.clone(), bytes, written, started, commit)?)
                });
            match published {
                Ok(published) => {
                    job.pool.delete(&published.victims);
                    if !published.indexed {
                        self.drops.add(&job.rel, DropReason::Rejected);
                    }
                }
                Err(e) => {
                    self.drops.add(&job.rel, DropReason::IoError);
                    warn!("[DiskCache] failed to write {path:?}: {e}");
                }
            }
            // Left when the entry was rejected or its write failed.
            let _ = std::fs::remove_file(&tmp);
        } else {
            self.drops.add(&job.rel, DropReason::Rejected);
        }
    }

    /// Evicts from the data, index and meta pools, in this order, while the file system is
    /// over the watermark: other data share the disk, spill files among them.
    fn relieve_disk_pressure(&self) {
        let ratio = self.settings.watermark_ratio();
        for pool in &self.pools {
            if !(self.over_watermark)(&self.root, ratio) {
                return;
            }
            pool.delete(&pool.evict_bytes(WATERMARK_EVICT_BYTES));
        }
    }
}

/// Over `percent` of the file system in use.
fn fs_over_watermark(root: &Path, percent: u64) -> bool {
    match rustix::fs::statvfs(root) {
        Ok(stat) if stat.f_blocks > 0 => {
            let used = stat.f_blocks.saturating_sub(stat.f_bavail);
            used as u128 * 100 > stat.f_blocks as u128 * percent as u128
        }
        _ => false,
    }
}
