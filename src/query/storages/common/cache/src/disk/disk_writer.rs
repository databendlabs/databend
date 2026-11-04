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
//! entry. It may reject it then, when the pool shrank or the kind was turned off, and the
//! temporary file is deleted.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
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

/// Bytes evicted per pool per pass when the file system is over the watermark.
const WATERMARK_EVICT_BYTES: u64 = 64 * 1024 * 1024;

/// How the writer starts, from `cache.disk`; tests shrink the queue and replace the file
/// system check.
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

pub struct WriteJob {
    pub pool: Arc<DiskPool>,
    /// Index key and path under the pool root.
    pub rel: String,
    pub value: Bytes,
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
    /// Entries queued or being written, by pool and key, with their bytes: a second miss of the
    /// same key while the first is pending is not queued again, and is served these bytes (see
    /// `DiskWriter::pending`).
    pending: Mutex<PendingSet>,
    drops: DropCounters,
    options: WriterOptions,
    over_watermark: fn(&Path, u64) -> bool,
    /// Pools in eviction order under disk pressure: data, index, meta.
    pools: Vec<Arc<DiskPool>>,
    root: PathBuf,
    seq: AtomicU64,
}

/// Entries queued or being written, by pool index and key, with their bytes.
type PendingSet = HashMap<(usize, String), Bytes>;

pub struct DiskWriter {
    tx: crossbeam_channel::Sender<WriteJob>,
    shared: Arc<Shared>,
}

impl DiskWriter {
    /// Writers over `pools`, in eviction order under disk pressure, all under `root`.
    pub fn start(pools: Vec<Arc<DiskPool>>, root: PathBuf, options: WriterOptions) -> DiskWriter {
        let (tx, rx) = crossbeam_channel::unbounded::<WriteJob>();
        let writers = options.writers;
        let shared = Arc::new(Shared {
            queued: AtomicU64::new(0),
            room: Notify::new(),
            pending: Mutex::new(HashMap::new()),
            drops: DropCounters::default(),
            options,
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
                    while let Ok(job) = rx.recv() {
                        // A panic loses the entry, not the thread; `write` keeps the
                        // accounting right on unwind.
                        if let Err(e) = catch_unwind(|| shared.write(job)) {
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

    pub fn options(&self) -> &WriterOptions {
        &self.shared.options
    }

    /// Bytes queued or being written.
    pub fn queued_bytes(&self) -> u64 {
        self.shared.queued.load(Ordering::Relaxed)
    }

    /// The bytes queued or being written under `rel` of the pool with index `pool`.
    pub fn pending(&self, pool: usize, rel: &str) -> Option<Bytes> {
        self.shared
            .pending
            .lock()
            .get(&(pool, rel.to_string()))
            .cloned()
    }

    /// Entries queued or being written.
    pub fn pending_entries(&self) -> usize {
        self.shared.pending.lock().len()
    }

    /// Queues `job`, waiting up to `queue_wait` for room. Returns false when dropped, or when
    /// the same entry is already queued or being written.
    pub async fn enqueue(&self, job: WriteJob) -> bool {
        let WriteJob { pool, rel, value } = job;
        let len = value.len() as u64;
        self.enqueue_with(pool, rel, len, move || value).await
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
        value: impl FnOnce() -> Bytes,
    ) -> bool {
        let key = (pool.pool().index(), rel);
        if self.shared.pending.lock().contains_key(&key) {
            return false;
        }
        let options = &self.shared.options;
        let deadline = Instant::now() + options.queue_wait;
        loop {
            let room = self.shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            let queued = self.shared.queued.load(Ordering::Acquire);
            // An entry larger than the whole queue still goes in when the queue is empty.
            if queued == 0 || queued + len <= options.queue_bytes {
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
        // Held from the room taken to the hand-over, and given back unless the job reaches a
        // writer: also when making the bytes or listing the entry unwinds, as on a query
        // over its memory limit. No await in between: a cancelled caller holds no room.
        struct Held<'a> {
            shared: &'a Shared,
            key: &'a (usize, String),
            len: u64,
            listed: bool,
            handed_over: bool,
        }
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                if self.handed_over {
                    return;
                }
                if self.listed {
                    self.shared.pending.lock().remove(self.key);
                }
                self.shared.queued.fetch_sub(self.len, Ordering::AcqRel);
                self.shared.room.notify_waiters();
            }
        }
        let mut held = Held {
            shared: &self.shared,
            key: &key,
            len,
            listed: false,
            handed_over: false,
        };
        let value = value();
        debug_assert_eq!(value.len() as u64, len);
        {
            let mut pending = self.shared.pending.lock();
            // Queued by another caller while this one waited for room, or written already: a
            // writer indexes its entry before it takes the key off the pending set, so under
            // this lock a key not pending is indexed if it was written.
            if pending.contains_key(&key) || pool.contains(&key.1) {
                return false;
            }
            pending.insert(key.clone(), value.clone());
            held.listed = true;
        }
        let job = WriteJob {
            pool,
            rel: key.1.clone(),
            value,
        };
        if let Err(e) = self.tx.send(job) {
            // No writer left: the guard gives the room back and takes the key off.
            warn!("[DiskCache] writer threads are gone: {e}");
            return false;
        }
        held.handed_over = true;
        // The writer takes the key off when done.
        true
    }
}

impl Shared {
    fn done(&self, pending_key: &(usize, String), len: u64) {
        self.queued.fetch_sub(len, Ordering::AcqRel);
        self.pending.lock().remove(pending_key);
        self.room.notify_waiters();
    }

    fn write(&self, job: WriteJob) {
        // Takes the entry off the queue when done, also on a panic.
        struct Queued<'a> {
            shared: &'a Shared,
            key: (usize, String),
            len: u64,
        }
        impl Drop for Queued<'_> {
            fn drop(&mut self) {
                self.shared.done(&self.key, self.len);
            }
        }
        // The key moves into the guard before anything allocates: an allocation refused
        // under the memory limit unwinds with the entry still taken off the queue.
        let WriteJob { pool, rel, value } = job;
        let len = value.len() as u64;
        let queued = Queued {
            shared: self,
            key: (pool.pool().index(), rel),
            len,
        };
        let rel = queued.key.1.as_str();
        // The file holds the value and its 4 byte crc.
        let bytes = len + 4;
        // An entry `reserve` would refuse (too large, its kind off, no room beside the writes
        // in flight, its key indexed meanwhile) is refused before it evicts entries under disk
        // pressure.
        if !pool.would_reserve(rel, bytes) {
            self.drops.add(rel, DropReason::Rejected);
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
        let reservation = pool.reserve(rel, bytes);
        pool.delete(&reservation.victims);
        if reservation.reserved {
            let mut held = Held {
                pool: &pool,
                rel,
                bytes,
                published: false,
            };
            let path = pool.root().join(rel);
            let tmp = tmp_path(&path, self.seq.fetch_add(1, Ordering::Relaxed));
            let published =
                write_tmp(&tmp, &value, self.options.sync_data).and_then(|written| {
                    held.published = true;
                    let commit = || std::fs::rename(&tmp, &path);
                    Ok(pool.publish(rel.to_string(), bytes, written, commit)?)
                });
            match published {
                Ok(published) => {
                    pool.delete(&published.victims);
                    if !published.indexed {
                        self.drops.add(rel, DropReason::Rejected);
                    }
                }
                Err(e) => {
                    self.drops.add(rel, DropReason::IoError);
                    warn!("[DiskCache] failed to write {path:?}: {e}");
                }
            }
            // Left when the entry was rejected or its write failed.
            let _ = std::fs::remove_file(&tmp);
        } else {
            self.drops.add(rel, DropReason::Rejected);
        }
    }

    /// Evicts from the data, index and meta pools, in this order, while the file system is
    /// over the watermark: other data share the disk, spill files among them.
    fn relieve_disk_pressure(&self) {
        let ratio = self.options.watermark_ratio;
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
