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
//! temporary name and rename it into place, then publish it.

use std::collections::HashSet;
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

use crate::disk::disk_file::tmp_path;
use crate::disk::disk_file::write_entry;
use crate::disk::disk_pool::DiskPool;

/// Bytes evicted per pool per pass when the file system is over the watermark.
const WATERMARK_EVICT_BYTES: u64 = 64 * 1024 * 1024;

/// How the writer runs. `DiskCache` uses `WriterOptions::new`; tests shrink the queue and
/// replace the file system check.
pub struct WriterOptions {
    pub writers: usize,
    /// Bytes queued for writing, at most.
    pub queue_bytes: u64,
    /// How long a reader waits for room in the queue before dropping its entry.
    pub queue_wait: Duration,
    pub sync_data: bool,
    /// Whether the file system under the root is too full to write without evicting first.
    pub over_watermark: fn(&Path) -> bool,
}

impl WriterOptions {
    /// 4 writer threads, a 256 MiB queue, 50 ms of wait, and evictions above 90% of the file
    /// system in use.
    pub fn new(sync_data: bool) -> Self {
        WriterOptions {
            writers: 4,
            queue_bytes: 256 * 1024 * 1024,
            queue_wait: Duration::from_millis(50),
            sync_data,
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
#[derive(Default)]
pub struct DropCounters {
    pub queue_full: AtomicU64,
    pub rejected: AtomicU64,
    pub io_error: AtomicU64,
}

struct Shared {
    queued: AtomicU64,
    room: Notify,
    /// Entries queued or being written, by pool and key: a second miss of the same key while
    /// the first is pending is not queued again.
    pending: Mutex<HashSet<(usize, String)>>,
    drops: DropCounters,
    options: WriterOptions,
    /// Pools in eviction order under disk pressure: data, index, meta.
    pools: Vec<Arc<DiskPool>>,
    root: PathBuf,
    seq: AtomicU64,
}

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
            pending: Mutex::new(HashSet::new()),
            drops: DropCounters::default(),
            options,
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

    /// Bytes queued or being written.
    pub fn queued_bytes(&self) -> u64 {
        self.shared.queued.load(Ordering::Relaxed)
    }

    /// Entries queued or being written.
    pub fn pending_entries(&self) -> usize {
        self.shared.pending.lock().len()
    }

    /// Queues `job`, waiting up to `queue_wait` for room. Returns false when dropped, or when
    /// the same entry is already queued or being written.
    pub async fn enqueue(&self, job: WriteJob) -> bool {
        let pending_key = (job.pool.pool().index(), job.rel.clone());
        if !self.shared.pending.lock().insert(pending_key.clone()) {
            return false;
        }
        let len = job.value.len() as u64;
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
                self.shared.pending.lock().remove(&pending_key);
                self.shared.drops.queue_full.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            let _ = tokio::time::timeout(deadline - now, room).await;
        }
        if let Err(e) = self.tx.send(job) {
            // No writer left: undo the accounting.
            self.shared.done(&pending_key, len);
            warn!("[DiskCache] writer threads are gone: {e}");
            return false;
        }
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
        let len = job.value.len() as u64;
        let _queued = Queued {
            shared: self,
            key: (job.pool.pool().index(), job.rel.clone()),
            len,
        };
        self.relieve_disk_pressure();
        // Gives the reservation back unless the entry was published, also on a panic.
        struct Held<'a> {
            pool: &'a DiskPool,
            bytes: u64,
            published: bool,
        }
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                if !self.published {
                    self.pool.release(self.bytes);
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
                bytes,
                published: false,
            };
            let path = job.pool.root().join(&job.rel);
            let tmp = tmp_path(&path, self.seq.fetch_add(1, Ordering::Relaxed));
            match write_entry(&path, &tmp, &job.value, self.options.sync_data) {
                Ok(written) => {
                    job.pool.publish(job.rel.clone(), bytes, written);
                    held.published = true;
                }
                Err(e) => {
                    self.drops.io_error.fetch_add(1, Ordering::Relaxed);
                    warn!("[DiskCache] failed to write {path:?}: {e}");
                }
            }
        } else {
            self.drops.rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Evicts from the data, index and meta pools, in this order, while the file system is
    /// over the watermark: other data share the disk, spill files among them.
    fn relieve_disk_pressure(&self) {
        for pool in &self.pools {
            if !(self.options.over_watermark)(&self.root) {
                return;
            }
            pool.delete(&pool.evict_bytes(WATERMARK_EVICT_BYTES));
        }
    }
}

/// Over 90% of the file system in use.
fn fs_over_watermark(root: &Path) -> bool {
    match rustix::fs::statvfs(root) {
        Ok(stat) if stat.f_blocks > 0 => {
            let used = stat.f_blocks.saturating_sub(stat.f_bavail);
            used as f64 / stat.f_blocks as f64 > 0.9
        }
        _ => false,
    }
}
