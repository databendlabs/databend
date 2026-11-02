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
use log::warn;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::disk::disk_file::tmp_path;
use crate::disk::disk_file::write_entry;
use crate::disk::disk_pool::DiskPool;

/// Writer threads.
pub(crate) const WRITERS: usize = 4;
/// Bytes queued for writing, at most.
pub(crate) const QUEUE_BYTES: u64 = 256 * 1024 * 1024;
/// How long a reader waits for room in the queue before dropping its entry.
pub(crate) const QUEUE_WAIT: Duration = Duration::from_millis(50);
/// Above this share of the file system in use, writers evict before writing.
pub(crate) const DISK_WATERMARK: f64 = 0.9;
/// Bytes evicted per pool per pass when the file system is over the watermark.
const WATERMARK_EVICT_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) struct WriteJob {
    pub pool: Arc<DiskPool>,
    /// Index key and path under the pool root.
    pub rel: String,
    pub value: Bytes,
}

/// Why an entry was not written, for `cache_disk_dropped`.
#[derive(Default)]
pub(crate) struct DropCounters {
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
    sync_data: bool,
    /// Pools in eviction order under disk pressure: data, index, meta.
    pools: Vec<Arc<DiskPool>>,
    root: PathBuf,
    seq: AtomicU64,
}

pub(crate) struct DiskWriter {
    tx: crossbeam_channel::Sender<WriteJob>,
    shared: Arc<Shared>,
}

impl DiskWriter {
    pub(crate) fn start(pools: Vec<Arc<DiskPool>>, root: PathBuf, sync_data: bool) -> DiskWriter {
        let (tx, rx) = crossbeam_channel::unbounded::<WriteJob>();
        let shared = Arc::new(Shared {
            queued: AtomicU64::new(0),
            room: Notify::new(),
            pending: Mutex::new(HashSet::new()),
            drops: DropCounters::default(),
            sync_data,
            pools,
            root,
            seq: AtomicU64::new(0),
        });
        for i in 0..WRITERS {
            let rx = rx.clone();
            let shared = shared.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("disk-cache-writer-{i}"))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        shared.write(job);
                    }
                });
            if let Err(e) = spawned {
                warn!("[DiskCache] failed to spawn writer thread {i}: {e}");
            }
        }
        DiskWriter { tx, shared }
    }

    pub(crate) fn drops(&self) -> &DropCounters {
        &self.shared.drops
    }

    pub(crate) fn queued_bytes(&self) -> u64 {
        self.shared.queued.load(Ordering::Relaxed)
    }

    /// Queues `job`, waiting up to `QUEUE_WAIT` for room. Returns false when dropped.
    pub(crate) async fn enqueue(&self, job: WriteJob) -> bool {
        let pending_key = (job.pool.pool().index(), job.rel.clone());
        if !self.shared.pending.lock().insert(pending_key.clone()) {
            return false;
        }
        let len = job.value.len() as u64;
        let deadline = Instant::now() + QUEUE_WAIT;
        loop {
            let room = self.shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            let queued = self.shared.queued.load(Ordering::Acquire);
            // An entry larger than the whole queue still goes in when the queue is empty.
            if queued == 0 || queued + len <= QUEUE_BYTES {
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
        let len = job.value.len() as u64;
        let pending_key = (job.pool.pool().index(), job.rel.clone());
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
        let reservation = job.pool.reserve(bytes);
        remove_files(&reservation.victims);
        if reservation.reserved {
            let mut held = Held {
                pool: &job.pool,
                bytes,
                published: false,
            };
            let path = job.pool.root().join(&job.rel);
            let tmp = tmp_path(&path, self.seq.fetch_add(1, Ordering::Relaxed));
            match write_entry(&path, &tmp, &job.value, self.sync_data) {
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
        self.done(&pending_key, len);
    }

    /// Evicts from the data, index and meta pools, in this order, while the file system is
    /// over `DISK_WATERMARK`: other data share the disk, spill files among them.
    fn relieve_disk_pressure(&self) {
        for pool in &self.pools {
            if !over_watermark(&self.root) {
                return;
            }
            remove_files(&pool.evict_bytes(WATERMARK_EVICT_BYTES));
        }
    }
}

pub(crate) fn remove_files(paths: &[PathBuf]) {
    for path in paths {
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("[DiskCache] failed to remove {path:?}: {e}");
            }
        }
    }
}

fn over_watermark(root: &Path) -> bool {
    match rustix::fs::statvfs(root) {
        Ok(stat) if stat.f_blocks > 0 => {
            let used = stat.f_blocks.saturating_sub(stat.f_bavail);
            used as f64 / stat.f_blocks as f64 > DISK_WATERMARK
        }
        _ => false,
    }
}
