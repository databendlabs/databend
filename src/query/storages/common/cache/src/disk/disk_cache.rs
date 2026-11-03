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

//! The disk cache: raw bytes of objects in storage, in three pools (meta, index, data) that split
//! `cache.disk.max_bytes` by their ratios. It knows nothing of the values decoded from the bytes,
//! only the kind each entry belongs to, which picks its pool and labels its metrics. Queries
//! fill it on a miss; writes and maintenance do not.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use databend_common_base::runtime::spawn_blocking;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeConfig;
use databend_common_config::DiskCacheInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_access_count;
use databend_common_metrics::cache::metrics_inc_cache_hit_count;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use databend_common_metrics::cache::metrics_inc_cache_miss_count;
use log::info;
use log::warn;
use parking_lot::Mutex;

use crate::CacheKind;
use crate::DiskKey;
use crate::Pool;
use crate::SingleFlight;
use crate::disk::DiskOption;
use crate::disk::disk_file::read_entry;
pub use crate::disk::disk_pool::DiskKindEvents;
use crate::disk::disk_pool::DiskPool;
pub use crate::disk::disk_pool::DiskPoolStats;
use crate::disk::disk_writer::DiskWriter;
use crate::disk::disk_writer::DropReason;
use crate::disk::disk_writer::WriteJob;
use crate::disk::disk_writer::WriterOptions;
use crate::disk::fill_fence::FillFence;
use crate::disk::fill_fence::FillTicket;

/// `cache.disk.max_bytes` at or below this turns the disk cache off: the cloud platform sets a
/// tiny size rather than 0 to disable it without unmounting the volume.
const MIN_DISK_CACHE_BYTES: u64 = 1024;

/// Version of the directory layout, `<path>/<tenant>/v2/<pool>/<kind>/<prefix>/<hash>`. Other
/// entries under `<path>/<tenant>` are from older layouts and are removed in the background.
const LAYOUT_DIR: &str = "v2";

/// `cache.disk.writer_threads` at most.
const MAX_WRITER_THREADS: u64 = 256;

/// Per-pool figures, plus the queue and the entries not written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskCacheStats {
    pub queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_rejected: u64,
    pub dropped_io_error: u64,
}

/// What the disk cache did for one kind since it started, for its metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskKindStats {
    /// Lookups served, the bytes served, and those served from the write queue.
    pub hits: u64,
    pub hit_bytes: u64,
    pub queued_hits: u64,
    /// Time spent reading cached files, in microseconds.
    pub read_micros: u64,
    pub misses: u64,
    /// Bytes read from storage on misses of reads through the disk cache.
    pub storage_read_bytes: u64,
    /// Entries not written, by `DropReason`.
    pub dropped: [u64; 3],
    /// Written, evicted and invalidated entries.
    pub events: DiskKindEvents,
}

#[derive(Default)]
struct KindCounters {
    hits: AtomicU64,
    hit_bytes: AtomicU64,
    queued_hits: AtomicU64,
    read_micros: AtomicU64,
    misses: AtomicU64,
    storage_read_bytes: AtomicU64,
}

pub struct DiskCache {
    pools: [Arc<DiskPool>; 3],
    /// By kind index.
    counters: Box<[KindCounters]>,
    /// Capacities from the configured ratios, by pool index: what `default` restores.
    default_caps: [u64; 3],
    max_bytes: u64,
    writer: DiskWriter,
    /// The writer's settings as configured: what `default` restores.
    default_writer: WriterOptions,
    /// Off when the license does not allow the disk cache.
    allowed: AtomicBool,
    /// Per kind, by kind index: false after `set_cache_capacity('disk:<kind>', 0)`.
    kinds_enabled: Box<[AtomicBool]>,
    /// Per kind, by kind index: the pre-refactor name of its disk cache, `disk_cache_<...>`,
    /// for its row in `system.caches` and its metrics.
    names: Box<[String]>,
    /// Concurrent misses of one unit read storage once.
    flight: SingleFlight<Bytes>,
    /// Orders fills against changes of objects (`begin_change`).
    fence: Arc<FillFence>,
    /// Serializes runtime updates: `set_pool_cap`, the check of the pools' sum and the update,
    /// and `set_kind_enabled`, the flag read by `is_enabled` and the pool's.
    updates: Mutex<()>,
}

impl DiskCache {
    /// The disk cache of the node, or `None` when `cache.data_cache_storage` is not `disk`,
    /// `max_bytes` is too small, or the build is not EE.
    pub fn from_config(
        config: &CacheConfig,
        tenant_id: &str,
        ee_mode: bool,
    ) -> Result<Option<Arc<DiskCache>>> {
        let disk = &config.disk_cache_config;
        if config.data_cache_storage != CacheStorageTypeConfig::Disk
            || disk.max_bytes <= MIN_DISK_CACHE_BYTES
            || !ee_mode
        {
            info!(
                "[DiskCache] off: storage {:?}, max_bytes {}, ee mode {ee_mode}",
                config.data_cache_storage, disk.max_bytes
            );
            return Ok(None);
        }
        let caps = split_caps(disk.max_bytes, [
            disk.meta_max_ratio,
            disk.index_max_ratio,
            disk.data_max_ratio,
        ])?;
        let tenant_root = PathBuf::from(&disk.path).join(tenant_id);
        if config.data_cache_key_reload_policy == DiskCacheKeyReloadPolicy::Reset {
            // Starts empty: the files of the previous run are set aside at once, then deleted
            // with the older layouts in the background. Nothing cached before a restart is read
            // after it, whatever happened to the objects meanwhile.
            set_aside_layout(&tenant_root)?;
        }
        let writer = writer_options(disk)?;
        let cache = DiskCache::with_writer(&tenant_root, disk.max_bytes, caps, writer)?;
        info!(
            "[DiskCache] on: {} bytes, meta {} / index {} / data {}, under {:?}",
            disk.max_bytes, caps[0], caps[1], caps[2], tenant_root
        );
        let cache = Arc::new(cache);
        cache.clone().start_reload(tenant_root);
        Ok(Some(cache))
    }

    /// A disk cache under `tenant_root` with the given pool capacities, by pool index. The
    /// index starts empty; `start_reload` fills it from the files left by a previous run.
    pub fn new(
        tenant_root: &Path,
        max_bytes: u64,
        caps: [u64; 3],
        sync_data: bool,
    ) -> Result<Self> {
        Self::with_writer(tenant_root, max_bytes, caps, WriterOptions::new(sync_data))
    }

    /// `new`, with the writer started as `writer` says.
    pub fn with_writer(
        tenant_root: &Path,
        max_bytes: u64,
        caps: [u64; 3],
        writer: WriterOptions,
    ) -> Result<Self> {
        let root = tenant_root.join(LAYOUT_DIR);
        let pools = Pool::ALL.map(|pool| {
            Arc::new(DiskPool::new(
                pool,
                root.join(pool.name()),
                caps[pool.index()],
            ))
        });
        for pool in &pools {
            fs::create_dir_all(pool.root()).map_err(|e| {
                ErrorCode::StorageOther(format!(
                    "failed to create disk cache directory {:?}: {e}",
                    pool.root()
                ))
            })?;
        }
        let fenced = pools.clone();
        let pressure_order = Pool::RECLAIM_ORDER
            .iter()
            .map(|pool| pools[pool.index()].clone())
            .collect();
        Ok(DiskCache {
            writer: DiskWriter::start(pressure_order, root, writer),
            default_writer: writer,
            pools,
            default_caps: caps,
            max_bytes,
            allowed: AtomicBool::new(true),
            kinds_enabled: CacheKind::all().map(|_| AtomicBool::new(true)).collect(),
            names: CacheKind::all().map(disk_name).collect(),
            counters: CacheKind::all().map(|_| KindCounters::default()).collect(),
            flight: SingleFlight::default(),
            fence: Arc::new(FillFence::with_on_advance(Box::new(move |point| {
                for pool in &fenced {
                    pool.prune_invalidations(point);
                }
            }))),
            updates: Mutex::new(()),
        })
    }

    /// Rebuilds the pools' indexes from their directories and removes older layouts, in a
    /// background thread: queries start at once, missing the entries not indexed yet.
    pub fn start_reload(self: Arc<Self>, tenant_root: PathBuf) {
        let spawned = std::thread::Builder::new()
            .name("disk-cache-reload".to_string())
            .spawn(move || {
                remove_old_layouts(&tenant_root);
                self.reload_now();
            });
        if let Err(e) = spawned {
            warn!("[DiskCache] failed to spawn the reload thread: {e}");
        }
    }

    /// Reloads synchronously; for tests. The invalidations kept for a pool's scan are pruned
    /// once it is done, as far as no running fill needs them: fills that finished during the
    /// scan pruned nothing.
    pub fn reload_now(&self) {
        for pool in &self.pools {
            pool.reload();
            pool.prune_invalidations(self.fence.frontier());
        }
    }

    fn pool(&self, kind: CacheKind) -> &Arc<DiskPool> {
        &self.pools[kind.pool().index()]
    }

    /// `disk_cache_<...>`: the name of the kind's disk cache in `system.caches` and metrics.
    pub fn metric_name(&self, kind: CacheKind) -> &str {
        &self.names[kind.index()]
    }

    pub(crate) fn flight(&self) -> &SingleFlight<Bytes> {
        &self.flight
    }

    /// Whether entries of `kind` are read from and written to disk.
    pub fn is_enabled(&self, kind: CacheKind) -> bool {
        kind.on_disk()
            && self.allowed.load(Ordering::Relaxed)
            && self.kinds_enabled[kind.index()].load(Ordering::Relaxed)
            && self.pool(kind).cap() > 0
    }

    /// Whether an entry of `kind` of `len` bytes can be kept: the kind is on and the entry not
    /// too large for its pool.
    pub fn admits(&self, kind: CacheKind, len: u64) -> bool {
        self.is_enabled(kind) && self.pool(kind).admits(len)
    }

    /// The bytes of `key`, or `None`. A file missing or failing its checksum is dropped from
    /// the index and reported as a miss.
    pub async fn get(&self, kind: CacheKind, key: &DiskKey) -> Option<Bytes> {
        if !self.is_enabled(kind) {
            return None;
        }
        let pool = self.pool(kind);
        let name = self.metric_name(kind);
        metrics_inc_cache_access_count(1, name);
        let rel = key.rel_path(kind);
        let counters = &self.counters[kind.index()];
        let hit = |bytes: &Bytes| {
            pool.record(true);
            metrics_inc_cache_hit_count(1, name);
            counters.hits.fetch_add(1, Ordering::Relaxed);
            counters
                .hit_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        };
        let miss = || {
            pool.record(false);
            metrics_inc_cache_miss_count(1, name);
            counters.misses.fetch_add(1, Ordering::Relaxed);
        };
        let Some((path, size)) = pool.lookup(&rel) else {
            // Read before and still waiting to be written: served, as the entry it becomes,
            // unless its object changed since. Concurrent scans of the same data then read it
            // from storage once.
            // The fill's ticket is held across the check: an invalidation of the object since
            // the fill started is not pruned meanwhile, even if the job is rejected and drops
            // its own ticket.
            if let Some((bytes, ticket)) = self.writer.pending(pool.pool().index(), &rel) {
                if pool.accepts(&rel, ticket.started()) {
                    hit(&bytes);
                    counters.queued_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(bytes);
                }
            }
            miss();
            return None;
        };
        let start = Instant::now();
        let read = spawn_blocking(move || read_entry(path, size as usize)).await;
        counters
            .read_micros
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        match read {
            Ok(Ok(bytes)) => {
                hit(&bytes);
                Some(bytes)
            }
            Ok(Err(e)) => {
                warn!("[DiskCache] dropping unreadable entry of {key}: {e}");
                pool.remove(&rel);
                miss();
                None
            }
            Err(e) => {
                warn!("[DiskCache] read task failed: {e}");
                miss();
                None
            }
        }
    }

    pub fn contains(&self, kind: CacheKind, key: &DiskKey) -> bool {
        self.is_enabled(kind) && self.pool(kind).contains(&key.rel_path(kind))
    }

    /// Starts a fill: taken before the bytes to put are read from storage, so an invalidation
    /// of their object meanwhile rejects them.
    pub fn begin_fill(&self) -> FillTicket {
        self.fence.begin()
    }

    /// Queues `value`, read by the fill `ticket`, to be written under `key`. Returns false when
    /// it was dropped.
    pub async fn put(
        &self,
        kind: CacheKind,
        key: &DiskKey,
        value: Bytes,
        ticket: &FillTicket,
    ) -> bool {
        if !self.is_enabled(kind) {
            return false;
        }
        let pool = self.pool(kind).clone();
        let rel = key.rel_path(kind);
        if pool.contains(&rel) {
            return false;
        }
        let ticket = ticket.clone();
        self.writer
            .enqueue(WriteJob {
                pool,
                rel,
                value,
                ticket,
            })
            .await
    }

    /// Drops `key` from the disk cache.
    pub fn evict(&self, kind: CacheKind, key: &DiskKey) -> bool {
        self.pool(kind).remove(&key.rel_path(kind))
    }

    /// The last point the object at `path` was invalidated at, as kept by the pool of `kind`
    /// while fills started before may run; 0 if none. Reads of an object in flight across an
    /// invalidation are not shared with reads started after it.
    pub fn invalidation_of(&self, kind: CacheKind, path: &str) -> u64 {
        self.pool(kind)
            .invalidated_at(&DiskKey::object_id(path))
            .unwrap_or(0)
    }

    /// The object at `path` is about to be rewritten in place or deleted: drops every unit of it
    /// on disk, in every kind and pool, files included, and refuses its units until the
    /// returned change is dropped, once the object is written or deleted. Then the units of
    /// fills started before are rejected too, still queued or being written. Its cached bytes
    /// are so never read again, also after a crash during the change. Fails when a file of the
    /// object cannot be deleted, as on a volume remounted read-only: it would be read again
    /// after a restart, so the object must not change.
    pub fn begin_change(self: &Arc<Self>, path: &str) -> Result<ObjectChange> {
        let mut change = ObjectChange {
            cache: self.clone(),
            object: DiskKey::object_id(path),
            begun: 0,
            dropped: 0,
        };
        for pool in &self.pools {
            // Counted first: the pool's change is ended on drop, also when it fails.
            change.begun += 1;
            change.dropped += pool.begin_change(&change.object).map_err(|e| {
                ErrorCode::StorageOther(format!(
                    "[DiskCache] cannot delete the cached files of {path} before it is \
                     rewritten or deleted: {e}"
                ))
            })?;
        }
        Ok(change)
    }

    /// Runtime capacity of a pool, in bytes; 0 empties it and stops reads and writes. Files are
    /// deleted in the background. Pools together may not exceed `max_bytes`.
    pub fn set_pool_cap(&self, pool: Pool, cap: u64) -> Result<()> {
        let _updates = self.updates.lock();
        let others: u64 = Pool::ALL
            .iter()
            .filter(|p| **p != pool)
            .map(|p| self.pools[p.index()].cap())
            .sum();
        // Checked: `cap` may be any u64.
        let total = others.saturating_add(cap);
        if total > self.max_bytes {
            return Err(ErrorCode::BadArguments(format!(
                "disk cache pools would hold {total} bytes, more than cache.disk.max_bytes {}",
                self.max_bytes
            )));
        }
        let pool = &self.pools[pool.index()];
        delete_in_background(pool.clone(), pool.set_cap(cap));
        Ok(())
    }

    /// The capacity of `pool` from the configured ratios.
    pub fn default_pool_cap(&self, pool: Pool) -> u64 {
        self.default_caps[pool.index()]
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// The current value of a writer setting.
    pub fn option(&self, option: DiskOption) -> u64 {
        let settings = self.writer.settings();
        match option {
            DiskOption::QueueBytes => settings.queue_bytes(),
            DiskOption::QueueWaitMs => settings.queue_wait().as_millis() as u64,
            DiskOption::WatermarkRatio => settings.watermark_ratio(),
            DiskOption::SyncData => settings.sync_data() as u64,
        }
    }

    /// The value of a writer setting from `cache.disk`.
    pub fn default_option(&self, option: DiskOption) -> u64 {
        let writer = &self.default_writer;
        match option {
            DiskOption::QueueBytes => writer.queue_bytes,
            DiskOption::QueueWaitMs => writer.queue_wait.as_millis() as u64,
            DiskOption::WatermarkRatio => writer.watermark_ratio,
            DiskOption::SyncData => writer.sync_data as u64,
        }
    }

    /// Changes a writer setting, for the entries queued from now on.
    pub fn set_option(&self, option: DiskOption, value: u64) -> Result<()> {
        option.check(value)?;
        match option {
            DiskOption::QueueBytes => self.writer.set_queue_bytes(value),
            DiskOption::QueueWaitMs => self.writer.set_queue_wait(Duration::from_millis(value)),
            DiskOption::WatermarkRatio => self.writer.set_watermark_ratio(value),
            DiskOption::SyncData => self.writer.set_sync_data(value == 1),
        }
        Ok(())
    }

    /// Turns the disk cache of `kind` on or off. Turning it off drops the kind's entries, and
    /// its pool refuses entries of it queued or scanned before.
    pub fn set_kind_enabled(&self, kind: CacheKind, enabled: bool) {
        let _updates = self.updates.lock();
        self.kinds_enabled[kind.index()].store(enabled, Ordering::Relaxed);
        let pool = self.pool(kind);
        delete_in_background(pool.clone(), pool.set_kind_enabled(kind, enabled));
    }

    pub fn is_kind_enabled(&self, kind: CacheKind) -> bool {
        self.kinds_enabled[kind.index()].load(Ordering::Relaxed)
    }

    /// Turns the whole disk cache on or off at runtime, by license.
    pub fn set_allowed(&self, allowed: bool) {
        self.allowed.store(allowed, Ordering::Relaxed);
    }

    pub fn is_allowed(&self) -> bool {
        self.allowed.load(Ordering::Relaxed)
    }

    pub fn pool_stats(&self) -> Vec<(Pool, DiskPoolStats)> {
        self.pools.iter().map(|p| (p.pool(), p.stats())).collect()
    }

    /// Items and bytes of `kind` on disk.
    pub fn kind_usage(&self, kind: CacheKind) -> (u64, u64) {
        self.pool(kind).kind_usage(kind)
    }

    /// Counts `bytes` read from storage for `kind` on a miss of a read through the disk cache.
    pub(crate) fn record_storage_read(&self, kind: CacheKind, bytes: u64) {
        metrics_inc_cache_miss_bytes(bytes, self.metric_name(kind));
        self.counters[kind.index()]
            .storage_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// See `DiskKindStats`.
    pub fn kind_stats(&self, kind: CacheKind) -> DiskKindStats {
        let c = &self.counters[kind.index()];
        let drops = self.writer.drops();
        DiskKindStats {
            hits: c.hits.load(Ordering::Relaxed),
            hit_bytes: c.hit_bytes.load(Ordering::Relaxed),
            queued_hits: c.queued_hits.load(Ordering::Relaxed),
            read_micros: c.read_micros.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            storage_read_bytes: c.storage_read_bytes.load(Ordering::Relaxed),
            dropped: DropReason::ALL.map(|reason| drops.of(kind, reason)),
            events: self.pool(kind).kind_events(kind),
        }
    }

    pub fn stats(&self) -> DiskCacheStats {
        let drops = self.writer.drops();
        DiskCacheStats {
            queued_bytes: self.writer.queued_bytes(),
            dropped_queue_full: drops.total(DropReason::QueueFull),
            dropped_rejected: drops.total(DropReason::Rejected),
            dropped_io_error: drops.total(DropReason::IoError),
        }
    }
}

/// An object being rewritten or deleted, from `DiskCache::begin_change` until dropped: when the
/// change is done, or abandoned by an error or a cancelled caller.
#[must_use]
pub struct ObjectChange {
    cache: Arc<DiskCache>,
    object: String,
    /// The pools the change was begun in, in order.
    begun: usize,
    dropped: usize,
}

impl ObjectChange {
    /// The entries dropped when the change began.
    pub fn dropped(&self) -> usize {
        self.dropped
    }
}

impl Drop for ObjectChange {
    fn drop(&mut self) {
        let (at, oldest_fill) = self.cache.fence.invalidation_point();
        for pool in self.cache.pools.iter().take(self.begun) {
            pool.delete(&pool.end_change(&self.object, at, oldest_fill));
        }
    }
}

/// The writer as `cache.disk` configures it, checked.
fn writer_options(disk: &DiskCacheInnerConfig) -> Result<WriterOptions> {
    let invalid = |e: ErrorCode| ErrorCode::InvalidConfig(e.message());
    for (option, value) in [
        (DiskOption::QueueBytes, disk.queue_bytes),
        (DiskOption::QueueWaitMs, disk.queue_wait_ms),
        (DiskOption::WatermarkRatio, disk.watermark_ratio),
    ] {
        option.check(value).map_err(invalid)?;
    }
    if !(1..=MAX_WRITER_THREADS).contains(&disk.writer_threads) {
        return Err(ErrorCode::InvalidConfig(format!(
            "cache.disk.writer_threads must be 1 to {MAX_WRITER_THREADS}, got {}",
            disk.writer_threads
        )));
    }
    Ok(WriterOptions {
        writers: disk.writer_threads as usize,
        queue_bytes: disk.queue_bytes,
        queue_wait: Duration::from_millis(disk.queue_wait_ms),
        watermark_ratio: disk.watermark_ratio,
        ..WriterOptions::new(disk.sync_data)
    })
}

/// `disk_cache_<...>`, the names of the pre-refactor disk caches.
fn disk_name(kind: CacheKind) -> String {
    format!("disk_{}", kind.name().trim_start_matches("memory_"))
}

/// Splits `max_bytes` among the pools in proportion to `ratios` (meta, index, data). Their sum
/// need not be 100; 0 turns a pool off.
pub fn split_caps(max_bytes: u64, ratios: [u64; 3]) -> Result<[u64; 3]> {
    // Summed wide: ratios are any u64. Each share is at most `max_bytes`.
    let total: u128 = ratios.iter().map(|r| *r as u128).sum();
    if total == 0 {
        return Err(ErrorCode::InvalidConfig(
            "cache.disk meta_max_ratio, index_max_ratio and data_max_ratio are all 0",
        ));
    }
    Ok(ratios.map(|r| (max_bytes as u128 * r as u128 / total) as u64))
}

fn delete_in_background(pool: Arc<DiskPool>, victims: Vec<String>) {
    if victims.is_empty() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("disk-cache-delete".to_string())
        .spawn(move || pool.delete(&victims));
    if let Err(e) = spawned {
        warn!("[DiskCache] failed to spawn the delete thread: {e}");
    }
}

/// Renames the current layout away, for `remove_old_layouts` to delete it.
fn set_aside_layout(tenant_root: &Path) -> Result<()> {
    let layout = tenant_root.join(LAYOUT_DIR);
    let aside = tenant_root.join(format!("{LAYOUT_DIR}.reset"));
    if aside.exists() {
        // Left by a reset that did not get to delete it.
        fs::remove_dir_all(&aside).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to remove disk cache {aside:?}: {e}"))
        })?;
    }
    match fs::rename(&layout, &aside) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(ErrorCode::StorageOther(format!(
            "failed to reset disk cache {layout:?}: {e}"
        ))),
    }
}

/// Removes what earlier layouts left under the tenant directory: everything but `v2`.
fn remove_old_layouts(tenant_root: &Path) {
    let Ok(entries) = fs::read_dir(tenant_root) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        if entry.file_name() == LAYOUT_DIR {
            continue;
        }
        let path = entry.path();
        let removed = if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        match removed {
            Ok(()) => info!("[DiskCache] removed old cache layout {path:?}"),
            Err(e) => warn!("[DiskCache] failed to remove old cache layout {path:?}: {e}"),
        }
    }
}
