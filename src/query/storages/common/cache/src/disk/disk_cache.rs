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
use std::sync::atomic::Ordering;

use bytes::Bytes;
use databend_common_base::runtime::spawn_blocking;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_access_count;
use databend_common_metrics::cache::metrics_inc_cache_hit_count;
use databend_common_metrics::cache::metrics_inc_cache_miss_count;
use log::info;
use log::warn;

use crate::CacheKind;
use crate::DiskKey;
use crate::Pool;
use crate::SingleFlight;
use crate::disk::disk_file::read_entry;
use crate::disk::disk_pool::DiskPool;
pub use crate::disk::disk_pool::DiskPoolStats;
use crate::disk::disk_writer::DiskWriter;
use crate::disk::disk_writer::WriteJob;
use crate::disk::disk_writer::remove_files;

/// `cache.disk.max_bytes` at or below this turns the disk cache off: the cloud platform sets a
/// tiny size rather than 0 to disable it without unmounting the volume.
const MIN_DISK_CACHE_BYTES: u64 = 1024;

/// Version of the directory layout, `<path>/<tenant>/v2/<pool>/<kind>/<prefix>/<hash>`. Other
/// entries under `<path>/<tenant>` are from older layouts and are removed in the background.
const LAYOUT_DIR: &str = "v2";

/// Per-pool figures, plus the queue and the entries not written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskCacheStats {
    pub queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_rejected: u64,
    pub dropped_io_error: u64,
}

pub struct DiskCache {
    pools: [Arc<DiskPool>; 3],
    /// Capacities from the configured ratios, by pool index: what `default` restores.
    default_caps: [u64; 3],
    max_bytes: u64,
    writer: DiskWriter,
    /// Off when the license does not allow the disk cache.
    allowed: AtomicBool,
    /// Per kind, by kind index: false after `set_cache_capacity('disk:<kind>', 0)`.
    kinds_enabled: Box<[AtomicBool]>,
    /// Per kind, by kind index: the pre-refactor name of its disk cache, `disk_cache_<...>`,
    /// for its row in `system.caches` and its metrics.
    names: Box<[String]>,
    /// Concurrent misses of one unit read storage once.
    flight: SingleFlight<Bytes>,
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
        let cache = DiskCache::new(&tenant_root, disk.max_bytes, caps, disk.sync_data)?;
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
        let pressure_order = Pool::RECLAIM_ORDER
            .iter()
            .map(|pool| pools[pool.index()].clone())
            .collect();
        Ok(DiskCache {
            writer: DiskWriter::start(pressure_order, root, sync_data),
            pools,
            default_caps: caps,
            max_bytes,
            allowed: AtomicBool::new(true),
            kinds_enabled: CacheKind::all().map(|_| AtomicBool::new(true)).collect(),
            names: CacheKind::all().map(disk_name).collect(),
            flight: SingleFlight::default(),
        })
    }

    /// Rebuilds the pools' indexes from their directories and removes older layouts, in a
    /// background thread: queries start at once, missing the entries not indexed yet.
    pub fn start_reload(self: Arc<Self>, tenant_root: PathBuf) {
        let spawned = std::thread::Builder::new()
            .name("disk-cache-reload".to_string())
            .spawn(move || {
                remove_old_layouts(&tenant_root);
                for pool in &self.pools {
                    pool.reload();
                }
            });
        if let Err(e) = spawned {
            warn!("[DiskCache] failed to spawn the reload thread: {e}");
        }
    }

    /// Reloads synchronously; for tests.
    pub fn reload_now(&self) {
        for pool in &self.pools {
            pool.reload();
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
        let Some((path, size)) = pool.lookup(&rel) else {
            pool.record(false);
            metrics_inc_cache_miss_count(1, name);
            return None;
        };
        let read = spawn_blocking(move || read_entry(path, size as usize)).await;
        match read {
            Ok(Ok(bytes)) => {
                pool.record(true);
                metrics_inc_cache_hit_count(1, name);
                Some(bytes)
            }
            Ok(Err(e)) => {
                warn!("[DiskCache] dropping unreadable entry of {key}: {e}");
                if let Some(file) = pool.remove(&rel) {
                    remove_files(&[file]);
                }
                pool.record(false);
                metrics_inc_cache_miss_count(1, name);
                None
            }
            Err(e) => {
                warn!("[DiskCache] read task failed: {e}");
                pool.record(false);
                metrics_inc_cache_miss_count(1, name);
                None
            }
        }
    }

    pub fn contains(&self, kind: CacheKind, key: &DiskKey) -> bool {
        self.is_enabled(kind) && self.pool(kind).contains(&key.rel_path(kind))
    }

    /// Queues `value` to be written under `key`. Returns false when it was dropped.
    pub async fn put(&self, kind: CacheKind, key: &DiskKey, value: Bytes) -> bool {
        if !self.is_enabled(kind) {
            return false;
        }
        let pool = self.pool(kind).clone();
        let rel = key.rel_path(kind);
        if pool.contains(&rel) {
            return false;
        }
        self.writer.enqueue(WriteJob { pool, rel, value }).await
    }

    /// Drops `key` from the disk cache.
    pub fn evict(&self, kind: CacheKind, key: &DiskKey) -> bool {
        match self.pool(kind).remove(&key.rel_path(kind)) {
            Some(file) => {
                remove_files(&[file]);
                true
            }
            None => false,
        }
    }

    /// Runtime capacity of a pool, in bytes; 0 empties it and stops reads and writes. Files are
    /// deleted in the background. Pools together may not exceed `max_bytes`.
    pub fn set_pool_cap(&self, pool: Pool, cap: u64) -> Result<()> {
        let others: u64 = Pool::ALL
            .iter()
            .filter(|p| **p != pool)
            .map(|p| self.pools[p.index()].cap())
            .sum();
        if others + cap > self.max_bytes {
            return Err(ErrorCode::BadArguments(format!(
                "disk cache pools would hold {} bytes, more than cache.disk.max_bytes {}",
                others + cap,
                self.max_bytes
            )));
        }
        let victims = self.pools[pool.index()].set_cap(cap);
        delete_in_background(victims);
        Ok(())
    }

    /// The capacity of `pool` from the configured ratios.
    pub fn default_pool_cap(&self, pool: Pool) -> u64 {
        self.default_caps[pool.index()]
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Turns the disk cache of `kind` on or off. Turning it off drops the kind's entries.
    pub fn set_kind_enabled(&self, kind: CacheKind, enabled: bool) {
        self.kinds_enabled[kind.index()].store(enabled, Ordering::Relaxed);
        if !enabled {
            delete_in_background(self.pool(kind).remove_kind(kind));
        }
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

    pub fn stats(&self) -> DiskCacheStats {
        let drops = self.writer.drops();
        DiskCacheStats {
            queued_bytes: self.writer.queued_bytes(),
            dropped_queue_full: drops.queue_full.load(Ordering::Relaxed),
            dropped_rejected: drops.rejected.load(Ordering::Relaxed),
            dropped_io_error: drops.io_error.load(Ordering::Relaxed),
        }
    }
}

/// `disk_cache_<...>`, the names of the pre-refactor disk caches.
fn disk_name(kind: CacheKind) -> String {
    format!("disk_{}", kind.name().trim_start_matches("memory_"))
}

/// Splits `max_bytes` among the pools in proportion to `ratios` (meta, index, data). Their sum
/// need not be 100; 0 turns a pool off.
pub fn split_caps(max_bytes: u64, ratios: [u64; 3]) -> Result<[u64; 3]> {
    let total: u64 = ratios.iter().sum();
    if total == 0 {
        return Err(ErrorCode::InvalidConfig(
            "cache.disk meta_max_ratio, index_max_ratio and data_max_ratio are all 0",
        ));
    }
    Ok(ratios.map(|r| (max_bytes as u128 * r as u128 / total as u128) as u64))
}

fn delete_in_background(files: Vec<PathBuf>) {
    if files.is_empty() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("disk-cache-delete".to_string())
        .spawn(move || remove_files(&files));
    if let Err(e) = spawned {
        warn!("[DiskCache] failed to spawn the delete thread: {e}");
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
