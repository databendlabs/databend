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

use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::CacheLookupCounters;
use databend_common_metrics::cache::get_cache_access_count;
use databend_common_metrics::cache::get_cache_hit_count;
use databend_common_metrics::cache::get_cache_miss_count;
use log::info;
use log::warn;

use crate::CacheDef;
use crate::CacheKind;
use crate::CacheRef;
use crate::CachedItem;
use crate::DiskCache;
use crate::KindLimit;
use crate::MemCache;
use crate::Pool;
use crate::SingleFlight;
use crate::Unit;
use crate::backend::KindEvents;
use crate::backend::KindStore;
use crate::backend::ReclaimStats;
use crate::backend::ShrinkReport;
use crate::legacy::LegacyKinds;
use crate::legacy::LegacyStore;
use crate::metrics::CacheMetricsCollector;
use crate::shared::SharedStore;

const POOL_PREFIX: &str = "pool:";
const DISK_POOL_PREFIX: &str = "disk_pool:";
const DISK_KIND_PREFIX: &str = "disk:";
const DEFAULT_CAPACITY: &str = "default";

/// One row of `system.caches`: a kind in memory or on disk, or a memory or disk pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRow {
    pub name: String,
    pub kind: Option<CacheKind>,
    pub pool: Option<Pool>,
    /// A row of the disk cache: a kind's bytes on disk, or a disk pool.
    pub disk: bool,
    pub enabled: bool,
    pub num_items: u64,
    /// Bytes still alive, including values evicted but held by readers.
    pub size: u64,
    pub in_cache_size: u64,
    /// `None`: bounded only by the pool.
    pub capacity: Option<u64>,
    pub unit: Unit,
    pub access: u64,
    pub hit: u64,
    pub miss: u64,
    /// Evictions and refusals of a kind's memory tier, by a backend that counts them.
    pub events: Option<KindEvents>,
}

/// The process-wide caches: the memory backend chosen by `cache.shared_memory_enabled`, and the
/// disk cache below the readers. Hands out a typed memory cache per registered kind.
pub struct CacheManager {
    store: Arc<dyn KindStore>,
    disk: Option<Arc<DiskCache>>,
    /// In-flight loads of each kind sharing loads, by kind index: every handle of a kind shares
    /// them, so concurrent misses on one key load once even through handles made per call.
    flights: Box<[OnceLock<Arc<dyn Any + Send + Sync>>]>,
    /// Lookup counters of each kind, by kind index, shared by its handles: callers that make a
    /// handle per lookup do not look the counters up each time.
    counters: Box<[OnceLock<Arc<CacheLookupCounters>>]>,
}

impl CacheManager {
    pub fn init(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<()> {
        let manager = Self::try_new(config, max_server_memory_usage, tenant_id, ee_mode)?;
        GlobalInstance::set(Arc::new(manager));
        CacheMetricsCollector::register_global();
        Ok(())
    }

    pub fn instance() -> Arc<CacheManager> {
        GlobalInstance::get()
    }

    /// The disk cache exists only in EE mode, and can then be turned off at runtime by
    /// `set_disk_allowed`.
    pub fn try_new(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<Self> {
        let ignored = ignored_disk_options(config);
        if !ignored.is_empty() {
            info!(
                "[CacheManager] The disk cache no longer reads: {}",
                ignored.join(", ")
            );
        }
        let disk = DiskCache::from_config(config, &tenant_id.into(), ee_mode)?;
        let store: Arc<dyn KindStore> = if config.shared_memory_enabled {
            let ignored = LegacyKinds::global().ignored_options(config, *max_server_memory_usage);
            if !ignored.is_empty() {
                warn!(
                    "[CacheManager] The shared cache backend ignores: {}",
                    ignored.join(", ")
                );
            }
            Arc::new(SharedStore::from_config(config, *max_server_memory_usage)?)
        } else {
            Arc::new(LegacyStore::from_config(config, *max_server_memory_usage))
        };
        Ok(CacheManager::new(store, disk))
    }

    pub fn new(store: Arc<dyn KindStore>, disk: Option<Arc<DiskCache>>) -> Self {
        CacheManager {
            store,
            disk,
            flights: CacheKind::all().map(|_| OnceLock::new()).collect(),
            counters: CacheKind::all().map(|_| OnceLock::new()).collect(),
        }
    }

    /// The memory cache of kind `D`.
    pub fn cache<D: CacheDef>(&self) -> MemCache<D::Value> {
        let kind = CacheKind::of::<D>();
        let flight = D::SHARE_LOADS.then(|| self.flight::<D::Value>(kind));
        let counters = self.counters[kind.index()]
            .get_or_init(|| Arc::new(CacheLookupCounters::new(kind.name())))
            .clone();
        MemCache::with_shared(self.store.clone(), kind, counters, flight)
    }

    /// The disk cache of the node, if `cache.data_cache_storage` is `disk` and the build is EE.
    pub fn disk(&self) -> Option<Arc<DiskCache>> {
        self.disk.clone()
    }

    fn flight<T: CachedItem>(&self, kind: CacheKind) -> Arc<SingleFlight<CacheRef<T>>> {
        let flight = self.flights[kind.index()]
            .get_or_init(|| Arc::new(SingleFlight::<CacheRef<T>>::default()));
        // A kind has one value type, its `CacheDef::Value`.
        flight
            .clone()
            .downcast()
            .unwrap_or_else(|_| panic!("cache {} read as another value type", kind.name()))
    }

    /// Turns the disk cache on or off at runtime, by license; memory caches are unaffected.
    pub fn set_disk_allowed(&self, allowed: bool) {
        if let Some(disk) = &self.disk {
            disk.set_allowed(allowed);
        }
    }

    /// Evicts unreferenced entries until `relieved` returns true. Registered with the memory
    /// manager for the shared backend. It runs on an allocating thread that may hold any lock,
    /// so it neither logs nor blocks on cache shard locks.
    pub fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        self.store.shrink_for_pressure(relieved)
    }

    /// Totals of the reclaims under memory pressure; `None` with the legacy backend.
    pub fn reclaim_stats(&self) -> Option<ReclaimStats> {
        self.store.reclaim_stats()
    }

    /// Rows for `system.caches`: every kind, its bytes on disk while the disk cache is on, then
    /// each memory pool and each disk pool. Kinds turned off are listed with `enabled: false`.
    pub fn rows(&self) -> Vec<CacheRow> {
        self.composition()
            .into_iter()
            .map(|row| {
                if row.kind.is_some() {
                    with_hits(row)
                } else if row.disk {
                    self.with_disk_pool_hits(row)
                } else {
                    row
                }
            })
            .collect()
    }

    /// `rows` without the access, hit and miss counts, which live in metric families: reading
    /// one the first time registers it with the global metrics registry. The metrics collector
    /// runs under that registry's lock, so it reads this instead.
    pub(crate) fn composition(&self) -> Vec<CacheRow> {
        let mut rows = Vec::new();
        for kind in CacheKind::all() {
            let stats = self.store.kind_stats(kind);
            rows.push(CacheRow {
                name: kind.name().to_string(),
                kind: Some(kind),
                pool: Some(kind.pool()),
                disk: false,
                enabled: self.store.is_enabled(kind),
                num_items: stats.items as u64,
                size: stats.resident as u64,
                in_cache_size: stats.in_cache as u64,
                capacity: stats.capacity.map(|c| c as u64),
                unit: stats.unit,
                access: 0,
                hit: 0,
                miss: 0,
                events: stats.events,
            });
            if let Some(disk) = self
                .disk
                .as_ref()
                .filter(|d| d.is_allowed() && kind.on_disk())
            {
                let (items, bytes) = disk.kind_usage(kind);
                rows.push(CacheRow {
                    name: disk.metric_name(kind).to_string(),
                    kind: Some(kind),
                    pool: None,
                    disk: true,
                    enabled: disk.is_enabled(kind),
                    num_items: items,
                    size: bytes,
                    in_cache_size: bytes,
                    // Bounded by its disk pool.
                    capacity: None,
                    unit: Unit::Bytes,
                    access: 0,
                    hit: 0,
                    miss: 0,
                    events: None,
                });
            }
        }
        for stats in self.store.pool_stats() {
            rows.push(CacheRow {
                name: format!("__{POOL_PREFIX}{}", stats.pool.name()),
                kind: None,
                pool: Some(stats.pool),
                disk: false,
                enabled: stats.cap > 0,
                num_items: stats.items as u64,
                size: stats.resident as u64,
                in_cache_size: stats.in_cache as u64,
                capacity: Some(stats.cap as u64),
                unit: Unit::Bytes,
                access: 0,
                hit: 0,
                miss: 0,
                events: None,
            });
        }
        if let Some(disk) = self.disk.as_ref().filter(|d| d.is_allowed()) {
            for (pool, stats) in disk.pool_stats() {
                rows.push(CacheRow {
                    name: format!("__{DISK_POOL_PREFIX}{}", pool.name()),
                    kind: None,
                    pool: Some(pool),
                    disk: true,
                    enabled: stats.cap > 0,
                    num_items: stats.items,
                    size: stats.bytes,
                    in_cache_size: stats.bytes,
                    capacity: Some(stats.cap),
                    unit: Unit::Bytes,
                    access: 0,
                    hit: 0,
                    miss: 0,
                    events: None,
                });
            }
        }
        rows
    }

    fn with_disk_pool_hits(&self, row: CacheRow) -> CacheRow {
        let stats = self
            .disk
            .as_ref()
            .zip(row.pool)
            .and_then(|(disk, pool)| {
                disk.pool_stats()
                    .into_iter()
                    .find(|(p, _)| *p == pool)
                    .map(|(_, s)| s)
            })
            .unwrap_or_default();
        CacheRow {
            access: stats.hit + stats.miss,
            hit: stats.hit,
            miss: stats.miss,
            ..row
        }
    }

    /// Accepts `all` (0 only) and `pool:<meta|index|data>` with the shared backend, and a kind
    /// name (`memory_cache_*` or `cache_*`). The legacy backend reads the capacity in the
    /// kind's legacy unit, items or bytes; the shared backend reads bytes. 0 turns a kind off.
    pub fn set_cache_capacity(&self, name: &str, capacity: u64) -> Result<()> {
        let capacity = usize::try_from(capacity)
            .map_err(|_| ErrorCode::BadArguments("cache capacity exceeds this platform"))?;
        if name == "all" {
            if capacity != 0 {
                return Err(ErrorCode::BadArguments(
                    "capacity of 'all' can only be set to 0",
                ));
            }
            return Pool::ALL
                .iter()
                .try_for_each(|pool| self.store.set_pool_cap(*pool, 0));
        }
        if let Some(pool) = name.strip_prefix(POOL_PREFIX) {
            let pool = Pool::from_name(pool)
                .ok_or_else(|| ErrorCode::BadArguments(format!("unknown cache pool '{pool}'")))?;
            return self.store.set_pool_cap(pool, capacity);
        }
        self.store
            .set_kind_limit(adjustable_kind(name)?, KindLimit::bytes(capacity));
        Ok(())
    }

    /// `set_cache_capacity(<name>, <arg>)` with the argument as written in SQL: a number of
    /// bytes (or items, for legacy kinds counted in items), `'<n>%'` of the disk cache's
    /// `max_bytes` for a disk pool, or `'default'`. Names: `all`, `pool:<meta|index|data>`,
    /// `disk_pool:<meta|index|data>`, `disk:<kind>`, a kind name or its old name.
    pub fn set_cache_capacity_arg(&self, name: &str, arg: &str) -> Result<()> {
        let arg = arg.trim();
        if let Some(pool) = name.strip_prefix(DISK_POOL_PREFIX) {
            let pool = Pool::from_name(pool).ok_or_else(|| {
                ErrorCode::BadArguments(format!("unknown disk cache pool '{pool}'"))
            })?;
            let disk = self.disk_or_err()?;
            let cap = if arg == DEFAULT_CAPACITY {
                disk.default_pool_cap(pool)
            } else if let Some(percent) = arg.strip_suffix('%') {
                let percent = parse_u64(percent.trim(), arg)?;
                if percent > 100 {
                    return Err(ErrorCode::BadArguments(format!(
                        "disk pool capacity {arg} is over 100%"
                    )));
                }
                (disk.max_bytes() as u128 * percent as u128 / 100) as u64
            } else {
                parse_u64(arg, arg)?
            };
            return disk.set_pool_cap(pool, cap);
        }
        if let Some(kind) = name.strip_prefix(DISK_KIND_PREFIX) {
            let kind = adjustable_kind(kind)?;
            if !kind.on_disk() {
                return Err(ErrorCode::BadArguments(format!(
                    "cache {} has no disk cache",
                    kind.name()
                )));
            }
            let enabled = arg == DEFAULT_CAPACITY || parse_u64(arg, arg)? > 0;
            self.disk_or_err()?.set_kind_enabled(kind, enabled);
            return Ok(());
        }
        if arg == DEFAULT_CAPACITY {
            return self.set_cache_capacity_default(name);
        }
        self.set_cache_capacity(name, parse_u64(arg, arg)?)
    }

    fn disk_or_err(&self) -> Result<&Arc<DiskCache>> {
        self.disk.as_ref().ok_or_else(|| {
            ErrorCode::BadArguments(
                "the disk cache is off: set cache.data_cache_storage = 'disk' (EE only)",
            )
        })
    }

    /// Capacity `default` of a kind: back to the limit it started with, from the kind's
    /// `CacheDef::SHARED_DEFAULT` or `cache.kind_max_bytes`. Shared backend only: the legacy
    /// backend has no pools to bound a kind without a capacity.
    pub fn set_cache_capacity_default(&self, name: &str) -> Result<()> {
        let kind = adjustable_kind(name)?;
        let limit = self.store.default_limit(kind).ok_or_else(|| {
            ErrorCode::Unimplemented("capacity 'default' requires the shared cache backend")
        })?;
        self.store.set_kind_limit(kind, limit);
        Ok(())
    }
}

fn parse_u64(value: &str, arg: &str) -> Result<u64> {
    value.parse::<u64>().map_err(|_| {
        ErrorCode::BadArguments(format!(
            "cache capacity must be a number of bytes, '<n>%' or 'default', got '{arg}'"
        ))
    })
}

/// Options of the pre-refactor disk tiers the disk cache no longer reads, when set: the disk
/// pools replace the per-kind sizes, column chunks are no longer kept in memory as raw bytes,
/// and the queue is bounded in bytes, by `cache.disk.queue_bytes`. They are still accepted, so configs written for older versions start.
fn ignored_disk_options(config: &CacheConfig) -> Vec<&'static str> {
    let sizes = [
        (
            "disk_cache_table_bloom_index_meta_size",
            config.disk_cache_table_bloom_index_meta_size,
        ),
        (
            "disk_cache_table_bloom_index_data_size",
            config.disk_cache_table_bloom_index_data_size,
        ),
        (
            "disk_cache_inverted_index_meta_size",
            config.disk_cache_inverted_index_meta_size,
        ),
        (
            "disk_cache_inverted_index_lookup_size",
            config.disk_cache_inverted_index_lookup_size,
        ),
        (
            "disk_cache_inverted_index_payload_size",
            config.disk_cache_inverted_index_payload_size,
        ),
        (
            "disk_cache_vector_index_meta_size",
            config.disk_cache_vector_index_meta_size,
        ),
        (
            "disk_cache_vector_index_data_size",
            config.disk_cache_vector_index_data_size,
        ),
        (
            "disk_cache_spatial_index_meta_size",
            config.disk_cache_spatial_index_meta_size,
        ),
        (
            "disk_cache_spatial_index_data_size",
            config.disk_cache_spatial_index_data_size,
        ),
        (
            "disk_cache_virtual_column_meta_size",
            config.disk_cache_virtual_column_meta_size,
        ),
    ];
    let mut ignored: Vec<&'static str> = sizes
        .into_iter()
        .filter(|(_, size)| *size > 0)
        .map(|(name, _)| name)
        .collect();
    if config.data_cache_in_memory_bytes > 0 {
        ignored.push("data_cache_in_memory_bytes");
    }
    if config.table_data_cache_population_queue_size > 0 {
        ignored.push("table_data_cache_population_queue_size");
    }
    ignored
}

fn adjustable_kind(name: &str) -> Result<CacheKind> {
    CacheKind::from_name(name).ok_or_else(|| {
        ErrorCode::BadArguments(format!(
            "cache {name} not found, or not allowed to be adjusted"
        ))
    })
}

fn with_hits(row: CacheRow) -> CacheRow {
    CacheRow {
        access: get_cache_access_count(&row.name),
        hit: get_cache_hit_count(&row.name),
        miss: get_cache_miss_count(&row.name),
        ..row
    }
}
