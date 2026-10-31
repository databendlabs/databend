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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::get_cache_access_count;
use databend_common_metrics::cache::get_cache_hit_count;
use databend_common_metrics::cache::get_cache_miss_count;
use log::warn;

use crate::CacheAccessor;
use crate::CacheDef;
use crate::CacheKind;
use crate::DiskCacheAccessor;
use crate::KindLimit;
use crate::MemCache;
use crate::Pool;
use crate::Unit;
use crate::backend::KindEvents;
use crate::backend::KindStore;
use crate::backend::ReclaimStats;
use crate::backend::ShrinkReport;
use crate::disk::disk_tiers;
use crate::legacy::LegacyKinds;
use crate::legacy::LegacyStore;
use crate::metrics::CacheMetricsCollector;
use crate::shared::SharedStore;

const POOL_PREFIX: &str = "pool:";

/// One row of `system.caches`: a kind's memory tier, its disk tier, or a pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRow {
    pub name: String,
    pub kind: Option<CacheKind>,
    pub pool: Option<Pool>,
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

/// The process-wide caches: the backend chosen by `cache.shared_memory_enabled` and the disk
/// tiers. Hands out a typed cache per registered kind.
pub struct CacheManager {
    store: Arc<dyn KindStore>,
    disks: HashMap<CacheKind, Arc<DiskCacheAccessor>>,
    /// Runtime switch of every disk tier; off when the license does not allow them.
    disk_allowed: Arc<AtomicBool>,
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

    /// Disk tiers exist only in EE mode, and can then be turned off at runtime by
    /// `set_disk_allowed`.
    pub fn try_new(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<Self> {
        let disks = disk_tiers(config, &tenant_id.into(), ee_mode)?;
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
        let manager = CacheManager::new(store, disks);
        manager.set_disk_allowed(ee_mode);
        Ok(manager)
    }

    pub fn new(
        store: Arc<dyn KindStore>,
        disks: HashMap<CacheKind, Arc<DiskCacheAccessor>>,
    ) -> Self {
        CacheManager {
            store,
            disks,
            disk_allowed: Arc::new(AtomicBool::new(true)),
        }
    }

    /// The cache of kind `D`, with its disk tier when `D` defines one and the config enables it.
    pub fn cache<D: CacheDef>(&self) -> MemCache<D::Value> {
        let kind = CacheKind::of::<D>();
        let cache = MemCache::new(self.store.clone(), kind);
        match (D::DISK, self.disks.get(&kind)) {
            (Some(disk), Some(tier)) => {
                cache.with_disk(tier.clone(), self.disk_allowed.clone(), disk.codec)
            }
            _ => cache,
        }
    }

    /// Turns every disk tier on or off at runtime; memory tiers are unaffected.
    pub fn set_disk_allowed(&self, allowed: bool) {
        self.disk_allowed.store(allowed, Ordering::Relaxed);
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

    /// Rows for `system.caches`: every kind, its disk tier while disk tiers are allowed, then
    /// each pool. Kinds turned off are listed with `enabled: false`.
    pub fn rows(&self) -> Vec<CacheRow> {
        let mut rows = Vec::new();
        for kind in CacheKind::all() {
            let stats = self.store.kind_stats(kind);
            rows.push(with_hits(CacheRow {
                name: kind.name().to_string(),
                kind: Some(kind),
                pool: Some(kind.pool()),
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
            }));
            let disk_allowed = self.disk_allowed.load(Ordering::Relaxed);
            if let Some(disk) = self.disks.get(&kind).filter(|_| disk_allowed) {
                rows.push(with_hits(CacheRow {
                    name: disk.name().to_string(),
                    kind: Some(kind),
                    pool: None,
                    // Works without the memory tier.
                    enabled: true,
                    num_items: disk.len() as u64,
                    size: disk.bytes_size(),
                    in_cache_size: disk.bytes_size(),
                    capacity: Some(disk.bytes_capacity()),
                    unit: Unit::Bytes,
                    access: 0,
                    hit: 0,
                    miss: 0,
                    events: None,
                }));
            }
        }
        for stats in self.store.pool_stats() {
            rows.push(CacheRow {
                name: format!("__{POOL_PREFIX}{}", stats.pool.name()),
                kind: None,
                pool: Some(stats.pool),
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
        rows
    }

    /// Accepts `all` (0 only) and `pool:<meta|index|data>` with the shared backend, and a kind
    /// name (`memory_cache_*` or `cache_*`). The legacy backend reads the capacity in the
    /// kind's legacy unit, items or bytes; the shared backend reads bytes.
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
        let kind = CacheKind::from_name(name).ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "cache {name} not found, or not allowed to be adjusted"
            ))
        })?;
        self.store.set_kind_limit(kind, KindLimit::bytes(capacity));
        Ok(())
    }
}

fn with_hits(row: CacheRow) -> CacheRow {
    CacheRow {
        access: get_cache_access_count(&row.name),
        hit: get_cache_hit_count(&row.name),
        miss: get_cache_miss_count(&row.name),
        ..row
    }
}
