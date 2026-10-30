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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::get_cache_access_count;
use databend_common_metrics::cache::get_cache_hit_count;
use databend_common_metrics::cache::get_cache_miss_count;

use crate::CacheAccessor;
use crate::DiskCacheAccessor;
use crate::Unit;
use crate::cache_kind::CacheKind;
use crate::cache_kind::Pool;
use crate::cached_item::CachedItem;
use crate::disk_codec::DiskCodec;
use crate::mem_cache::MemCache;
use crate::pool_caps::KindLimit;
use crate::store::KindStats;
use crate::store::KindStore;
use crate::store::PoolStats;
use crate::store::ShrinkReport;

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
}

/// Owns the store and the disk tiers and hands out typed caches. Backend-agnostic.
pub struct CacheManager {
    store: Arc<dyn KindStore>,
    disks: HashMap<CacheKind, Arc<DiskCacheAccessor>>,
}

impl CacheManager {
    pub fn new(
        store: Arc<dyn KindStore>,
        disks: HashMap<CacheKind, Arc<DiskCacheAccessor>>,
    ) -> Self {
        CacheManager { store, disks }
    }

    pub fn mem_cache<T: CachedItem>(&self, kind: CacheKind) -> MemCache<T> {
        MemCache::new(self.store.clone(), kind)
    }

    /// Like `mem_cache`, with the kind's disk tier attached when one is configured.
    pub fn hybrid_cache<T: CachedItem + DiskCodec>(&self, kind: CacheKind) -> MemCache<T> {
        let cache = self.mem_cache(kind);
        match self.disks.get(&kind) {
            Some(disk) => cache.with_disk(disk.clone()),
            None => cache,
        }
    }

    pub fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        self.store.shrink_for_pressure(relieved)
    }

    pub fn kind_stats(&self) -> Vec<KindStats> {
        CacheKind::ALL
            .iter()
            .map(|kind| self.store.kind_stats(*kind))
            .collect()
    }

    pub fn pool_stats(&self) -> Vec<PoolStats> {
        self.store.pool_stats()
    }

    /// Rows for `system.caches`: every kind, its disk tier if any, then each pool.
    pub fn rows(&self) -> Vec<CacheRow> {
        let mut rows = Vec::new();
        for stats in self.kind_stats() {
            let kind = stats.kind;
            rows.push(with_hits(CacheRow {
                name: kind.legacy_name().to_string(),
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
            }));
            if let Some(disk) = self.disks.get(&kind) {
                rows.push(with_hits(CacheRow {
                    name: disk.name().to_string(),
                    kind: Some(kind),
                    pool: None,
                    enabled: self.store.is_enabled(kind),
                    num_items: disk.len() as u64,
                    size: disk.bytes_size(),
                    in_cache_size: disk.bytes_size(),
                    capacity: Some(disk.bytes_capacity()),
                    unit: Unit::Bytes,
                    access: 0,
                    hit: 0,
                    miss: 0,
                }));
            }
        }
        for stats in self.pool_stats() {
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
            });
        }
        rows
    }

    /// Accepts `all` (0 only), `pool:<meta|index|data>`, a kind name, or a legacy cache name.
    pub fn set_capacity_by_name(&self, name: &str, bytes: usize) -> Result<()> {
        if name == "all" {
            if bytes != 0 {
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
            return self.store.set_pool_cap(pool, bytes);
        }
        let kind = CacheKind::from_any_name(name)
            .ok_or_else(|| ErrorCode::BadArguments(format!("unknown cache '{name}'")))?;
        self.store.set_kind_limit(kind, KindLimit::bytes(bytes));
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
