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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_access_count;
use databend_common_metrics::cache::metrics_inc_cache_hit_count;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use databend_common_metrics::cache::metrics_inc_cache_miss_count;
use databend_common_metrics::cache::metrics_inc_cache_miss_load_millisecond;
use log::warn;

use crate::CacheAccessor;
use crate::CacheKind;
use crate::CachedItem;
use crate::Codec;
use crate::DiskCacheAccessor;
use crate::Reader;
use crate::SingleFlight;
use crate::backend::InsertMode;
use crate::backend::InsertOutcome;
use crate::backend::KindStats;
use crate::backend::KindStore;
use crate::handle::cache_ref::CacheRef;

struct DiskTier<T> {
    cache: Arc<DiskCacheAccessor>,
    /// Runtime switch shared by every disk tier (off when the license does not allow it).
    allowed: Arc<AtomicBool>,
    codec: Codec<T>,
}

impl<T> DiskTier<T> {
    fn allowed(&self) -> bool {
        self.allowed.load(Ordering::Relaxed)
    }

    fn read(&self, key: &str) -> Option<T> {
        if !self.allowed() {
            return None;
        }
        let bytes = self.cache.get(key)?;
        match (self.codec.decode)(bytes.as_ref().clone()) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!("failed to decode disk cache value, key {key}: {e}");
                self.cache.evict(key);
                None
            }
        }
    }

    fn write(&self, key: &str, value: &T) {
        if !self.allowed() || self.cache.contains_key(key) {
            return;
        }
        match (self.codec.encode)(value) {
            Ok(bytes) => {
                self.cache.insert(key.to_string(), bytes.into());
            }
            Err(e) => warn!("failed to encode disk cache value, key {key}: {e}"),
        }
    }
}

/// Typed handle to one kind: memory tier in the store, optional disk tier, single-flight loads.
/// Cheap to clone; clones share in-flight loads.
pub struct MemCache<T: CachedItem> {
    kind: CacheKind,
    store: Arc<dyn KindStore>,
    disk: Option<Arc<DiskTier<T>>>,
    flight: Arc<SingleFlight<CacheRef<T>>>,
    read_only: bool,
}

impl<T: CachedItem> Clone for MemCache<T> {
    fn clone(&self) -> Self {
        MemCache {
            kind: self.kind,
            store: self.store.clone(),
            disk: self.disk.clone(),
            flight: self.flight.clone(),
            read_only: self.read_only,
        }
    }
}

impl<T: CachedItem> MemCache<T> {
    /// A handle binding `T` to `kind` as given. `CacheManager::cache` derives both, and the disk
    /// tier, from the kind's `CacheDef`; this one is for tests and benchmarks.
    pub fn new(store: Arc<dyn KindStore>, kind: CacheKind) -> Self {
        MemCache {
            kind,
            store,
            disk: None,
            flight: Arc::new(SingleFlight::default()),
            read_only: false,
        }
    }

    pub fn with_disk(
        mut self,
        cache: Arc<DiskCacheAccessor>,
        allowed: Arc<AtomicBool>,
        codec: Codec<T>,
    ) -> Self {
        self.disk = Some(Arc::new(DiskTier {
            cache,
            allowed,
            codec,
        }));
        self
    }

    /// Same cache, but reads never refill it. `put` and `evict` still apply.
    ///
    /// Shares in-flight loads with the cache it came from: a caller on the full cache that
    /// joins a load started by a read-only view gets the value, but nothing is cached. The
    /// next lookup misses again and fills then.
    pub fn read_only(&self) -> Self {
        MemCache {
            read_only: true,
            ..self.clone()
        }
    }

    /// Whether values of this kind can be cached at all; lets writers skip building them.
    /// True with only the disk tier: as before the refactor, a memory capacity of 0 leaves
    /// the disk tier working.
    pub fn is_enabled(&self) -> bool {
        self.memory_enabled() || self.disk_allowed()
    }

    fn memory_enabled(&self) -> bool {
        self.store.is_enabled(self.kind)
    }

    fn disk_allowed(&self) -> bool {
        self.disk.as_ref().is_some_and(|d| d.allowed())
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn kind(&self) -> CacheKind {
        self.kind
    }

    pub fn name(&self) -> &'static str {
        self.kind.name()
    }

    /// Items, bytes and capacity of the kind's memory tier.
    pub fn stats(&self) -> KindStats {
        self.store.kind_stats(self.kind)
    }

    pub fn lookup(&self, key: &str) -> Option<CacheRef<T>> {
        self.lookup_tiers(key, None)
    }

    /// Like `lookup`, also accounting `len` bytes as served by or missed in the cache.
    pub fn lookup_sized(&self, key: &str, len: u64) -> Option<CacheRef<T>> {
        self.lookup_tiers(key, Some(len))
    }

    pub fn contains(&self, key: &str) -> bool {
        (self.memory_enabled() && self.store.contains(self.kind, key))
            || self
                .disk
                .as_ref()
                .is_some_and(|d| d.allowed() && d.cache.contains_key(key))
    }

    pub async fn get<R: Reader<Item = T>>(&self, reader: &R, key: R::Key) -> Result<CacheRef<T>> {
        let cache_key = reader.cache_key(&key);
        if let Some(hit) = self.lookup(&cache_key) {
            return Ok(hit);
        }
        let cache_key = cache_key.into_owned();
        // Type-erased so the reader's future does not nest into every caller's future type,
        // as `async_trait` loaders did before. Allocates only on a miss, next to the IO.
        let load: Pin<Box<dyn Future<Output = Result<CacheRef<T>>> + Send + '_>> =
            Box::pin(self.flight.run(&cache_key, async {
                let start = Instant::now();
                let value = reader.read(&key).await?;
                if self.is_enabled() {
                    let millis = start.elapsed().as_millis() as u64;
                    metrics_inc_cache_miss_load_millisecond(millis, self.name());
                }
                Ok(self.fill(&cache_key, value))
            }));
        load.await
    }

    pub fn get_sync(&self, key: &str, compute: impl FnOnce() -> Result<T>) -> Result<CacheRef<T>> {
        if let Some(hit) = self.lookup(key) {
            return Ok(hit);
        }
        Ok(self.fill(key, compute()?))
    }

    /// Read-path refill: subject to admission; a no-op in read-only mode.
    pub fn fill(&self, key: &str, value: T) -> CacheRef<T> {
        self.store_value(key, value, InsertMode::Fill, true)
    }

    /// Write-path insertion: bypasses admission and applies in read-only mode.
    pub fn put(&self, key: &str, value: T) -> CacheRef<T> {
        self.store_value(key, value, InsertMode::Put, true)
    }

    /// Removes the entry from every tier; readers still holding it keep it charged until they drop it.
    pub fn evict(&self, key: &str) -> bool {
        let in_memory = self.store.evict(self.kind, key);
        let on_disk = self
            .disk
            .as_ref()
            .is_some_and(|d| d.allowed() && d.cache.evict(key));
        in_memory || on_disk
    }

    fn lookup_tiers(&self, key: &str, len: Option<u64>) -> Option<CacheRef<T>> {
        if !self.memory_enabled() {
            return self.lookup_disk(key);
        }
        let name = self.name();
        metrics_inc_cache_access_count(1, name);
        let hit = self.store.get(self.kind, key).and_then(|erased| {
            let hit = CacheRef::from_erased(erased);
            // A kind is bound to one value type; a mismatch means two `MemCache`s with
            // different `T` share a kind. Treated as a miss, and the refill overwrites it.
            debug_assert!(
                hit.is_some(),
                "cache kind {:?} bound to two types",
                self.kind
            );
            if hit.is_none() {
                warn!("cache kind {:?} holds a value of another type", self.kind);
            }
            hit
        });
        if let Some(hit) = hit {
            metrics_inc_cache_hit_count(1, name);
            if let Some(len) = len {
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytesFromDataCache,
                    len as usize,
                );
            }
            if let Some(disk) = &self.disk {
                disk.write(key, &hit);
            }
            return Some(hit);
        }
        metrics_inc_cache_miss_count(1, name);
        let hit = self.lookup_disk(key);
        if hit.is_none()
            && let Some(len) = len
        {
            metrics_inc_cache_miss_bytes(len, name);
        }
        hit
    }

    /// A disk hit refills the memory tier when it is enabled. The disk cache records its own
    /// hits and bytes.
    fn lookup_disk(&self, key: &str) -> Option<CacheRef<T>> {
        let value = self.disk.as_ref().and_then(|d| d.read(key))?;
        Some(self.store_value(key, value, InsertMode::Fill, false))
    }

    fn store_value(&self, key: &str, value: T, mode: InsertMode, to_disk: bool) -> CacheRef<T> {
        if mode == InsertMode::Fill && self.read_only {
            return CacheRef::new(value);
        }
        if to_disk && let Some(disk) = &self.disk {
            disk.write(key, &value);
        }
        if !self.memory_enabled() {
            return CacheRef::new(value);
        }
        let bytes = value.mem_bytes();
        let value = CacheRef::new(value);
        if let InsertOutcome::Inserted(reservation) =
            self.store
                .insert(self.kind, key, value.erase(), bytes, mode)
        {
            value.attach(reservation);
        }
        value
    }
}
