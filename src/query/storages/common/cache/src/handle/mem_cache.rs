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
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_metrics::cache::CacheLookupCounters;
use databend_common_metrics::cache::metrics_inc_cache_miss_load_millisecond;
use log::warn;

use crate::CacheKind;
use crate::CachePolicy;
use crate::CachedItem;
use crate::Reader;
use crate::SingleFlight;
use crate::backend::InsertOutcome;
use crate::backend::KindStats;
use crate::backend::KindStore;
use crate::handle::cache_ref::CacheRef;

/// Typed handle to one kind's memory cache, optionally sharing loads. Cheap to clone; clones
/// share in-flight loads, and so do all handles of a kind from one `CacheManager`, for kinds
/// that share loads (`CacheDef::SHARE_LOADS`). The disk cache is below the readers
/// (`DiskCachedStorage`); a handle passes its `CachePolicy` to a reader on a miss, and applies
/// its `memory` part itself.
pub struct MemCache<T: CachedItem> {
    kind: CacheKind,
    store: Arc<dyn KindStore>,
    /// `None`: every miss loads on its own.
    flight: Option<Arc<SingleFlight<CacheRef<T>>>>,
    /// Counted on every lookup, so looked up once.
    counters: Arc<CacheLookupCounters>,
    /// What reads through this handle may do with the caches.
    policy: CachePolicy,
}

impl<T: CachedItem> Clone for MemCache<T> {
    fn clone(&self) -> Self {
        MemCache {
            kind: self.kind,
            store: self.store.clone(),
            flight: self.flight.clone(),
            counters: self.counters.clone(),
            policy: self.policy,
        }
    }
}

impl<T: CachedItem> MemCache<T> {
    /// A handle binding `T` to `kind` as given, sharing loads among its clones.
    /// `CacheManager::cache` derives the shared loads from the kind's `CacheDef`; this one is
    /// for tests and benchmarks.
    pub fn new(store: Arc<dyn KindStore>, kind: CacheKind) -> Self {
        let counters = Arc::new(CacheLookupCounters::new(kind.name()));
        let flight = Some(Arc::new(SingleFlight::default()));
        Self::with_shared(store, kind, counters, flight)
    }

    /// A handle sharing the kind's lookup counters and, with `Some`, in-flight loads with the
    /// other handles of the kind; with `None`, every miss loads on its own.
    pub(crate) fn with_shared(
        store: Arc<dyn KindStore>,
        kind: CacheKind,
        counters: Arc<CacheLookupCounters>,
        flight: Option<Arc<SingleFlight<CacheRef<T>>>>,
    ) -> Self {
        MemCache {
            kind,
            store,
            flight,
            counters,
            policy: CachePolicy::DEFAULT,
        }
    }

    /// Same cache, read under both its policy and `policy` (`CachePolicy::at_most`). With a
    /// `memory` that does not write, reads never refill it; `put` and `evict` still apply.
    /// With one that does not read, lookups miss.
    ///
    /// Shares in-flight loads with the cache it came from: a caller on the full cache that
    /// joins a load started by a read-only view gets the value, but nothing is cached. The
    /// next lookup misses again and fills then.
    pub fn with_policy(&self, policy: CachePolicy) -> Self {
        MemCache {
            policy: self.policy.at_most(policy),
            ..self.clone()
        }
    }

    /// Whether values of this kind can be cached in memory; lets writers skip building them.
    pub fn is_enabled(&self) -> bool {
        self.store.is_enabled(self.kind)
    }

    /// What reads through this handle may do with the caches; a reader gets it on a miss.
    pub fn policy(&self) -> CachePolicy {
        self.policy
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
        self.is_enabled() && self.store.contains(self.kind, key)
    }

    pub async fn get<R: Reader<Item = T>>(&self, reader: &R, key: R::Key) -> Result<CacheRef<T>> {
        let cache_key = reader.cache_key(&key);
        if let Some(hit) = self.lookup(&cache_key) {
            return Ok(hit);
        }
        let cache_key = cache_key.into_owned();
        let policy = self.policy;
        let load = async {
            let start = Instant::now();
            let value = reader.read(&key, &policy).await?;
            if self.is_enabled() {
                let millis = start.elapsed().as_millis() as u64;
                metrics_inc_cache_miss_load_millisecond(millis, self.name());
            }
            Ok(self.fill(&cache_key, value))
        };
        // Type-erased so the reader's future does not nest into every caller's future type,
        // as `async_trait` loaders did before. Allocates only on a miss, next to the IO.
        let load: Pin<Box<dyn Future<Output = Result<CacheRef<T>>> + Send + '_>> =
            match &self.flight {
                None => Box::pin(load),
                Some(flight) => Box::pin(flight.run(&cache_key, async {
                    // A load that completed between the miss and joining the flight.
                    let hit = self
                        .policy
                        .memory
                        .reads()
                        .then(|| self.memory_hit(&cache_key));
                    match hit.flatten() {
                        Some(hit) => Ok(hit),
                        None => load.await,
                    }
                })),
            };
        load.await
    }

    pub fn get_sync(&self, key: &str, compute: impl FnOnce() -> Result<T>) -> Result<CacheRef<T>> {
        if let Some(hit) = self.lookup(key) {
            return Ok(hit);
        }
        Ok(self.fill(key, compute()?))
    }

    /// Read-path refill; a no-op unless the policy writes the memory cache.
    pub fn fill(&self, key: &str, value: T) -> CacheRef<T> {
        self.store_value(key, value, true)
    }

    /// Write-path insertion; applies in read-only mode too.
    pub fn put(&self, key: &str, value: T) -> CacheRef<T> {
        self.store_value(key, value, false)
    }

    /// Removes the entry; readers still holding it keep it charged until they drop it.
    pub fn evict(&self, key: &str) -> bool {
        self.store.evict(self.kind, key)
    }

    /// The value in the memory tier, without counting an access.
    fn memory_hit(&self, key: &str) -> Option<CacheRef<T>> {
        let erased = self.store.get(self.kind, key)?;
        let hit = CacheRef::from_erased(erased);
        // A kind is bound to one value type; a mismatch means two `MemCache`s with different
        // `T` share a kind. Treated as a miss, and the refill overwrites it.
        debug_assert!(
            hit.is_some(),
            "cache kind {:?} bound to two types",
            self.kind
        );
        if hit.is_none() {
            warn!("cache kind {:?} holds a value of another type", self.kind);
        }
        hit
    }

    fn lookup_tiers(&self, key: &str, len: Option<u64>) -> Option<CacheRef<T>> {
        if !self.is_enabled() || !self.policy.memory.reads() {
            return None;
        }
        self.counters.access();
        if let Some(hit) = self.memory_hit(key) {
            self.counters.hit();
            if let Some(len) = len {
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytesFromDataCache,
                    len as usize,
                );
            }
            return Some(hit);
        }
        self.counters.miss();
        if let Some(len) = len {
            self.counters.miss_bytes(len);
        }
        None
    }

    /// `fill`: a read-path refill, skipped in read-only mode.
    fn store_value(&self, key: &str, value: T, fill: bool) -> CacheRef<T> {
        if fill && !self.policy.memory.writes() {
            return CacheRef::new(value);
        }
        if !self.is_enabled() {
            return CacheRef::new(value);
        }
        let bytes = if self.store.charges_legacy_bytes() {
            value.legacy_bytes()
        } else {
            value.mem_bytes()
        };
        let value = CacheRef::new(value);
        if let InsertOutcome::Inserted(reservation) =
            self.store.insert(self.kind, key, value.erase(), bytes)
        {
            value.attach(reservation);
        }
        value
    }
}
