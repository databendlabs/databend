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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_meta_app::tenant::Tenant;

use super::registry::CachedRange;
use super::ttl_cache::TtlCache;

/// Reads answered from memory and reads sent to meta-service, of one range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeStats {
    pub hit: u64,
    pub miss: u64,
}

/// The cached ranges of one tenant, and how long a result is kept.
///
/// The TTL starts at zero, which keeps nothing: every read goes to meta-service. The owner sets
/// it, see [`Self::set_ttl`] and [`Self::set_ttl_as_read`].
pub struct MetaReadCache {
    tenant: Tenant,
    ranges: Vec<(CachedRange, TtlCache)>,
    ttl_ms: AtomicU64,
    /// Incremented by [`Self::set_ttl`]; held while the TTL changes.
    ttl_epoch: Mutex<u64>,
}

impl MetaReadCache {
    pub fn create(tenant: &Tenant) -> Arc<Self> {
        let ranges = CachedRange::ALL
            .iter()
            .map(|r| (*r, TtlCache::new(r.dir(tenant), r.keep_absent())))
            .collect();
        Arc::new(MetaReadCache {
            tenant: tenant.clone(),
            ranges,
            ttl_ms: AtomicU64::new(0),
            ttl_epoch: Mutex::new(0),
        })
    }

    /// Whether the ranges of `tenant` are the cached ones.
    pub fn is_for(&self, tenant: &Tenant) -> bool {
        self.tenant.tenant_name() == tenant.tenant_name()
    }

    /// The setting giving the TTL was written by this process: keep results for `ttl` from now
    /// on; zero stops answering from memory at once.
    pub fn set_ttl(&self, ttl: Duration) {
        let mut epoch = self.ttl_epoch.lock().unwrap();
        *epoch += 1;
        self.ttl_ms.store(ttl.as_millis() as u64, Ordering::Release);
    }

    /// Take before reading the setting that gives the TTL, and pass to [`Self::set_ttl_as_read`].
    pub fn ttl_epoch(&self) -> u64 {
        *self.ttl_epoch.lock().unwrap()
    }

    /// The setting giving the TTL was read as `ttl` by a read that started at `epoch`. Apply it
    /// unless [`Self::set_ttl`] ran since: the read may predate that write, and would undo it.
    /// Returns whether it was applied.
    pub fn set_ttl_as_read(&self, epoch: u64, ttl: Duration) -> bool {
        let current = self.ttl_epoch.lock().unwrap();
        if *current != epoch {
            return false;
        }
        self.ttl_ms.store(ttl.as_millis() as u64, Ordering::Release);
        true
    }

    pub fn is_enabled(&self) -> bool {
        self.ttl().is_some()
    }

    /// `None` if nothing is answered from memory.
    pub(crate) fn ttl(&self) -> Option<Duration> {
        match self.ttl_ms.load(Ordering::Acquire) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// The cached range containing `key`.
    pub(crate) fn find(&self, key: &str) -> Option<&TtlCache> {
        self.ranges
            .iter()
            .map(|(_, c)| c)
            .find(|c| key.starts_with(&c.dir))
    }

    /// Drop every result of every range.
    pub(crate) fn invalidate_all(&self) {
        for (_, c) in &self.ranges {
            c.invalidate();
        }
    }

    /// Per-range counters, for logs and tests.
    pub fn stats(&self) -> Vec<(&'static str, RangeStats)> {
        self.ranges
            .iter()
            .map(|(r, c)| {
                let (hit, miss) = c.stats();
                (r.name(), RangeStats { hit, miss })
            })
            .collect()
    }
}
