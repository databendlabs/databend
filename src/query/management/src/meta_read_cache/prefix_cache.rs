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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::register_counter;
use databend_meta_client::ClientHandle;
use databend_meta_client::types::SeqV;
use databend_meta_plugin_cache::Cache;
use databend_meta_plugin_cache::CacheReader;
use databend_meta_runtime::DatabendRuntime;
use log::info;
use log::warn;

/// A write this process made that the watch stream has not delivered yet.
///
/// Whether a fence is observed depends only on the fence and a cache state, so checking it
/// against a state older than the fence is never wrong, only conservative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fence {
    /// The key was written with `seq`; the cache must have applied events up to `seq`. Watch
    /// events arrive in seq order, so the write is then reflected, even if a later change
    /// deleted the key.
    Present { seq: u64, since: Instant },
    /// The key, last at `seq`, was deleted. The cache must have applied `seq` and must not hold
    /// it any more: without the first condition, a cache that has not seen the put yet would
    /// count as having seen the delete.
    Absent { seq: u64, since: Instant },
}

impl Fence {
    fn observed(&self, cached: Option<&SeqV>, last_seq: u64) -> bool {
        match self {
            Fence::Present { seq, .. } => last_seq >= *seq,
            Fence::Absent { seq, .. } => last_seq >= *seq && cached.is_none_or(|v| v.seq > *seq),
        }
    }

    fn since(&self) -> Instant {
        match self {
            Fence::Present { since, .. } | Fence::Absent { since, .. } => *since,
        }
    }

    /// The position of the write in the key's history. A put at `seq` is followed by the delete
    /// of that value, which is followed by any put with a larger seq.
    fn position(&self) -> (u64, bool) {
        match self {
            Fence::Present { seq, .. } => (*seq, false),
            Fence::Absent { seq, .. } => (*seq, true),
        }
    }
}

/// Keep the latest write of `key`. Concurrent writes may record their fences out of commit
/// order; an older fence must not replace a newer one, or observing the older write would clear
/// the fence of the newer.
fn merge_fence(fences: &mut BTreeMap<String, Fence>, key: &str, fence: Fence) {
    match fences.get(key) {
        Some(existing) if existing.position() >= fence.position() => {}
        _ => {
            fences.insert(key.to_string(), fence);
        }
    }
}

/// Counters of one prefix. Every read is counted once, as a hit or by its fallback reason.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrefixStats {
    pub hit: u64,
    pub fallback_not_ready: u64,
    pub fallback_suspect: u64,
    pub fallback_fence: u64,
    pub fallback_absent: u64,
    pub resync: u64,
}

/// One event counter: per instance for [`PrefixStats`], and process-wide in Prometheus as
/// `meta_read_cache_<prefix>_<event>`.
struct EventCounter {
    local: AtomicU64,
    exported: Counter,
}

impl EventCounter {
    fn new(prefix: &'static str, event: &'static str) -> Self {
        static EXPORTED: LazyLock<Mutex<HashMap<String, Counter>>> =
            LazyLock::new(Default::default);
        let name = format!("meta_read_cache_{prefix}_{event}");
        let exported = EXPORTED
            .lock()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| register_counter(&name))
            .clone();
        EventCounter {
            local: AtomicU64::new(0),
            exported,
        }
    }

    fn fetch_add(&self, n: u64, order: Ordering) {
        self.local.fetch_add(n, order);
        self.exported.inc_by(n);
    }

    fn load(&self, order: Ordering) -> u64 {
        self.local.load(order)
    }
}

struct Counters {
    hit: EventCounter,
    fallback_not_ready: EventCounter,
    fallback_suspect: EventCounter,
    fallback_fence: EventCounter,
    fallback_absent: EventCounter,
    resync: EventCounter,
}

impl Counters {
    fn new(prefix: &'static str) -> Self {
        Counters {
            hit: EventCounter::new(prefix, "hit"),
            fallback_not_ready: EventCounter::new(prefix, "fallback_not_ready"),
            fallback_suspect: EventCounter::new(prefix, "fallback_suspect"),
            fallback_fence: EventCounter::new(prefix, "fallback_fence"),
            fallback_absent: EventCounter::new(prefix, "fallback_absent"),
            resync: EventCounter::new(prefix, "resync"),
        }
    }
}

pub(crate) struct PrefixCache {
    pub(crate) name: &'static str,
    /// The cached key range, with a trailing `/`.
    pub(crate) dir: String,
    /// Whether an absent key or an empty list may be answered from memory. Off for users, so
    /// that a user just created elsewhere is never rejected by a cache that has not caught up.
    serve_absent: bool,
    /// Owns the watcher task; dropping it stops the watch stream.
    _cache: Cache,
    reader: CacheReader,
    fences: Mutex<BTreeMap<String, Fence>>,
    /// Number of entries in `fences`, so that the read path skips the lock when there is none.
    fence_count: AtomicUsize,
    /// Set by the verifier; all reads fall back until a later verification passes.
    suspect: AtomicBool,
    /// Consecutive verifications that found a difference from the leader.
    pub(crate) mismatches: AtomicU32,
    counters: Counters,
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Whether `v` is still alive at `now_ms`; a TTL key may outlive its expiry until meta-service
/// applies the next log and deletes it, and a direct read would not return it.
pub(crate) fn is_live(v: &SeqV, now_ms: u64) -> bool {
    v.meta
        .as_ref()
        .and_then(|m| m.get_expire_at_ms())
        .is_none_or(|expire_at| expire_at > now_ms)
}

impl PrefixCache {
    pub(crate) async fn new(
        client: Arc<ClientHandle<DatabendRuntime>>,
        name: &'static str,
        dir: String,
        serve_absent: bool,
    ) -> Self {
        let cache = Cache::new(
            client,
            dir.trim_end_matches('/'),
            format!("meta-read-cache-{name}"),
        )
        .await;
        let reader = cache.reader();
        PrefixCache {
            name,
            dir,
            serve_absent,
            _cache: cache,
            reader,
            fences: Mutex::new(BTreeMap::new()),
            fence_count: AtomicUsize::new(0),
            suspect: AtomicBool::new(false),
            mismatches: AtomicU32::new(0),
            counters: Counters::new(name),
        }
    }

    /// Answer `get(key)` from memory, or `None` to fall back.
    pub(crate) fn get(&self, key: &str, now_ms: u64) -> Option<Option<SeqV>> {
        if self.suspect.load(Ordering::Acquire) {
            return self.count(&self.counters.fallback_suspect);
        }
        let Some(Ok((cached, last_seq))) = self
            .reader
            .try_access_now(|d| (d.data.get(key).cloned(), d.last_seq))
        else {
            return self.count(&self.counters.fallback_not_ready);
        };
        if !self.fences_observed(last_seq, |k| (k == key).then_some(cached.as_ref())) {
            return self.count(&self.counters.fallback_fence);
        }
        let live = cached.filter(|v| is_live(v, now_ms));
        if live.is_none() && !self.serve_absent {
            return self.count(&self.counters.fallback_absent);
        }
        self.counters.hit.fetch_add(1, Ordering::Relaxed);
        Some(live)
    }

    /// Answer `list(prefix)` from memory, or `None` to fall back. `prefix` must be under `dir`.
    pub(crate) fn list(&self, prefix: &str, now_ms: u64) -> Option<Vec<(String, SeqV)>> {
        if self.suspect.load(Ordering::Acquire) {
            return self.count(&self.counters.fallback_suspect);
        }
        let Some(Ok((items, last_seq))) = self.reader.try_access_now(|d| {
            let items = d
                .data
                .range(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>();
            (items, d.last_seq)
        }) else {
            return self.count(&self.counters.fallback_not_ready);
        };
        if !self.fences_observed(last_seq, |k| k.starts_with(prefix).then(|| items.get(k))) {
            return self.count(&self.counters.fallback_fence);
        }
        let live: Vec<_> = items
            .into_iter()
            .filter(|(_, v)| is_live(v, now_ms))
            .collect();
        if live.is_empty() && !self.serve_absent {
            return self.count(&self.counters.fallback_absent);
        }
        self.counters.hit.fetch_add(1, Ordering::Relaxed);
        Some(live)
    }

    /// Check, and drop once observed, the fences of keys in a read range.
    ///
    /// `in_range(key)` returns `None` for a key outside the range, otherwise the cached value.
    fn fences_observed<'a>(
        &self,
        last_seq: u64,
        in_range: impl Fn(&str) -> Option<Option<&'a SeqV>>,
    ) -> bool {
        if self.fence_count.load(Ordering::Acquire) == 0 {
            return true;
        }
        let mut fences = self.fences.lock().unwrap();
        let mut observed = true;
        fences.retain(|k, f| match in_range(k) {
            None => true,
            Some(cached) if f.observed(cached, last_seq) => false,
            Some(_) => {
                observed = false;
                true
            }
        });
        self.fence_count.store(fences.len(), Ordering::Release);
        observed
    }

    /// Record that this process wrote `key` and meta-service assigned `seq`.
    pub(crate) fn record_put(&self, key: &str, seq: u64) {
        self.add_fence(key, Fence::Present {
            seq,
            since: Instant::now(),
        });
    }

    /// Record that this process deleted `key`, whose last value had `seq`.
    pub(crate) fn record_delete(&self, key: &str, seq: u64) {
        self.add_fence(key, Fence::Absent {
            seq,
            since: Instant::now(),
        });
    }

    fn add_fence(&self, key: &str, fence: Fence) {
        let mut fences = self.fences.lock().unwrap();
        merge_fence(&mut fences, key, fence);
        self.fence_count.store(fences.len(), Ordering::Release);
    }

    /// Drop the fences the cache has observed; return the age of the oldest remaining one.
    pub(crate) fn oldest_unobserved_fence(&self) -> Option<Duration> {
        if self.fence_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        let Some(Ok(observed)) = self.reader.try_access_now(|d| {
            let fences = self.fences.lock().unwrap();
            fences
                .iter()
                .filter(|(k, f)| f.observed(d.data.get(*k), d.last_seq))
                .map(|(k, f)| (k.clone(), *f))
                .collect::<Vec<_>>()
        }) else {
            return None;
        };
        let mut fences = self.fences.lock().unwrap();
        for (k, f) in observed {
            // A write may have replaced the fence since it was checked.
            if fences.get(&k) == Some(&f) {
                fences.remove(&k);
            }
        }
        self.fence_count.store(fences.len(), Ordering::Release);
        fences.values().map(|f| f.since().elapsed()).max()
    }

    /// The cache matched the leader in a read that started at `started`: every write recorded
    /// before then is reflected or superseded, even one whose fence can no longer be observed,
    /// e.g. a key deleted remotely while the stream was down, after which `last_seq` may stay
    /// below the fence.
    pub(crate) fn retire_fences_before(&self, started: Instant) {
        if self.fence_count.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut fences = self.fences.lock().unwrap();
        fences.retain(|_, f| f.since() >= started);
        self.fence_count.store(fences.len(), Ordering::Release);
    }

    /// Key to seq of the live entries, or `None` if the cache is not readable right now.
    pub(crate) fn snapshot(&self, now_ms: u64) -> Option<BTreeMap<String, u64>> {
        match self.reader.try_access_now(|d| {
            d.data
                .iter()
                .filter(|(_, v)| is_live(v, now_ms))
                .map(|(k, v)| (k.clone(), v.seq))
                .collect()
        }) {
            Some(Ok(m)) => Some(m),
            _ => None,
        }
    }

    /// Stop serving reads and make the watcher drop its stream and re-fetch everything.
    pub(crate) fn resync(&self, reason: &str) {
        self.suspect.store(true, Ordering::Release);
        self.counters.resync.fetch_add(1, Ordering::Relaxed);
        warn!(
            "meta read cache {}: {}; serve reads from meta-service and re-fetch the watch stream",
            self.name, reason
        );
        self.reader.request_resync();
    }

    /// A verification matched the leader: serve reads again.
    pub(crate) fn verified(&self) {
        self.mismatches.store(0, Ordering::Relaxed);
        if self.suspect.swap(false, Ordering::AcqRel) {
            info!(
                "meta read cache {}: verified against the leader, serve reads again",
                self.name
            );
        }
    }

    pub(crate) fn is_serving(&self) -> bool {
        !self.suspect.load(Ordering::Acquire)
            && matches!(self.reader.try_access_now(|_| ()), Some(Ok(())))
    }

    pub(crate) fn stats(&self) -> PrefixStats {
        let c = &self.counters;
        PrefixStats {
            hit: c.hit.load(Ordering::Relaxed),
            fallback_not_ready: c.fallback_not_ready.load(Ordering::Relaxed),
            fallback_suspect: c.fallback_suspect.load(Ordering::Relaxed),
            fallback_fence: c.fallback_fence.load(Ordering::Relaxed),
            fallback_absent: c.fallback_absent.load(Ordering::Relaxed),
            resync: c.resync.load(Ordering::Relaxed),
        }
    }

    fn count<T>(&self, counter: &EventCounter) -> Option<T> {
        counter.fetch_add(1, Ordering::Relaxed);
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use databend_meta_client::types::SeqV;

    use super::Fence;
    use super::merge_fence;

    fn present(seq: u64) -> Fence {
        Fence::Present {
            seq,
            since: Instant::now(),
        }
    }

    fn absent(seq: u64) -> Fence {
        Fence::Absent {
            seq,
            since: Instant::now(),
        }
    }

    #[test]
    fn test_merge_keeps_the_latest_write() {
        let mut fences = BTreeMap::new();
        // Seq 11 is recorded first, then the delayed seq 10: 11 stays.
        merge_fence(&mut fences, "k", present(11));
        merge_fence(&mut fences, "k", present(10));
        assert_eq!(fences["k"].position(), (11, false));

        // The delete of value 11 comes after the put of 11.
        merge_fence(&mut fences, "k", absent(11));
        assert_eq!(fences["k"].position(), (11, true));
        merge_fence(&mut fences, "k", present(11));
        assert_eq!(fences["k"].position(), (11, true));

        // A put with a larger seq comes after the delete.
        merge_fence(&mut fences, "k", present(12));
        assert_eq!(fences["k"].position(), (12, false));
    }

    #[test]
    fn test_delete_needs_the_put_applied() {
        let f = absent(10);
        // The cache has not applied the put of 10 yet: it will hold the value later.
        assert!(!f.observed(None, 9));
        assert!(!f.observed(Some(&SeqV::new(10, vec![])), 10));
        assert!(f.observed(None, 10));
        assert!(f.observed(Some(&SeqV::new(12, vec![])), 12));
    }
}
