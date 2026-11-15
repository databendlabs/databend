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
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_meta_client::types::SeqV;

/// Results kept at most; reaching it drops them all. The cached ranges are small, so this only
/// guards against a caller reading unboundedly many distinct keys.
const MAX_ENTRIES: usize = 100_000;

/// A read of meta-service that may be cached.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Read {
    Get(String),
    List(String),
}

/// The result of a [`Read`]: one value for `Get`, the key-values in key order for `List`.
pub(crate) type Items = Vec<(String, SeqV)>;

struct Entry {
    items: Items,
    loaded_at: Instant,
}

/// Results of reads under one key range, each kept for a TTL after it was loaded.
///
/// It knows nothing about what the keys mean. A write under the range drops every result, and
/// the generation keeps a read that started before the write from storing what it loaded.
pub(crate) struct TtlCache {
    /// The cached key range.
    pub(crate) dir: String,
    /// Whether an absent key or an empty list is kept; if not, it is read again every time.
    keep_absent: bool,
    entries: Mutex<HashMap<Read, Entry>>,
    /// Incremented by every write under `dir`.
    generation: AtomicU64,
    hit: AtomicU64,
    miss: AtomicU64,
}

impl TtlCache {
    pub(crate) fn new(dir: String, keep_absent: bool) -> Self {
        TtlCache {
            dir,
            keep_absent,
            entries: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
        }
    }

    /// The result of `read` if it was loaded less than `ttl` ago.
    pub(crate) fn get(&self, read: &Read, ttl: Duration) -> Option<Items> {
        let entries = self.entries.lock().unwrap();
        let found = entries
            .get(read)
            .filter(|e| e.loaded_at.elapsed() < ttl)
            .map(|e| e.items.clone());
        let counter = if found.is_some() {
            &self.hit
        } else {
            &self.miss
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    /// Call before reading meta-service, and pass the result to [`Self::put`].
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Keep `items` as the result of `read`, unless a write happened since `generation`: the
    /// read may have run before that write.
    pub(crate) fn put(&self, read: Read, items: Items, generation: u64) {
        if items.is_empty() && !self.keep_absent {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        // Checked under the lock: `invalidate` increments before it takes the lock to clear.
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        if entries.len() >= MAX_ENTRIES {
            entries.clear();
        }
        entries.insert(read, Entry {
            items,
            loaded_at: Instant::now(),
        });
    }

    /// A write under `dir` completed: drop every result.
    pub(crate) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.entries.lock().unwrap().clear();
    }

    /// Reads answered from memory, and reads that were not.
    pub(crate) fn stats(&self) -> (u64, u64) {
        (
            self.hit.load(Ordering::Relaxed),
            self.miss.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use databend_meta_client::types::SeqV;

    use super::Read;
    use super::TtlCache;

    const TTL: Duration = Duration::from_secs(60);

    fn items(seq: u64) -> Vec<(String, SeqV)> {
        vec![("d/k".to_string(), SeqV::new(seq, vec![]))]
    }

    fn get(key: &str) -> Read {
        Read::Get(key.to_string())
    }

    #[test]
    fn test_kept_for_ttl() {
        let c = TtlCache::new("d/".to_string(), false);
        assert_eq!(c.get(&get("d/k"), TTL), None);

        c.put(get("d/k"), items(1), c.generation());
        assert_eq!(c.get(&get("d/k"), TTL), Some(items(1)));
        // A `Get` and a `List` of the same string are different reads.
        assert_eq!(c.get(&Read::List("d/k".to_string()), TTL), None);
        // Older than the TTL.
        assert_eq!(c.get(&get("d/k"), Duration::ZERO), None);
        assert_eq!(c.stats(), (1, 3));
    }

    #[test]
    fn test_write_drops_results_and_in_flight_reads() {
        let c = TtlCache::new("d/".to_string(), false);
        c.put(get("d/k"), items(1), c.generation());

        // A read starts, then a write completes, then the read stores what it loaded.
        let before_write = c.generation();
        c.invalidate();
        assert_eq!(c.get(&get("d/k"), TTL), None);
        c.put(get("d/k"), items(1), before_write);
        assert_eq!(c.get(&get("d/k"), TTL), None);

        // A read that started after the write is kept.
        c.put(get("d/k"), items(2), c.generation());
        assert_eq!(c.get(&get("d/k"), TTL), Some(items(2)));
    }

    #[test]
    fn test_absent_is_kept_only_if_asked() {
        let c = TtlCache::new("d/".to_string(), false);
        c.put(get("d/k"), vec![], c.generation());
        assert_eq!(c.get(&get("d/k"), TTL), None);

        let c = TtlCache::new("d/".to_string(), true);
        c.put(get("d/k"), vec![], c.generation());
        assert_eq!(c.get(&get("d/k"), TTL), Some(vec![]));
    }
}
