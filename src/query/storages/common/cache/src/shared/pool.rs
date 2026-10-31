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

use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_cache::DefaultHashBuilder;
use databend_common_cache::Placement;
use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;
use parking_lot::RwLock;
use parking_lot::RwLockWriteGuard;

use crate::CacheKind;
use crate::Pool;
use crate::backend::entry::Entry;
use crate::backend::entry::EntryKey;
use crate::backend::entry::KeyRef;
use crate::handle::cache_ref::ErasedValue;
use crate::handle::cache_ref::Usage;

const BATCH_ENTRIES: usize = 64;
const BATCH_BYTES: usize = 64 * 1024 * 1024;
const CHECKS_PER_POP: usize = 32;
const GHOST_ENTRIES_PER_SHARD: usize = 16 * 1024;

pub(crate) type Removed = Vec<(EntryKey, Entry)>;

/// Sizes of a pool's internal structures, for checking that they stay bounded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStructure {
    pub shards: usize,
    pub entries: usize,
    /// Queue positions, including stale ones not compacted yet.
    pub queue_positions: usize,
    pub ghost_entries: usize,
    pub ghost_capacity: usize,
}

/// One pool: sharded S3-FIFO queues under a single byte cap. Shards only spread lock contention.
pub(crate) struct PoolStore {
    pool: Pool,
    shards: Box<[RwLock<S3Fifo<EntryKey, Entry>>]>,
    hasher: DefaultHashBuilder,
    cap: AtomicUsize,
    usage: Arc<Usage>,
    cursor: AtomicUsize,
}

impl PoolStore {
    pub fn new(pool: Pool, cap: usize, shards: usize) -> Self {
        let config = S3FifoConfig {
            admit_after: if pool == Pool::Data { 2 } else { 1 },
            ghost_entries: GHOST_ENTRIES_PER_SHARD,
            large_entry_bytes: (cap / 64).max(1),
            ..Default::default()
        };
        PoolStore {
            pool,
            shards: (0..shards.max(1))
                .map(|_| RwLock::new(S3Fifo::new(config)))
                .collect(),
            hasher: DefaultHashBuilder::default(),
            cap: AtomicUsize::new(cap),
            usage: Arc::new(Usage::default()),
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn pool(&self) -> Pool {
        self.pool
    }

    pub fn usage(&self) -> &Arc<Usage> {
        &self.usage
    }

    pub fn cap(&self) -> usize {
        self.cap.load(Ordering::Relaxed)
    }

    pub fn set_cap(&self, bytes: usize) {
        self.cap.store(bytes, Ordering::Relaxed);
    }

    pub fn structure(&self) -> PoolStructure {
        let mut structure = PoolStructure {
            shards: self.shards.len(),
            ghost_capacity: GHOST_ENTRIES_PER_SHARD * self.shards.len(),
            ..PoolStructure::default()
        };
        for shard in self.shards.iter() {
            let shard = shard.read();
            structure.entries += shard.len();
            structure.queue_positions += shard.queue_len();
            structure.ghost_entries += shard.ghost_len();
        }
        structure
    }

    pub fn shard_of(&self, key: KeyRef) -> usize {
        self.hasher.hash_one(key) as usize % self.shards.len()
    }

    pub fn next_cursor(&self) -> usize {
        self.cursor.fetch_add(1, Ordering::Relaxed) % self.shards.len()
    }

    pub fn get(&self, key: KeyRef) -> Option<ErasedValue> {
        self.shards[self.shard_of(key)]
            .read()
            .get(&key)
            .map(|e| e.value.clone())
    }

    pub fn contains(&self, key: KeyRef) -> bool {
        self.shards[self.shard_of(key)].read().contains(&key)
    }

    pub fn admission(&self, key: KeyRef) -> Option<Placement> {
        let mut shard = self.shards[self.shard_of(key)].write();
        if shard.contains(&key) {
            return Some(Placement::Small);
        }
        shard.admission(&key.to_owned())
    }

    /// `placement: None` bypasses admission.
    pub fn insert(&self, key: KeyRef, entry: Entry, placement: Option<Placement>) -> Removed {
        let bytes = entry.bytes;
        let owned = key.to_owned();
        let mut shard = self.shards[self.shard_of(key)].write();
        let replaced = match placement {
            Some(placement) => shard.insert_at(owned, entry, bytes, placement),
            None => shard.insert(owned, entry, bytes),
        };
        replaced.map(|e| (key.to_owned(), e)).into_iter().collect()
    }

    pub fn remove(&self, key: KeyRef) -> Removed {
        let removed = self.shards[self.shard_of(key)].write().remove(&key);
        removed.map(|e| (key.to_owned(), e)).into_iter().collect()
    }

    /// Removes entries of `kind`; with `excess`, stops once that many bytes are removed.
    pub fn remove_kind(&self, kind: CacheKind, excess: Option<usize>) -> Removed {
        let mut left = excess.unwrap_or(usize::MAX);
        let mut removed = Vec::new();
        for shard in self.shards.iter() {
            if left == 0 {
                break;
            }
            let taken = shard.write().remove_if(|k, e| {
                if k.kind != kind || left == 0 {
                    return false;
                }
                left = left.saturating_sub(e.bytes);
                true
            });
            removed.extend(taken.into_iter().map(|(k, e, _)| (k, e)));
        }
        removed
    }

    pub fn remove_all(&self) -> Removed {
        let mut removed = Vec::new();
        for shard in self.shards.iter() {
            let taken = shard.write().remove_if(|_, _| true);
            removed.extend(taken.into_iter().map(|(k, e, _)| (k, e)));
        }
        removed
    }

    /// Pops unreferenced entries until `target` bytes or one batch, visiting shards from `start`.
    /// With `blocking == false`, busy shards are skipped.
    pub fn evict_batch(&self, start: usize, target: usize, blocking: bool) -> Removed {
        let target = target.min(BATCH_BYTES);
        let mut victims = Vec::new();
        let mut bytes = 0;
        for step in 0..self.shards.len() {
            let lock = &self.shards[(start + step) % self.shards.len()];
            let Some(mut shard) = lock_shard(lock, blocking) else {
                continue;
            };
            while victims.len() < BATCH_ENTRIES && bytes < target {
                let Some((key, entry, size)) =
                    shard.pop_evictable(Entry::is_shared, CHECKS_PER_POP)
                else {
                    break;
                };
                bytes += size;
                victims.push((key, entry));
            }
            if victims.len() >= BATCH_ENTRIES || bytes >= target {
                break;
            }
        }
        victims
    }
}

fn lock_shard<T>(lock: &RwLock<T>, blocking: bool) -> Option<RwLockWriteGuard<'_, T>> {
    if blocking {
        Some(lock.write())
    } else {
        lock.try_write()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kinds::table;

    fn entry(bytes: usize) -> Entry {
        Entry {
            value: Arc::new(()),
            bytes,
        }
    }

    #[test]
    fn test_non_blocking_eviction_skips_locked_shards() {
        let store = PoolStore::new(Pool::Meta, 1000, 1);
        store.insert(
            KeyRef::new(CacheKind::of::<table::Snapshot>(), "a"),
            entry(10),
            None,
        );
        let guard = store.shards[0].write();
        assert!(store.evict_batch(0, usize::MAX, false).is_empty());
        drop(guard);
        assert_eq!(store.evict_batch(0, usize::MAX, false).len(), 1);
    }

    #[test]
    fn test_eviction_stops_at_target() {
        let store = PoolStore::new(Pool::Meta, 1000, 2);
        for i in 0..10 {
            let key = i.to_string();
            store.insert(
                KeyRef::new(CacheKind::of::<table::Snapshot>(), &key),
                entry(10),
                None,
            );
        }
        assert_eq!(store.evict_batch(0, 25, true).len(), 3);
    }
}
