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
    /// Slab slots, including free ones kept for reuse.
    pub slots: usize,
    pub ghost_entries: usize,
    pub ghost_capacity: usize,
    /// Bytes of the entries the ghosts remember, and their bound summed over the shards.
    pub ghost_bytes: usize,
    pub ghost_byte_capacity: usize,
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
        let shards = shards.max(1);
        let config = S3FifoConfig {
            admit_after: if pool == Pool::Data { 2 } else { 1 },
            ghost_entries: GHOST_ENTRIES_PER_SHARD,
            ghost_bytes: ghost_bytes_per_shard(cap, shards),
            large_entry_bytes: large_entry_bytes(cap),
            ..Default::default()
        };
        PoolStore {
            pool,
            shards: (0..shards)
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
        let ghost_bytes = ghost_bytes_per_shard(bytes, self.shards.len());
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.set_large_entry_bytes(large_entry_bytes(bytes));
            shard.set_ghost_bytes(ghost_bytes);
        }
    }

    pub fn structure(&self) -> PoolStructure {
        let mut structure = PoolStructure {
            shards: self.shards.len(),
            ghost_capacity: GHOST_ENTRIES_PER_SHARD * self.shards.len(),
            ghost_byte_capacity: ghost_bytes_per_shard(self.cap(), self.shards.len())
                * self.shards.len(),
            ..PoolStructure::default()
        };
        for shard in self.shards.iter() {
            let shard = shard.read();
            structure.entries += shard.len();
            structure.slots += shard.slots();
            structure.ghost_entries += shard.ghost_len();
            structure.ghost_bytes += shard.ghost_bytes();
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

    /// Admission of a value of `bytes` for `key`; see `S3Fifo::admission`.
    pub fn admission(&self, key: KeyRef, bytes: usize) -> Option<Placement> {
        let mut shard = self.shards[self.shard_of(key)].write();
        if shard.contains(&key) {
            return Some(Placement::Small);
        }
        shard.admission(&key.to_owned(), bytes)
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
    /// Evicts entries of `kind` only, in eviction order, from shard `start` on, until `target`
    /// bytes or a batch; held entries stay. Entries of other kinds keep their place.
    pub fn evict_kind_batch(&self, kind: CacheKind, start: usize, target: usize) -> Removed {
        let target = target.min(BATCH_BYTES);
        let mut victims = Vec::new();
        let mut bytes = 0;
        for step in 0..self.shards.len() {
            let mut shard = self.shards[(start + step) % self.shards.len()].write();
            let taken = shard.evict_where(
                |k| k.kind == kind,
                Entry::is_shared,
                target - bytes,
                BATCH_ENTRIES - victims.len(),
            );
            for (key, entry, size) in taken {
                bytes += size;
                victims.push((key, entry));
            }
            if victims.len() >= BATCH_ENTRIES || bytes >= target {
                break;
            }
        }
        victims
    }

    pub fn evict_batch(&self, start: usize, target: usize, blocking: bool) -> Removed {
        let target = target.min(BATCH_BYTES);
        let mut victims = Vec::new();
        let mut bytes = 0;
        for step in 0..self.shards.len() {
            let lock = &self.shards[(start + step) % self.shards.len()];
            let Some(mut shard) = lock_shard(lock, blocking) else {
                continue;
            };
            // A pop gives up after `CHECKS_PER_POP` held entries, rotated to the back; the
            // shard is done only once as many checks as it has entries came back empty.
            let mut checked = 0;
            while victims.len() < BATCH_ENTRIES && bytes < target {
                let Some((key, entry, size)) =
                    shard.pop_evictable(Entry::is_shared, CHECKS_PER_POP)
                else {
                    checked += CHECKS_PER_POP;
                    if checked >= shard.len() {
                        break;
                    }
                    // Let readers in between two scans of a shard full of held entries.
                    RwLockWriteGuard::bump(&mut shard);
                    continue;
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

/// Entries this large skip the small queue: a 64th of the cap. Follows the cap, so a pool that
/// starts off (cap 0) does not later send every entry past probation.
fn large_entry_bytes(cap: usize) -> usize {
    match cap / 64 {
        0 => usize::MAX,
        bytes => bytes,
    }
}

/// The ghosts of a pool remember about 0.9 times its cap in bytes, as libCacheSim's default
/// ghost ratio, split over the shards. A record larger than its shard's share is still kept
/// until the next one comes; a pool that is off (cap 0) remembers nothing.
fn ghost_bytes_per_shard(cap: usize, shards: usize) -> usize {
    ((cap - cap / 10) / shards).max(cap.min(1))
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

    fn small_bytes(store: &PoolStore) -> usize {
        store.shards.iter().map(|s| s.read().small_bytes()).sum()
    }

    /// A pool that starts off and is sized later still puts new entries on probation, and the
    /// large-entry threshold follows the cap.
    #[test]
    fn test_large_entry_threshold_follows_the_cap() {
        let snapshot = CacheKind::of::<table::Snapshot>();
        let store = PoolStore::new(Pool::Meta, 0, 1);
        store.set_cap(64_000);
        store.insert(KeyRef::new(snapshot, "small"), entry(100), None);
        assert_eq!(small_bytes(&store), 100);
        // A 64th of the cap or more skips the small queue.
        store.insert(KeyRef::new(snapshot, "large"), entry(1000), None);
        assert_eq!(small_bytes(&store), 100);

        store.set_cap(640_000);
        store.insert(KeyRef::new(snapshot, "now small"), entry(1000), None);
        assert_eq!(small_bytes(&store), 1100);
    }

    /// The ghosts follow the cap in bytes: deferred values of a pool are remembered up to
    /// about 0.9 times its cap, and a pool turned off forgets them.
    #[test]
    fn test_ghost_bytes_follow_the_cap() {
        let column = CacheKind::of::<crate::kinds::column::Data>();
        let store = PoolStore::new(Pool::Data, 10_000, 1);
        // First reads are deferred and recorded, 1000 bytes each.
        for i in 0..20 {
            let key = i.to_string();
            assert!(store.admission(KeyRef::new(column, &key), 1000).is_none());
        }
        let structure = store.structure();
        assert_eq!(structure.ghost_byte_capacity, 9000);
        assert_eq!((structure.ghost_entries, structure.ghost_bytes), (9, 9000));
        // The newest are remembered: a second read is admitted, an old one deferred again.
        assert!(store.admission(KeyRef::new(column, "19"), 1000).is_some());
        assert!(store.admission(KeyRef::new(column, "0"), 1000).is_none());

        store.set_cap(0);
        assert_eq!(store.structure().ghost_bytes, 0);
        store.set_cap(100_000);
        assert_eq!(store.structure().ghost_byte_capacity, 90_000);
        assert!(store.admission(KeyRef::new(column, "a"), 50_000).is_none());
        assert!(store.admission(KeyRef::new(column, "a"), 50_000).is_some());
    }

    /// A value larger than its shard's ghost share is still admitted on a prompt second read.
    #[test]
    fn test_large_value_is_admitted_on_second_read() {
        let column = CacheKind::of::<crate::kinds::column::Data>();
        let store = PoolStore::new(Pool::Data, 10_000, 4);
        let key = KeyRef::new(column, "large");
        assert!(store.admission(key, 2500).is_none());
        assert!(store.admission(key, 2500).is_some());
    }

    /// Held entries at the front of a shard do not end its eviction early.
    #[test]
    fn test_eviction_scans_past_held_entries() {
        let snapshot = CacheKind::of::<table::Snapshot>();
        let store = PoolStore::new(Pool::Meta, 100_000, 1);
        let held: Vec<ErasedValue> = (0..CHECKS_PER_POP * 3)
            .map(|i| {
                let value: ErasedValue = Arc::new(());
                let e = Entry {
                    value: value.clone(),
                    bytes: 10,
                };
                store.insert(KeyRef::new(snapshot, &i.to_string()), e, None);
                value
            })
            .collect();
        store.insert(KeyRef::new(snapshot, "free"), entry(10), None);
        for blocking in [true, false] {
            let victims = store.evict_batch(0, usize::MAX, blocking);
            assert_eq!(victims.len(), usize::from(blocking), "blocking {blocking}");
            assert!(victims.iter().all(|(key, _)| key.key == "free"));
        }
        assert_eq!(store.structure().entries, held.len());
    }

    /// Only entries of the kind leave, coldest first; others keep their place.
    #[test]
    fn test_kind_eviction_leaves_other_kinds() {
        let (snapshot, segment) = (
            CacheKind::of::<table::Snapshot>(),
            CacheKind::of::<crate::kinds::segment::Info>(),
        );
        let store = PoolStore::new(Pool::Meta, 100_000, 2);
        for i in 0..10 {
            let key = i.to_string();
            store.insert(KeyRef::new(snapshot, &key), entry(10), None);
            store.insert(KeyRef::new(segment, &key), entry(10), None);
        }
        let victims = store.evict_kind_batch(snapshot, 0, 30);
        assert_eq!(victims.len(), 3);
        assert!(victims.iter().all(|(key, _)| key.kind == snapshot));
        assert!((0..10).all(|i| store.contains(KeyRef::new(segment, &i.to_string()))));
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
