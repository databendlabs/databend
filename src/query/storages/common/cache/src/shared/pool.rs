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
use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
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
/// Smaller for memory pressure: a reclaim stops between two batches once the limit is
/// relieved, and reclaims running at once each finish theirs.
const RECLAIM_BATCH_BYTES: usize = 16 * 1024 * 1024;
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
/// Each kind is a class of the queues, so that its entries can be found without walking the
/// others'. What is in the cache is counted by the queues, under the shard locks; the pool's
/// `Usage` is its budget.
pub(crate) struct PoolStore {
    pool: Pool,
    shards: Box<[RwLock<S3Fifo<EntryKey, Entry>>]>,
    hasher: DefaultHashBuilder,
    cap: AtomicUsize,
    usage: Arc<Usage>,
    cursor: AtomicUsize,
    /// Held for reading by an insert from its first look at the cap and limits until its entry
    /// is in, and for writing while the cap or a kind limit changes: a resize waits for the
    /// inserts that saw the old bounds, and later inserts see the new ones.
    bounds: RwLock<()>,
}

impl PoolStore {
    pub fn new(pool: Pool, cap: usize, shards: usize) -> Self {
        let shards = shards.max(1);
        let config = S3FifoConfig {
            ghost_entries: GHOST_ENTRIES_PER_SHARD,
            ghost_bytes: ghost_bytes_per_shard(cap, shards),
            large_entry_bytes: large_entry_bytes(cap),
            classes: CacheKind::all().count(),
            ..Default::default()
        };
        // One hasher for the pool and its shards: a key is hashed once to pick its shard and to
        // find it there.
        let hasher = DefaultHashBuilder::default();
        PoolStore {
            pool,
            shards: (0..shards)
                .map(|_| RwLock::new(S3Fifo::with_hasher(config, hasher)))
                .collect(),
            hasher,
            cap: AtomicUsize::new(cap),
            usage: Arc::new(Usage::default()),
            cursor: AtomicUsize::new(0),
            bounds: RwLock::new(()),
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

    /// For an insert, from checking the bounds until its entry is in.
    pub fn admitting(&self) -> RwLockReadGuard<'_, ()> {
        self.bounds.read()
    }

    /// For changing the cap or a kind limit, and evicting down to it.
    pub fn resizing(&self) -> RwLockWriteGuard<'_, ()> {
        self.bounds.write()
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

    /// Entries and their bytes in the cache.
    pub fn in_cache(&self) -> (usize, usize) {
        self.sum_shards(|shard| (shard.len(), shard.bytes()))
    }

    /// Entries of `kind` and their bytes in the cache.
    pub fn kind_in_cache(&self, kind: CacheKind) -> (usize, usize) {
        let class = kind.index();
        self.sum_shards(|shard| (shard.class_len(class), shard.class_bytes(class)))
    }

    fn sum_shards(
        &self,
        count: impl Fn(&S3Fifo<EntryKey, Entry>) -> (usize, usize),
    ) -> (usize, usize) {
        self.shards.iter().fold((0, 0), |(items, bytes), shard| {
            let (i, b) = count(&shard.read());
            (items + i, bytes + b)
        })
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
        self.shard_of_hash(self.hasher.hash_one(key))
    }

    /// By the high half of the hash: a shard's table picks buckets by the low bits, which
    /// would otherwise be alike for every key of the shard.
    fn shard_of_hash(&self, hash: u64) -> usize {
        (hash >> 32) as usize % self.shards.len()
    }

    pub fn next_cursor(&self) -> usize {
        self.cursor.fetch_add(1, Ordering::Relaxed) % self.shards.len()
    }

    pub fn get(&self, key: KeyRef) -> Option<ErasedValue> {
        let hash = self.hasher.hash_one(key);
        self.shards[self.shard_of_hash(hash)]
            .read()
            .get_hashed(hash, &key)
            .map(|e| e.value.clone())
    }

    pub fn contains(&self, key: KeyRef) -> bool {
        let hash = self.hasher.hash_one(key);
        self.shards[self.shard_of_hash(hash)]
            .read()
            .contains_hashed(hash, &key)
    }

    /// Caches `entry` on its first insertion: on probation in the small queue, or in the main
    /// queue when the ghost remembers the key or the entry is large. Returns the entry it
    /// replaced. Allocates nothing once the entry is in.
    pub fn insert(&self, key: KeyRef, entry: Entry) -> Option<Entry> {
        let bytes = entry.bytes;
        let owned = key.to_owned();
        self.shards[self.shard_of(key)]
            .write()
            .insert_in(key.kind.index(), owned, entry, bytes)
    }

    pub fn remove(&self, key: KeyRef) -> Option<Entry> {
        self.shards[self.shard_of(key)].write().remove(&key)
    }

    /// Removes every entry of `kind`, held or not.
    pub fn remove_kind(&self, kind: CacheKind) -> Removed {
        let mut removed = Vec::new();
        for shard in self.shards.iter() {
            let taken = shard.write().remove_class(kind.index());
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

    /// Evicts entries of `kind` only, in eviction order, from shard `start` on, until `target`
    /// bytes or a batch; held entries stay. Entries of other kinds keep their place.
    pub fn evict_kind_batch(&self, kind: CacheKind, start: usize, target: usize) -> Removed {
        let target = target.min(BATCH_BYTES);
        let mut victims = Vec::new();
        let mut bytes = 0;
        for step in 0..self.shards.len() {
            let mut shard = self.shards[(start + step) % self.shards.len()].write();
            let taken = shard.evict_class(
                kind.index(),
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

    /// Evicts from shard `start` on, until `target` bytes or a batch; held entries stay.
    pub fn evict_batch(&self, start: usize, target: usize) -> Removed {
        let target = target.min(BATCH_BYTES);
        let mut victims = Vec::new();
        let mut bytes = 0;
        for step in 0..self.shards.len() {
            let mut shard = self.shards[(start + step) % self.shards.len()].write();
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

    /// Evicts a batch for memory pressure, from shard `start` on; held entries stay. Runs
    /// inside allocations, possibly while the allocating thread holds a lock, so it never
    /// waits: a shard locked by another thread is skipped. Looks at `steps` entries at most,
    /// lowering it by those, and leaves the slabs as they are: compacting one allocates.
    pub fn reclaim_batch(&self, start: usize, steps: &mut usize) -> Removed {
        let mut victims = Vec::with_capacity(BATCH_ENTRIES);
        let mut bytes = 0;
        let full = |victims: &Removed, bytes| {
            victims.len() >= BATCH_ENTRIES || bytes >= RECLAIM_BATCH_BYTES
        };
        for step in 0..self.shards.len() {
            let Some(mut shard) = self.shards[(start + step) % self.shards.len()].try_write()
            else {
                continue;
            };
            let mut checked = 0;
            while !full(&victims, bytes) && *steps > 0 {
                let Some((key, entry, size)) =
                    shard.pop_evictable_bounded(Entry::is_shared, CHECKS_PER_POP, steps)
                else {
                    checked += CHECKS_PER_POP;
                    if checked >= shard.len() {
                        break;
                    }
                    continue;
                };
                bytes += size;
                victims.push((key, entry));
            }
            if full(&victims, bytes) || *steps == 0 {
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
        store.insert(KeyRef::new(snapshot, "small"), entry(100));
        assert_eq!(small_bytes(&store), 100);
        // A 64th of the cap or more skips the small queue.
        store.insert(KeyRef::new(snapshot, "large"), entry(1000));
        assert_eq!(small_bytes(&store), 100);

        store.set_cap(640_000);
        store.insert(KeyRef::new(snapshot, "now small"), entry(1000));
        assert_eq!(small_bytes(&store), 1100);
    }

    /// The ghosts follow the cap in bytes: entries evicted from probation are remembered up to
    /// about 0.9 times the cap, and a pool turned off forgets them.
    #[test]
    fn test_ghost_bytes_follow_the_cap() {
        let column = CacheKind::of::<crate::kinds::column::Data>();
        let store = PoolStore::new(Pool::Data, 10_000, 1);
        let key = |i: usize| i.to_string();
        for i in 0..200 {
            store.insert(KeyRef::new(column, &key(i)), entry(100));
        }
        while !store.evict_batch(0, usize::MAX).is_empty() {}
        let structure = store.structure();
        assert_eq!(structure.ghost_byte_capacity, 9000);
        assert_eq!((structure.ghost_entries, structure.ghost_bytes), (90, 9000));
        // A recently evicted key comes back into the main queue, an old one on probation.
        store.insert(KeyRef::new(column, &key(199)), entry(100));
        assert_eq!(small_bytes(&store), 0);
        store.insert(KeyRef::new(column, &key(0)), entry(100));
        assert_eq!(small_bytes(&store), 100);

        store.set_cap(0);
        assert_eq!(store.structure().ghost_bytes, 0);
        store.set_cap(100_000);
        assert_eq!(store.structure().ghost_byte_capacity, 90_000);
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
                store.insert(KeyRef::new(snapshot, &i.to_string()), e);
                value
            })
            .collect();
        store.insert(KeyRef::new(snapshot, "free"), entry(10));
        let victims = store.evict_batch(0, usize::MAX);
        assert_eq!(victims.len(), 1);
        assert!(victims.iter().all(|(key, _)| key.key == "free"));
        store.insert(KeyRef::new(snapshot, "free"), entry(10));
        let victims = store.reclaim_batch(0, &mut { usize::MAX });
        assert_eq!(victims.len(), 1);
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
            store.insert(KeyRef::new(snapshot, &key), entry(10));
            store.insert(KeyRef::new(segment, &key), entry(10));
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
        );
        let guard = store.shards[0].write();
        assert!(store.reclaim_batch(0, &mut { usize::MAX }).is_empty());
        drop(guard);
        assert_eq!(store.reclaim_batch(0, &mut { usize::MAX }).len(), 1);
    }

    /// A reclaim looks at no more entries than its budget, held or not, and spends it.
    #[test]
    fn test_reclaim_is_bounded_by_steps() {
        let snapshot = CacheKind::of::<table::Snapshot>();
        let store = PoolStore::new(Pool::Meta, 1 << 30, 1);
        let held: Vec<ErasedValue> = (0..1000)
            .map(|i| {
                let value: ErasedValue = Arc::new(());
                let e = Entry {
                    value: value.clone(),
                    bytes: 10,
                };
                store.insert(KeyRef::new(snapshot, &i.to_string()), e);
                value
            })
            .collect();
        store.insert(KeyRef::new(snapshot, "free"), entry(10));
        let mut steps = 100;
        assert!(store.reclaim_batch(0, &mut steps).is_empty());
        assert_eq!(steps, 0);
        let mut steps = 10_000;
        assert_eq!(store.reclaim_batch(0, &mut steps).len(), 1);
        assert!(steps > 0);
        assert_eq!(store.structure().entries, held.len());
    }

    /// What is in the cache is counted from the queues, per kind and for the pool.
    #[test]
    fn test_in_cache_counts_follow_the_queues() {
        let (snapshot, segment) = (
            CacheKind::of::<table::Snapshot>(),
            CacheKind::of::<crate::kinds::segment::Info>(),
        );
        let store = PoolStore::new(Pool::Meta, 100_000, 4);
        for i in 0..10 {
            store.insert(KeyRef::new(snapshot, &i.to_string()), entry(10));
            store.insert(KeyRef::new(segment, &i.to_string()), entry(20));
        }
        store.insert(KeyRef::new(segment, "0"), entry(5));
        store.remove(KeyRef::new(snapshot, "1"));
        assert_eq!(store.kind_in_cache(snapshot), (9, 90));
        assert_eq!(store.kind_in_cache(segment), (10, 185));
        assert_eq!(store.in_cache(), (19, 275));
        store.remove_kind(segment);
        assert_eq!(store.in_cache(), (9, 90));
    }

    /// An admission that already read the cap owns the bounds until it publishes. A
    /// disable cannot pass it, and removes its entry before later admissions see cap 0.
    #[test]
    fn test_bounds_cover_publication_and_disable() {
        use std::sync::mpsc;
        use std::time::Duration;

        let store = Arc::new(PoolStore::new(Pool::Meta, 1000, 1));
        let kind = CacheKind::of::<table::Snapshot>();
        let admitting = store.admitting();
        assert_eq!(store.cap(), 1000);
        assert!(store.bounds.try_write().is_none());
        let (started, start) = mpsc::channel();
        let (finished, finish) = mpsc::channel();
        let resized = store.clone();
        let thread = databend_common_base::runtime::Thread::spawn(move || {
            started.send(()).unwrap();
            let _resizing = resized.resizing();
            resized.set_cap(0);
            let removed = resized.remove_all();
            finished.send(removed.len()).unwrap();
        });
        start.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(matches!(finish.try_recv(), Err(mpsc::TryRecvError::Empty)));
        store.insert(KeyRef::new(kind, "old-bounds"), entry(10));
        drop(admitting);
        assert_eq!(finish.recv_timeout(Duration::from_secs(10)).unwrap(), 1);
        thread.join().unwrap();
        let _admitting = store.admitting();
        assert_eq!(store.cap(), 0);
        assert!(!store.contains(KeyRef::new(kind, "old-bounds")));
    }

    /// The shard is picked from the hash its table uses, by other bits: within a shard, the
    /// bits the table picks buckets by still differ between keys.
    #[test]
    fn test_shard_bits_leave_the_table_bits_free() {
        let snapshot = CacheKind::of::<table::Snapshot>();
        let store = PoolStore::new(Pool::Meta, 1 << 30, 16);
        let keys: Vec<String> = (0..4096).map(|i| i.to_string()).collect();
        let mut low_bits = vec![std::collections::HashSet::new(); 16];
        for key in &keys {
            let key = KeyRef::new(snapshot, key);
            let hash = store.hasher.hash_one(key);
            assert_eq!(hash, store.shards[0].read().hash_of(&key));
            low_bits[store.shard_of(key)].insert(hash & 0xff);
            store.insert(key, entry(1));
        }
        let distinct: Vec<usize> = low_bits.iter().map(|bits| bits.len()).collect();
        assert!(distinct.iter().all(|n| *n > 64), "{distinct:?}");
        assert!(
            keys.iter()
                .all(|k| store.get(KeyRef::new(snapshot, k)).is_some())
        );
    }

    #[test]
    fn test_eviction_stops_at_target() {
        let store = PoolStore::new(Pool::Meta, 1000, 2);
        for i in 0..10 {
            let key = i.to_string();
            store.insert(
                KeyRef::new(CacheKind::of::<table::Snapshot>(), &key),
                entry(10),
            );
        }
        assert_eq!(store.evict_batch(0, 25).len(), 3);
    }
}
