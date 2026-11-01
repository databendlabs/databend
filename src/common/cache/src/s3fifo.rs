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
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use hashbrown::DefaultHashBuilder;
use hashbrown::Equivalent;
use hashbrown::HashTable;

use crate::ghost::Ghost;

const MAX_FREQ: u8 = 3;

#[derive(Clone, Copy, Debug)]
pub struct S3FifoConfig {
    /// Share of bytes kept in the small (probation) queue.
    pub small_ratio: f64,
    /// Bounds of the ghost: records, and bytes of the entries they stand for.
    pub ghost_entries: usize,
    pub ghost_bytes: usize,
    /// Entries at least this large skip the small queue.
    pub large_entry_bytes: usize,
    /// Classes entries belong to, e.g. kinds sharing one cache, numbered from 0. Each class
    /// keeps its own entries in queue order as well, so `evict_class` walks that class only.
    pub classes: usize,
}

impl Default for S3FifoConfig {
    fn default() -> Self {
        S3FifoConfig {
            small_ratio: 0.1,
            ghost_entries: 65536,
            ghost_bytes: usize::MAX,
            large_entry_bytes: usize::MAX,
            classes: 1,
        }
    }
}

const NIL: usize = usize::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Queue {
    Small = 0,
    Main = 1,
}

/// The two lists an entry is linked into: its queue, and its class's share of that queue.
const QUEUE: usize = 0;
const CLASS: usize = 1;

#[derive(Clone, Copy)]
struct Link {
    prev: usize,
    next: usize,
}

impl Link {
    const NONE: Link = Link {
        prev: NIL,
        next: NIL,
    };
}

struct Node<K, V> {
    key: K,
    value: V,
    hash: u64,
    bytes: usize,
    freq: AtomicU8,
    queue: Queue,
    class: usize,
    /// Indexed by `QUEUE` and `CLASS`.
    links: [Link; 2],
}

/// A doubly linked list threaded through the slots.
#[derive(Clone, Copy)]
struct List {
    head: usize,
    tail: usize,
    len: usize,
    bytes: usize,
}

impl List {
    const EMPTY: List = List {
        head: NIL,
        tail: NIL,
        len: 0,
        bytes: 0,
    };
}

/// S3-FIFO eviction order without a capacity of its own: the owner decides when to evict.
///
/// Entries live in a slab of slots, linked into the small and the main queue by slot index,
/// as in quick_cache's linked slab: removing or moving an entry is O(1) and leaves nothing
/// behind. Each entry is also linked into its class's list of its queue, in the same order.
/// Hits only bump an atomic counter, so lookups need `&self`.
///
/// Changes allocate, if at all, before they modify anything: an allocation failure, which
/// panics in Databend, leaves the structure consistent. An operation on several entries may
/// stop between two of them. Removals and evictions do not allocate, except for the entries
/// they return and the ghost, which gives up a record rather than fail; that keeps them usable
/// under memory pressure. The free list has room for every slot, reserved as the slab grows.
pub struct S3Fifo<K, V, S = DefaultHashBuilder> {
    /// Slot index of each entry, found by the entry's hash and key.
    table: HashTable<usize>,
    slots: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    small: List,
    main: List,
    /// Per class, indexed by `Queue`.
    class_lists: Box<[[List; 2]]>,
    ghost: Ghost,
    hasher: S,
    config: S3FifoConfig,
}

impl<K: Hash + Eq, V> S3Fifo<K, V> {
    pub fn new(config: S3FifoConfig) -> Self {
        Self::with_hasher(config, DefaultHashBuilder::default())
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> S3Fifo<K, V, S> {
    pub fn with_hasher(config: S3FifoConfig, hasher: S) -> Self {
        S3Fifo {
            table: HashTable::new(),
            slots: Vec::new(),
            free: Vec::new(),
            small: List::EMPTY,
            main: List::EMPTY,
            class_lists: vec![[List::EMPTY; 2]; config.classes.max(1)].into_boxed_slice(),
            ghost: Ghost::new(config.ghost_entries, config.ghost_bytes),
            hasher,
            config,
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        self.get_hashed(self.hasher.hash_one(key), key)
    }

    /// The hash the lookups by hash take, for an owner that hashes a key once, e.g. to pick a
    /// shard too. Built from the hasher given to `with_hasher`.
    pub fn hash_of<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hasher.hash_one(key)
    }

    /// `get` by `hash`, the key's `hash_of`.
    pub fn get_hashed<Q>(&self, hash: u64, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        debug_assert_eq!(hash, self.hasher.hash_one(key), "hash of another hasher");
        let node = self.node(self.find_hashed(hash, key)?);
        let freq = node.freq.load(Ordering::Relaxed);
        if freq < MAX_FREQ {
            node.freq.store(freq + 1, Ordering::Relaxed);
        }
        Some(&node.value)
    }

    pub fn peek<Q>(&self, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        self.find(key).map(|i| &self.node(i).value)
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where Q: Hash + Equivalent<K> + ?Sized {
        self.find(key).is_some()
    }

    /// `contains` by `hash`, the key's `hash_of`.
    pub fn contains_hashed<Q>(&self, hash: u64, key: &Q) -> bool
    where Q: Hash + Equivalent<K> + ?Sized {
        debug_assert_eq!(hash, self.hasher.hash_one(key), "hash of another hasher");
        self.find_hashed(hash, key).is_some()
    }

    /// `insert_in` class 0.
    pub fn insert(&mut self, key: K, value: V, bytes: usize) -> Option<V> {
        self.insert_in(0, key, value, bytes)
    }

    /// Caches `value` under `key` in `class`, on its first insertion: on probation in the small
    /// queue, or in the main queue when the ghost remembers the key (evicted from probation
    /// recently) or the entry is large. An existing entry keeps its position and its class.
    /// Returns the replaced value.
    pub fn insert_in(&mut self, class: usize, key: K, value: V, bytes: usize) -> Option<V> {
        if let Some(i) = self.find(&key) {
            let (queue, class, old) = {
                let node = self.node(i);
                (node.queue, node.class, node.bytes)
            };
            for which in [QUEUE, CLASS] {
                let list = self.list_mut(which, queue, class);
                list.bytes = list.bytes - old + bytes;
            }
            let node = self.node_mut(i);
            node.bytes = bytes;
            return Some(std::mem::replace(&mut node.value, value));
        }
        assert!(
            class < self.class_lists.len(),
            "class {class} of {}",
            self.class_lists.len()
        );
        let hash = self.hasher.hash_one(&key);
        let slots = &self.slots;
        self.table.reserve(1, |&j| Self::slot_hash(slots, j));
        if self.free.is_empty() {
            self.slots.reserve(1);
            self.free.reserve(self.slots.capacity());
        }
        let in_main = self.ghost.take(hash) || bytes >= self.config.large_entry_bytes;
        let i = self.alloc(Node {
            key,
            value,
            hash,
            bytes,
            freq: AtomicU8::new(0),
            queue: if in_main { Queue::Main } else { Queue::Small },
            class,
            links: [Link::NONE; 2],
        });
        let slots = &self.slots;
        self.table
            .insert_unique(hash, i, |&j| Self::slot_hash(slots, j));
        self.link_back(i);
        None
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where Q: Hash + Equivalent<K> + ?Sized {
        let i = self.find(key)?;
        let node = self.take(i);
        self.compact();
        Some(node.value)
    }

    /// Pops the next victim, rotating entries for which `keep` returns true.
    /// Gives up after `max_checks` entries were kept. Aging recently accessed entries (moving
    /// them from small to main, or lowering their frequency) does not count: it is bounded by
    /// `MAX_FREQ` per entry, and counting it could report no victim while every entry is
    /// evictable.
    pub fn pop_evictable(
        &mut self,
        keep: impl Fn(&V) -> bool,
        max_checks: usize,
    ) -> Option<(K, V, usize)> {
        let mut unbounded = usize::MAX;
        let victim = self.pop(keep, max_checks, &mut unbounded, true);
        self.compact();
        victim
    }

    /// `pop_evictable` for memory pressure: also gives up once it looked at `steps` entries,
    /// kept, aged or evicted, lowering `steps` by those, a budget the caller may share between
    /// pops. Does not allocate: neither compacts the slab nor updates the ghost. Pressure
    /// eviction need not remember probation entries at the cost of growing or scanning it.
    pub fn pop_evictable_bounded(
        &mut self,
        keep: impl Fn(&V) -> bool,
        max_checks: usize,
        steps: &mut usize,
    ) -> Option<(K, V, usize)> {
        self.pop(keep, max_checks, steps, false)
    }

    fn pop(
        &mut self,
        keep: impl Fn(&V) -> bool,
        max_checks: usize,
        steps: &mut usize,
        record_ghost: bool,
    ) -> Option<(K, V, usize)> {
        let mut checks = 0;
        let mut flip = false;
        while checks < max_checks && *steps > 0 {
            *steps -= 1;
            let from_small = self.pick_small(flip);
            let i = if from_small {
                self.small.head
            } else {
                self.main.head
            };
            if i == NIL {
                return None;
            }
            let node = self.node(i);
            let freq = node.freq.load(Ordering::Relaxed);
            if from_small {
                let kept = freq == 0 && keep(&node.value);
                if freq > 0 || kept {
                    checks += usize::from(kept);
                    node.freq.store(0, Ordering::Relaxed);
                    self.move_to_main(i);
                    continue;
                }
                if record_ghost {
                    self.ghost.record(node.hash, node.bytes);
                }
            } else if freq > 0 {
                node.freq.store(freq - 1, Ordering::Relaxed);
                self.move_to_main(i);
                continue;
            } else if keep(&node.value) {
                checks += 1;
                self.move_to_main(i);
                flip = !flip;
                continue;
            }
            let node = self.take(i);
            return Some((node.key, node.value, node.bytes));
        }
        None
    }

    /// Evicts entries of `class`, in eviction order, until `target` bytes or `max_entries`
    /// entries are gone; entries for which `keep` returns true stay. They are aged as by
    /// `pop_evictable`, clock-style: an accessed entry in the small queue, or an entry kept,
    /// moves to the back of the main queue; an accessed entry in the main queue loses one
    /// access and moves to its back. Entries of other classes keep their place and their
    /// access counts.
    ///
    /// Walks the class's small queue once and its main queue up to `MAX_FREQ + 1` times, each
    /// pass over the entries it started with: the cost is in entries of the class only.
    pub fn evict_class(
        &mut self,
        class: usize,
        keep: impl Fn(&V) -> bool,
        target: usize,
        max_entries: usize,
    ) -> Vec<(K, V, usize)> {
        let mut victims = Vec::with_capacity(max_entries.min(self.class_len(class)));
        let mut bytes = 0;
        // A main pass lowers each access count by one, so enough passes reach every entry
        // that can be evicted.
        let passes = std::iter::once(Queue::Small)
            .chain(std::iter::repeat_n(Queue::Main, MAX_FREQ as usize + 1));
        'passes: for queue in passes {
            let list = self.class_lists[class][queue as usize];
            let mut i = list.head;
            for _ in 0..list.len {
                if victims.len() >= max_entries || bytes >= target {
                    break 'passes;
                }
                let node = self.node(i);
                let next = node.links[CLASS].next;
                let freq = node.freq.load(Ordering::Relaxed);
                if freq > 0 || keep(&node.value) {
                    let aged = match queue {
                        Queue::Small => 0,
                        Queue::Main => freq.saturating_sub(1),
                    };
                    node.freq.store(aged, Ordering::Relaxed);
                    self.move_to_main(i);
                } else {
                    if queue == Queue::Small {
                        self.ghost.record(node.hash, node.bytes);
                    }
                    let node = self.take(i);
                    bytes += node.bytes;
                    victims.push((node.key, node.value, node.bytes));
                }
                i = next;
            }
        }
        self.compact();
        victims
    }

    /// Removes every entry of `class`.
    pub fn remove_class(&mut self, class: usize) -> Vec<(K, V, usize)> {
        let mut removed = Vec::with_capacity(self.class_len(class));
        for queue in [Queue::Small, Queue::Main] {
            let mut i = self.class_lists[class][queue as usize].head;
            while i != NIL {
                let next = self.node(i).links[CLASS].next;
                let node = self.take(i);
                removed.push((node.key, node.value, node.bytes));
                i = next;
            }
        }
        self.compact();
        removed
    }

    pub fn remove_if(&mut self, mut pred: impl FnMut(&K, &V) -> bool) -> Vec<(K, V, usize)> {
        let matched: Vec<usize> = (0..self.slots.len())
            .filter(|&i| {
                self.slots[i]
                    .as_ref()
                    .is_some_and(|node| pred(&node.key, &node.value))
            })
            .collect();
        let mut removed = Vec::with_capacity(matched.len());
        for i in matched {
            let node = self.take(i);
            removed.push((node.key, node.value, node.bytes));
        }
        self.compact();
        removed
    }

    /// Entries at least this large skip the small queue from now on, e.g. after the owner's
    /// capacity changed.
    pub fn set_large_entry_bytes(&mut self, bytes: usize) {
        self.config.large_entry_bytes = bytes;
    }

    /// Bytes of the entries in the small (probation) queue.
    pub fn small_bytes(&self) -> usize {
        self.small.bytes
    }

    /// Slots allocated for entries, including free ones kept for reuse.
    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.slots.iter().flatten().map(|n| (&n.key, &n.value))
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.small.bytes + self.main.bytes
    }

    pub fn class_len(&self, class: usize) -> usize {
        let [small, main] = &self.class_lists[class];
        small.len + main.len
    }

    pub fn class_bytes(&self, class: usize) -> usize {
        let [small, main] = &self.class_lists[class];
        small.bytes + main.bytes
    }

    pub fn ghost_len(&self) -> usize {
        self.ghost.len()
    }

    /// Bytes of the entries the ghost remembers.
    pub fn ghost_bytes(&self) -> usize {
        self.ghost.bytes()
    }

    /// Changes the ghost's byte bound, e.g. after the owner's capacity changed.
    pub fn set_ghost_bytes(&mut self, bytes: usize) {
        self.config.ghost_bytes = bytes;
        self.ghost.set_byte_capacity(bytes);
    }

    fn pick_small(&self, flip: bool) -> bool {
        match (self.small.len, self.main.len) {
            (0, _) => false,
            (_, 0) => true,
            _ => {
                let over = self.small.bytes as f64 > self.bytes() as f64 * self.config.small_ratio;
                over != flip
            }
        }
    }

    fn find<Q>(&self, key: &Q) -> Option<usize>
    where Q: Hash + Equivalent<K> + ?Sized {
        self.find_hashed(self.hasher.hash_one(key), key)
    }

    fn find_hashed<Q>(&self, hash: u64, key: &Q) -> Option<usize>
    where Q: Equivalent<K> + ?Sized {
        self.table
            .find(hash, |&i| key.equivalent(&self.node(i).key))
            .copied()
    }

    fn node(&self, i: usize) -> &Node<K, V> {
        self.slots[i].as_ref().expect("linked slot holds an entry")
    }

    fn node_mut(&mut self, i: usize) -> &mut Node<K, V> {
        self.slots[i].as_mut().expect("linked slot holds an entry")
    }

    fn slot_hash(slots: &[Option<Node<K, V>>], i: usize) -> u64 {
        slots[i].as_ref().map_or(0, |node| node.hash)
    }

    /// The `QUEUE` list of `queue`, or `class`'s `CLASS` list of it.
    fn list_mut(&mut self, which: usize, queue: Queue, class: usize) -> &mut List {
        match (which, queue) {
            (QUEUE, Queue::Small) => &mut self.small,
            (QUEUE, Queue::Main) => &mut self.main,
            _ => &mut self.class_lists[class][queue as usize],
        }
    }

    /// Fills a free slot, or a new one within the capacity reserved by the caller.
    fn alloc(&mut self, node: Node<K, V>) -> usize {
        match self.free.pop() {
            Some(i) => {
                self.slots[i] = Some(node);
                i
            }
            None => {
                self.slots.push(Some(node));
                self.slots.len() - 1
            }
        }
    }

    /// Unlinks and frees slot `i`, dropping it from the table. Does not allocate: the free
    /// list has room for every slot.
    fn take(&mut self, i: usize) -> Node<K, V> {
        debug_assert!(self.free.len() < self.free.capacity());
        self.unlink(i);
        let hash = self.node(i).hash;
        if let Ok(entry) = self.table.find_entry(hash, |&j| j == i) {
            entry.remove();
        }
        self.free.push(i);
        self.slots[i].take().expect("linked slot holds an entry")
    }

    fn link_back(&mut self, i: usize) {
        let (queue, class, bytes) = {
            let node = self.node(i);
            (node.queue, node.class, node.bytes)
        };
        for which in [QUEUE, CLASS] {
            let tail = self.list_mut(which, queue, class).tail;
            self.node_mut(i).links[which] = Link {
                prev: tail,
                next: NIL,
            };
            if tail == NIL {
                self.list_mut(which, queue, class).head = i;
            } else {
                self.node_mut(tail).links[which].next = i;
            }
            let list = self.list_mut(which, queue, class);
            list.tail = i;
            list.len += 1;
            list.bytes += bytes;
        }
    }

    fn unlink(&mut self, i: usize) {
        let (queue, class, bytes, links) = {
            let node = self.node(i);
            (node.queue, node.class, node.bytes, node.links)
        };
        for which in [QUEUE, CLASS] {
            let Link { prev, next } = links[which];
            if prev == NIL {
                self.list_mut(which, queue, class).head = next;
            } else {
                self.node_mut(prev).links[which].next = next;
            }
            if next == NIL {
                self.list_mut(which, queue, class).tail = prev;
            } else {
                self.node_mut(next).links[which].prev = prev;
            }
            let list = self.list_mut(which, queue, class);
            list.len -= 1;
            list.bytes -= bytes;
        }
    }

    /// Moves entry `i` to the back of the main queue.
    fn move_to_main(&mut self, i: usize) {
        self.unlink(i);
        self.node_mut(i).queue = Queue::Main;
        self.link_back(i);
    }

    /// Free slots are reused, so the slab only grows to the most entries held at once. Once
    /// free slots outnumber the entries, rebuilds the slab in queue order and releases the
    /// rest, so it stays within twice the entries (plus a constant) after the entries shrink.
    /// Amortized O(1) per removal. The new slab, table and free list are allocated before the
    /// old ones are touched. Pressure eviction leaves compaction to normal removals.
    pub fn compact(&mut self) {
        const SLACK: usize = 1024;
        if self.slots.len() <= 2 * self.len() + SLACK {
            return;
        }
        let mut slots: Vec<Option<Node<K, V>>> = Vec::with_capacity(self.len());
        let mut table = HashTable::with_capacity(self.len());
        let free = Vec::with_capacity(self.len());
        for lists in self.class_lists.iter_mut() {
            *lists = [List::EMPTY; 2];
        }
        for queue in [Queue::Small, Queue::Main] {
            let mut i = self.list_mut(QUEUE, queue, 0).head;
            let first = slots.len();
            while i != NIL {
                let mut node = self.slots[i].take().expect("linked slot holds an entry");
                i = node.links[QUEUE].next;
                let at = slots.len();
                node.links[QUEUE] = Link {
                    prev: if at == first { NIL } else { at - 1 },
                    next: if i == NIL { NIL } else { at + 1 },
                };
                let class = &mut self.class_lists[node.class][queue as usize];
                node.links[CLASS] = Link {
                    prev: class.tail,
                    next: NIL,
                };
                match class.tail {
                    NIL => class.head = at,
                    tail => {
                        if let Some(prev) = slots[tail].as_mut() {
                            prev.links[CLASS].next = at;
                        }
                    }
                }
                class.tail = at;
                class.len += 1;
                class.bytes += node.bytes;
                slots.push(Some(node));
            }
            let list = self.list_mut(QUEUE, queue, 0);
            (list.head, list.tail) = match slots.len() {
                end if end == first => (NIL, NIL),
                end => (first, end - 1),
            };
        }
        for (i, slot) in slots.iter().enumerate() {
            let hash = slot.as_ref().map_or(0, |node| node.hash);
            table.insert_unique(hash, i, |&j| Self::slot_hash(&slots, j));
        }
        self.slots = slots;
        self.table = table;
        self.free = free;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::VecDeque;

    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;

    /// Slots linked into `list` through their `which` links, checking the back links, the
    /// queue and class of each, and the list's counts.
    fn walk<K: Hash + Eq + std::fmt::Debug, V, S: BuildHasher>(
        c: &S3Fifo<K, V, S>,
        list: List,
        which: usize,
        queue: Queue,
        class: Option<usize>,
    ) -> Vec<usize> {
        let (mut slots, mut i, mut prev, mut bytes) = (Vec::new(), list.head, NIL, 0);
        while i != NIL {
            let node = c.node(i);
            assert!(node.queue == queue, "slot {i} linked into the wrong queue");
            assert!(
                class.is_none_or(|class| node.class == class),
                "slot {i}: class"
            );
            assert_eq!(node.links[which].prev, prev, "slot {i}: prev link");
            assert_eq!(node.hash, c.hasher.hash_one(&node.key));
            assert_eq!(c.find(&node.key), Some(i), "{:?} not at its slot", node.key);
            slots.push(i);
            bytes += node.bytes;
            (prev, i) = (i, node.links[which].next);
        }
        assert_eq!(list.tail, prev, "tail");
        assert_eq!((list.len, list.bytes), (slots.len(), bytes), "list counts");
        slots
    }

    /// The slab's links, counts and table agree with each other; each class lists its entries
    /// of each queue in queue order.
    fn check_structure<K: Hash + Eq + std::fmt::Debug, V, S: BuildHasher>(c: &S3Fifo<K, V, S>) {
        let mut linked = 0;
        for (queue, list) in [(Queue::Small, c.small), (Queue::Main, c.main)] {
            let slots = walk(c, list, QUEUE, queue, None);
            for (class, lists) in c.class_lists.iter().enumerate() {
                let in_class: Vec<usize> = slots
                    .iter()
                    .copied()
                    .filter(|&i| c.node(i).class == class)
                    .collect();
                let listed = walk(c, lists[queue as usize], CLASS, queue, Some(class));
                assert_eq!(listed, in_class, "class {class} out of queue order");
            }
            linked += slots.len();
        }
        let live = c.slots.iter().flatten().count();
        assert_eq!((linked, live, c.table.len()), (live, c.len(), live));
        let mut free = c.free.clone();
        free.sort_unstable();
        free.dedup();
        assert_eq!(free.len(), c.free.len(), "a free slot listed twice");
        assert!(free.iter().all(|&i| c.slots[i].is_none()));
        assert_eq!(
            free.len() + live,
            c.slots.len(),
            "a slot neither live nor free"
        );
        assert!(c.free.capacity() >= c.slots.len(), "no room to free a slot");
    }

    fn order<K: Clone, V, S>(c: &S3Fifo<K, V, S>, list: List) -> Vec<K> {
        let mut keys = Vec::new();
        let mut i = list.head;
        while i != NIL {
            let node = c.slots[i].as_ref().unwrap();
            keys.push(node.key.clone());
            i = node.links[QUEUE].next;
        }
        keys
    }

    struct ModelEntry {
        value: u32,
        bytes: usize,
        freq: u8,
        main: bool,
    }

    /// S3-FIFO written the plain way: entries in a map, queues as lists of keys. Slow, but
    /// each rule is one line, to compare the slab against.
    struct Model {
        entries: HashMap<u32, ModelEntry>,
        small: VecDeque<u32>,
        main: VecDeque<u32>,
        ghost: Ghost,
        hasher: DefaultHashBuilder,
        config: S3FifoConfig,
    }

    impl Model {
        fn new(config: S3FifoConfig, hasher: DefaultHashBuilder) -> Self {
            Model {
                entries: HashMap::new(),
                small: VecDeque::new(),
                main: VecDeque::new(),
                ghost: Ghost::new(config.ghost_entries, config.ghost_bytes),
                hasher,
                config,
            }
        }

        fn queue(&mut self, main: bool) -> &mut VecDeque<u32> {
            if main {
                &mut self.main
            } else {
                &mut self.small
            }
        }

        fn unqueue(&mut self, key: u32) {
            let main = self.entries[&key].main;
            let queue = self.queue(main);
            let at = queue.iter().position(|k| *k == key).unwrap();
            queue.remove(at);
        }

        fn move_back(&mut self, key: u32) {
            self.unqueue(key);
            self.entries.get_mut(&key).unwrap().main = true;
            self.main.push_back(key);
        }

        fn evict(&mut self, key: u32, ghost: bool) -> (u32, u32, usize) {
            if ghost {
                let hash = self.hasher.hash_one(key);
                let bytes = self.entries[&key].bytes;
                self.ghost.record(hash, bytes);
            }
            self.unqueue(key);
            let e = self.entries.remove(&key).unwrap();
            (key, e.value, e.bytes)
        }

        fn bytes(&self, main: bool) -> usize {
            self.entries
                .values()
                .filter(|e| e.main == main)
                .map(|e| e.bytes)
                .sum()
        }

        fn get(&mut self, key: u32) -> Option<u32> {
            let e = self.entries.get_mut(&key)?;
            e.freq = (e.freq + 1).min(MAX_FREQ);
            Some(e.value)
        }

        fn insert(&mut self, key: u32, value: u32, bytes: usize) -> Option<u32> {
            if let Some(e) = self.entries.get_mut(&key) {
                e.bytes = bytes;
                return Some(std::mem::replace(&mut e.value, value));
            }
            let remembered = self.ghost.take(self.hasher.hash_one(key));
            let main = remembered || bytes >= self.config.large_entry_bytes;
            self.entries.insert(key, ModelEntry {
                value,
                bytes,
                freq: 0,
                main,
            });
            self.queue(main).push_back(key);
            None
        }

        fn remove(&mut self, key: u32) -> Option<u32> {
            self.entries
                .contains_key(&key)
                .then(|| self.evict(key, false).1)
        }

        fn pick_small(&self, flip: bool) -> bool {
            match (self.small.len(), self.main.len()) {
                (0, _) => false,
                (_, 0) => true,
                _ => {
                    let total = self.bytes(false) + self.bytes(true);
                    let over = self.bytes(false) as f64 > total as f64 * self.config.small_ratio;
                    over != flip
                }
            }
        }

        fn pop_evictable(
            &mut self,
            keep: impl Fn(u32) -> bool,
            max_checks: usize,
            mut steps: usize,
            record_ghost: bool,
        ) -> Option<(u32, u32, usize)> {
            let (mut checks, mut flip) = (0, false);
            while checks < max_checks && steps > 0 {
                steps -= 1;
                let from_small = self.pick_small(flip);
                let key = *self.queue(!from_small).front()?;
                let e = &self.entries[&key];
                let (freq, kept) = (e.freq, keep(e.value));
                if from_small {
                    if freq > 0 || kept {
                        checks += usize::from(freq == 0 && kept);
                        self.entries.get_mut(&key).unwrap().freq = 0;
                        self.move_back(key);
                        continue;
                    }
                    return Some(self.evict(key, record_ghost));
                }
                if freq > 0 {
                    self.entries.get_mut(&key).unwrap().freq = freq - 1;
                    self.move_back(key);
                } else if kept {
                    checks += 1;
                    flip = !flip;
                    self.move_back(key);
                } else {
                    return Some(self.evict(key, false));
                }
            }
            None
        }

        fn evict_where(
            &mut self,
            matches: impl Fn(u32) -> bool,
            keep: impl Fn(u32) -> bool,
            target: usize,
            max_entries: usize,
        ) -> Vec<(u32, u32, usize)> {
            let (mut victims, mut bytes) = (Vec::new(), 0);
            let passes =
                std::iter::once(false).chain(std::iter::repeat_n(true, MAX_FREQ as usize + 1));
            'passes: for main in passes {
                let snapshot: Vec<u32> = self.queue(main).iter().copied().collect();
                for key in snapshot {
                    if victims.len() >= max_entries || bytes >= target {
                        break 'passes;
                    }
                    if !matches(key) {
                        continue;
                    }
                    let e = self.entries.get_mut(&key).unwrap();
                    if e.freq > 0 || keep(e.value) {
                        e.freq = if main { e.freq.saturating_sub(1) } else { 0 };
                        self.move_back(key);
                    } else {
                        let victim = self.evict(key, !main);
                        bytes += victim.2;
                        victims.push(victim);
                    }
                }
            }
            victims
        }

        fn remove_if(&mut self, pred: impl Fn(u32, u32) -> bool) -> Vec<(u32, u32, usize)> {
            let keys: Vec<u32> = self
                .entries
                .iter()
                .filter(|(k, e)| pred(**k, e.value))
                .map(|(k, _)| *k)
                .collect();
            keys.into_iter().map(|k| self.evict(k, false)).collect()
        }
    }

    fn sorted(mut v: Vec<(u32, u32, usize)>) -> Vec<(u32, u32, usize)> {
        v.sort_unstable();
        v
    }

    const CLASSES: u32 = 3;

    fn class(key: u32) -> usize {
        (key % CLASSES) as usize
    }

    /// Runs random operations on the slab and on the model; every result, the queues in order,
    /// the byte counts and the structure must agree after each one. Keys are in class
    /// `key % CLASSES`.
    fn compare(seed: u64, ops: usize, keys: u32, config: S3FifoConfig) {
        let config = S3FifoConfig {
            classes: CLASSES as usize,
            ..config
        };
        let mut rng = SmallRng::seed_from_u64(seed);
        let hasher = DefaultHashBuilder::default();
        let mut c = S3Fifo::<u32, u32>::with_hasher(config, hasher);
        let mut m = Model::new(config, hasher);
        for op in 0..ops {
            let key = rng.gen_range(0..keys);
            let value = op as u32;
            let bytes = rng.gen_range(1..=8);
            // Values below `held` are kept: held by someone else.
            let held = rng.gen_range(0..ops as u32 / 2 + 1);
            let at = format!("seed {seed} op {op}");
            match rng.gen_range(0..100) {
                0..25 => assert_eq!(c.get(&key).copied(), m.get(key), "{at} get"),
                25..55 => assert_eq!(
                    c.insert_in(class(key), key, value, bytes),
                    m.insert(key, value, bytes),
                    "{at} insert"
                ),
                55..62 => assert_eq!(c.remove(&key), m.remove(key), "{at} remove"),
                62..72 => {
                    let checks = rng.gen_range(1..40);
                    let got = c.pop_evictable(|v| *v < held, checks);
                    let want = m.pop_evictable(|v| v < held, checks, usize::MAX, true);
                    assert_eq!(got, want, "{at} pop");
                }
                72..80 => {
                    let checks = rng.gen_range(1..40);
                    let steps = rng.gen_range(0..20);
                    let mut left = steps;
                    let got = c.pop_evictable_bounded(|v| *v < held, checks, &mut left);
                    let want = m.pop_evictable(|v| v < held, checks, steps, false);
                    assert_eq!(got, want, "{at} bounded pop");
                    c.compact();
                }
                80..94 => {
                    let class_of = rng.gen_range(0..CLASSES as usize);
                    let target = rng.gen_range(1..64);
                    let max = rng.gen_range(1..16);
                    let matches = |k: u32| class(k) == class_of;
                    let got = c.evict_class(class_of, |v| *v < held, target, max);
                    assert_eq!(
                        got,
                        m.evict_where(matches, |v| v < held, target, max),
                        "{at} evict_class"
                    );
                }
                94..95 => {
                    let class_of = rng.gen_range(0..CLASSES as usize);
                    let got = sorted(c.remove_class(class_of));
                    let want = sorted(m.remove_if(|k, _| class(k) == class_of));
                    assert_eq!(got, want, "{at} remove_class");
                }
                95..99 => {
                    let modulo = rng.gen_range(1..5);
                    let got = sorted(c.remove_if(|k, _| k.is_multiple_of(modulo)));
                    assert_eq!(
                        got,
                        sorted(m.remove_if(|k, _| k.is_multiple_of(modulo))),
                        "{at} remove_if"
                    );
                }
                _ => {
                    // Fill up, so that removals shrink the slab and compaction runs.
                    for k in keys..keys + 3000 {
                        assert_eq!(c.insert_in(class(k), k, value, 1), m.insert(k, value, 1));
                    }
                }
            }
            check_structure(&c);
            assert_eq!(
                order(&c, c.small),
                Vec::from(m.small.clone()),
                "{at} small order"
            );
            assert_eq!(
                order(&c, c.main),
                Vec::from(m.main.clone()),
                "{at} main order"
            );
            assert_eq!(
                (c.small_bytes(), c.bytes()),
                (m.bytes(false), m.bytes(false) + m.bytes(true)),
                "{at}"
            );
            assert!(c.slots() <= 2 * c.len() + 1024, "{at} slab not compacted");
            assert_eq!(
                (c.ghost_len(), c.ghost_bytes()),
                (m.ghost.len(), m.ghost.bytes()),
                "{at} ghost"
            );
        }
    }

    #[test]
    fn test_matches_model() {
        for seed in 0..8 {
            compare(seed, 3000, 64, S3FifoConfig {
                ghost_entries: 32,
                ..Default::default()
            });
        }
    }

    #[test]
    fn test_matches_model_with_byte_bounded_ghost() {
        for seed in 100..108 {
            compare(seed, 3000, 64, S3FifoConfig {
                ghost_entries: 16,
                ghost_bytes: 40,
                large_entry_bytes: 7,
                ..Default::default()
            });
        }
    }

    /// Compaction rebuilds the slab in queue order; eviction order is unchanged.
    #[test]
    fn test_compaction_keeps_order() {
        let hasher = DefaultHashBuilder::default();
        let mut c = S3Fifo::<u32, u32>::with_hasher(S3FifoConfig::default(), hasher);
        let mut m = Model::new(S3FifoConfig::default(), hasher);
        for k in 0..5000 {
            c.insert(k, k, 1);
            m.insert(k, k, 1);
        }
        for k in (0..5000).step_by(10) {
            c.get(&k);
            m.get(k);
        }
        let removed = |k: u32| !k.is_multiple_of(10) && k < 4990;
        assert_eq!(c.remove_if(|k, _| removed(*k)).len(), 4491);
        m.remove_if(|k, _| removed(k));
        check_structure(&c);
        assert_eq!(c.slots(), c.len(), "not compacted");
        loop {
            let got = c.pop_evictable(|_| false, usize::MAX);
            assert_eq!(
                got,
                m.pop_evictable(|_| false, usize::MAX, usize::MAX, true)
            );
            check_structure(&c);
            if got.is_none() {
                break;
            }
        }
        assert!(c.is_empty());
    }
}
