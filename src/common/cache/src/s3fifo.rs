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
    /// A key is cached on its `admit_after`-th admission attempt; earlier attempts only touch the ghost.
    pub admit_after: u8,
    pub ghost_entries: usize,
    /// Entries at least this large skip the small queue.
    pub large_entry_bytes: usize,
}

impl Default for S3FifoConfig {
    fn default() -> Self {
        S3FifoConfig {
            small_ratio: 0.1,
            admit_after: 1,
            ghost_entries: 65536,
            large_entry_bytes: usize::MAX,
        }
    }
}

pub enum Admission<V> {
    Admitted { replaced: Option<V> },
    Deferred(V),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    Small,
    Main,
}

const NIL: usize = usize::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Queue {
    Small,
    Main,
}

struct Node<K, V> {
    key: K,
    value: V,
    hash: u64,
    bytes: usize,
    freq: AtomicU8,
    queue: Queue,
    prev: usize,
    next: usize,
}

/// A doubly linked queue threaded through the slots.
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
/// behind. Hits only bump an atomic counter, so lookups need `&self`.
pub struct S3Fifo<K, V, S = DefaultHashBuilder> {
    /// Slot index of each entry, found by the entry's hash and key.
    table: HashTable<usize>,
    slots: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    small: List,
    main: List,
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
            ghost: Ghost::new(config.ghost_entries),
            hasher,
            config,
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        let node = self.node(self.find(key)?);
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

    /// Read-path insertion, subject to the `admit_after` threshold.
    pub fn admit(&mut self, key: K, value: V, bytes: usize) -> Admission<V> {
        if self.contains(&key) {
            return Admission::Admitted {
                replaced: self.insert(key, value, bytes),
            };
        }
        match self.admission(&key) {
            Some(placement) => Admission::Admitted {
                replaced: self.insert_at(key, value, bytes, placement),
            },
            None => Admission::Deferred(value),
        }
    }

    /// Admission decision for a key that is not cached. `None` means deferred: the access is
    /// only recorded in the ghost. Pair with `insert_at` when eviction must happen in between.
    pub fn admission(&mut self, key: &K) -> Option<Placement> {
        let hash = self.hasher.hash_one(key);
        match self.ghost.take(hash) {
            None if self.config.admit_after <= 1 => Some(Placement::Small),
            None => {
                self.ghost.record(hash, 1);
                None
            }
            Some(seen) if seen.saturating_add(1) >= self.config.admit_after => {
                Some(Placement::Main)
            }
            Some(seen) => {
                self.ghost.record(hash, seen + 1);
                None
            }
        }
    }

    /// Write-path insertion, bypassing admission. Returns the replaced value.
    pub fn insert(&mut self, key: K, value: V, bytes: usize) -> Option<V> {
        let placement = if self.contains(&key) {
            Placement::Small
        } else {
            match self.ghost.take(self.hasher.hash_one(&key)) {
                Some(_) => Placement::Main,
                None => Placement::Small,
            }
        };
        self.insert_at(key, value, bytes, placement)
    }

    /// Inserts with a given placement; an existing entry keeps its position.
    pub fn insert_at(&mut self, key: K, value: V, bytes: usize, placement: Placement) -> Option<V> {
        if let Some(i) = self.find(&key) {
            let node = self.slots[i].as_mut()?;
            let list = match node.queue {
                Queue::Small => &mut self.small,
                Queue::Main => &mut self.main,
            };
            list.bytes = list.bytes - node.bytes + bytes;
            node.bytes = bytes;
            return Some(std::mem::replace(&mut node.value, value));
        }
        let in_main = placement == Placement::Main || bytes >= self.config.large_entry_bytes;
        let hash = self.hasher.hash_one(&key);
        let i = self.alloc(Node {
            key,
            value,
            hash,
            bytes,
            freq: AtomicU8::new(0),
            queue: if in_main { Queue::Main } else { Queue::Small },
            prev: NIL,
            next: NIL,
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
        let mut checks = 0;
        let mut flip = false;
        while checks < max_checks {
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
                self.ghost
                    .record(node.hash, self.config.admit_after.saturating_sub(1));
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
            self.compact();
            return Some((node.key, node.value, node.bytes));
        }
        None
    }

    /// Evicts entries whose key `matches`, in eviction order, until `target` bytes or
    /// `max_entries` entries are gone; entries for which `keep` returns true stay. Matching
    /// entries are aged as by `pop_evictable`, clock-style: an accessed entry in the small
    /// queue, or an entry kept, moves to the back of the main queue; an accessed entry in the
    /// main queue loses one access and moves to its back. Other entries keep their place and
    /// their access counts.
    ///
    /// Walks the small queue once and the main queue up to `MAX_FREQ + 1` times, each pass
    /// over the entries it started with, so it always ends. Passing other keys costs a step
    /// each: a call costs up to the queue length, once per batch rather than per victim.
    pub fn evict_where(
        &mut self,
        matches: impl Fn(&K) -> bool,
        keep: impl Fn(&V) -> bool,
        target: usize,
        max_entries: usize,
    ) -> Vec<(K, V, usize)> {
        let mut victims = Vec::new();
        let mut bytes = 0;
        // A main pass lowers each access count by one, so enough passes reach every entry
        // that can be evicted.
        let passes = std::iter::once(true).chain(std::iter::repeat_n(false, MAX_FREQ as usize + 1));
        'passes: for from_small in passes {
            let list = if from_small { self.small } else { self.main };
            let mut i = list.head;
            for _ in 0..list.len {
                if victims.len() >= max_entries || bytes >= target {
                    break 'passes;
                }
                let node = self.node(i);
                let next = node.next;
                if !matches(&node.key) {
                    i = next;
                    continue;
                }
                let freq = node.freq.load(Ordering::Relaxed);
                if freq > 0 || keep(&node.value) {
                    let aged = if from_small {
                        0
                    } else {
                        freq.saturating_sub(1)
                    };
                    node.freq.store(aged, Ordering::Relaxed);
                    self.move_to_main(i);
                } else {
                    if from_small {
                        self.ghost
                            .record(node.hash, self.config.admit_after.saturating_sub(1));
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

    /// Entries at least this large skip the small queue from now on, e.g. after the owner's
    /// capacity changed.
    pub fn set_large_entry_bytes(&mut self, bytes: usize) {
        self.config.large_entry_bytes = bytes;
    }

    /// Bytes of the entries in the small (probation) queue.
    pub fn small_bytes(&self) -> usize {
        self.small.bytes
    }

    pub fn remove_if(&mut self, mut pred: impl FnMut(&K, &V) -> bool) -> Vec<(K, V, usize)> {
        let matched: Vec<usize> = (0..self.slots.len())
            .filter(|&i| {
                self.slots[i]
                    .as_ref()
                    .is_some_and(|node| pred(&node.key, &node.value))
            })
            .collect();
        let removed = matched
            .into_iter()
            .map(|i| {
                let node = self.take(i);
                (node.key, node.value, node.bytes)
            })
            .collect();
        self.compact();
        removed
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

    pub fn ghost_len(&self) -> usize {
        self.ghost.len()
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
        let hash = self.hasher.hash_one(key);
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

    fn list_mut(&mut self, queue: Queue) -> &mut List {
        match queue {
            Queue::Small => &mut self.small,
            Queue::Main => &mut self.main,
        }
    }

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

    /// Unlinks and frees slot `i`, dropping it from the table.
    fn take(&mut self, i: usize) -> Node<K, V> {
        self.unlink(i);
        let hash = self.node(i).hash;
        if let Ok(entry) = self.table.find_entry(hash, |&j| j == i) {
            entry.remove();
        }
        self.free.push(i);
        self.slots[i].take().expect("linked slot holds an entry")
    }

    fn link_back(&mut self, i: usize) {
        let (queue, bytes) = {
            let node = self.node(i);
            (node.queue, node.bytes)
        };
        let tail = self.list_mut(queue).tail;
        {
            let node = self.node_mut(i);
            node.prev = tail;
            node.next = NIL;
        }
        if tail == NIL {
            self.list_mut(queue).head = i;
        } else {
            self.node_mut(tail).next = i;
        }
        let list = self.list_mut(queue);
        list.tail = i;
        list.len += 1;
        list.bytes += bytes;
    }

    fn unlink(&mut self, i: usize) {
        let (queue, bytes, prev, next) = {
            let node = self.node(i);
            (node.queue, node.bytes, node.prev, node.next)
        };
        if prev == NIL {
            self.list_mut(queue).head = next;
        } else {
            self.node_mut(prev).next = next;
        }
        if next == NIL {
            self.list_mut(queue).tail = prev;
        } else {
            self.node_mut(next).prev = prev;
        }
        let list = self.list_mut(queue);
        list.len -= 1;
        list.bytes -= bytes;
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
    /// Amortized O(1) per removal.
    fn compact(&mut self) {
        const SLACK: usize = 1024;
        if self.slots.len() <= 2 * self.len() + SLACK {
            return;
        }
        let mut old = std::mem::take(&mut self.slots);
        let mut slots = Vec::with_capacity(self.len());
        for list in [&mut self.small, &mut self.main] {
            let mut i = list.head;
            let first = slots.len();
            while i != NIL {
                let mut node = old[i].take().expect("linked slot holds an entry");
                i = node.next;
                let at = slots.len();
                node.prev = if at == first { NIL } else { at - 1 };
                node.next = if i == NIL { NIL } else { at + 1 };
                slots.push(Some(node));
            }
            (list.head, list.tail) = match slots.len() {
                end if end == first => (NIL, NIL),
                end => (first, end - 1),
            };
        }
        self.table.clear();
        for (i, slot) in slots.iter().enumerate() {
            let hash = slot.as_ref().map_or(0, |node| node.hash);
            self.table
                .insert_unique(hash, i, |&j| Self::slot_hash(&slots, j));
        }
        self.table.shrink_to_fit(|&j| Self::slot_hash(&slots, j));
        self.slots = slots;
        self.free = Vec::new();
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

    /// The slab's links, counts and table agree with each other.
    fn check_structure<K: Hash + Eq + std::fmt::Debug, V, S: BuildHasher>(c: &S3Fifo<K, V, S>) {
        let mut linked = 0;
        for (queue, list) in [(Queue::Small, c.small), (Queue::Main, c.main)] {
            let (mut i, mut prev, mut len, mut bytes) = (list.head, NIL, 0, 0);
            while i != NIL {
                let node = c.node(i);
                assert!(node.queue == queue, "slot {i} linked into the wrong queue");
                assert_eq!(node.prev, prev, "slot {i}: prev link");
                assert_eq!(node.hash, c.hasher.hash_one(&node.key));
                assert_eq!(
                    c.find(&node.key),
                    Some(i),
                    "{:?} not found at its slot",
                    node.key
                );
                (prev, i) = (i, node.next);
                len += 1;
                bytes += node.bytes;
            }
            assert_eq!(list.tail, prev, "tail");
            assert_eq!((list.len, list.bytes), (len, bytes), "queue counts");
            linked += len;
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
    }

    fn order<K: Clone, V, S>(c: &S3Fifo<K, V, S>, list: List) -> Vec<K> {
        let mut keys = Vec::new();
        let mut i = list.head;
        while i != NIL {
            let node = c.slots[i].as_ref().unwrap();
            keys.push(node.key.clone());
            i = node.next;
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
                ghost: Ghost::new(config.ghost_entries),
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
                self.ghost
                    .record(hash, self.config.admit_after.saturating_sub(1));
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

        fn admission(&mut self, key: u32) -> Option<Placement> {
            let hash = self.hasher.hash_one(key);
            let admit_after = self.config.admit_after;
            match self.ghost.take(hash) {
                None if admit_after <= 1 => Some(Placement::Small),
                None => {
                    self.ghost.record(hash, 1);
                    None
                }
                Some(seen) if seen.saturating_add(1) >= admit_after => Some(Placement::Main),
                Some(seen) => {
                    self.ghost.record(hash, seen + 1);
                    None
                }
            }
        }

        fn insert(&mut self, key: u32, value: u32, bytes: usize) -> Option<u32> {
            let placement = if self.entries.contains_key(&key) {
                Placement::Small
            } else {
                match self.ghost.take(self.hasher.hash_one(key)) {
                    Some(_) => Placement::Main,
                    None => Placement::Small,
                }
            };
            self.insert_at(key, value, bytes, placement)
        }

        fn insert_at(&mut self, key: u32, value: u32, bytes: usize, p: Placement) -> Option<u32> {
            if let Some(e) = self.entries.get_mut(&key) {
                e.bytes = bytes;
                return Some(std::mem::replace(&mut e.value, value));
            }
            let main = p == Placement::Main || bytes >= self.config.large_entry_bytes;
            self.entries.insert(key, ModelEntry {
                value,
                bytes,
                freq: 0,
                main,
            });
            self.queue(main).push_back(key);
            None
        }

        fn admit(&mut self, key: u32, value: u32, bytes: usize) -> Option<Option<u32>> {
            if self.entries.contains_key(&key) {
                return Some(self.insert(key, value, bytes));
            }
            let placement = self.admission(key)?;
            Some(self.insert_at(key, value, bytes, placement))
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
        ) -> Option<(u32, u32, usize)> {
            let (mut checks, mut flip) = (0, false);
            while checks < max_checks {
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
                    return Some(self.evict(key, true));
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

    /// Runs random operations on the slab and on the model; every result, the queues in order,
    /// the byte counts and the structure must agree after each one.
    fn compare(seed: u64, ops: usize, keys: u32, config: S3FifoConfig) {
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
                25..40 => assert_eq!(
                    c.insert(key, value, bytes),
                    m.insert(key, value, bytes),
                    "{at} insert"
                ),
                40..55 => {
                    let got = match c.admit(key, value, bytes) {
                        Admission::Admitted { replaced } => Some(replaced),
                        Admission::Deferred(_) => None,
                    };
                    assert_eq!(got, m.admit(key, value, bytes), "{at} admit");
                }
                55..62 => assert_eq!(c.remove(&key), m.remove(key), "{at} remove"),
                62..80 => {
                    let checks = rng.gen_range(1..40);
                    let got = c.pop_evictable(|v| *v < held, checks);
                    assert_eq!(got, m.pop_evictable(|v| v < held, checks), "{at} pop");
                }
                80..95 => {
                    let modulo = rng.gen_range(1..4);
                    let target = rng.gen_range(1..64);
                    let max = rng.gen_range(1..16);
                    let matches = |k: u32| k.is_multiple_of(modulo);
                    let got = c.evict_where(|k| matches(*k), |v| *v < held, target, max);
                    assert_eq!(
                        got,
                        m.evict_where(matches, |v| v < held, target, max),
                        "{at} evict_where"
                    );
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
                        assert_eq!(c.insert(k, value, 1), m.insert(k, value, 1));
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
        }
    }

    #[test]
    fn test_matches_model_admit_on_first_access() {
        for seed in 0..8 {
            compare(seed, 3000, 64, S3FifoConfig {
                ghost_entries: 32,
                ..Default::default()
            });
        }
    }

    #[test]
    fn test_matches_model_admit_on_second_access() {
        for seed in 100..108 {
            compare(seed, 3000, 64, S3FifoConfig {
                admit_after: 2,
                ghost_entries: 16,
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
            assert_eq!(got, m.pop_evictable(|_| false, usize::MAX));
            check_structure(&c);
            if got.is_none() {
                break;
            }
        }
        assert!(c.is_empty());
    }
}
