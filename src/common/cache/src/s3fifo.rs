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

use std::collections::VecDeque;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use hashbrown::DefaultHashBuilder;
use hashbrown::Equivalent;
use hashbrown::HashMap;

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

struct Slot<V> {
    value: V,
    bytes: usize,
    freq: AtomicU8,
    seq: u64,
    in_main: bool,
}

/// S3-FIFO eviction order without a capacity of its own: the owner decides when to evict.
///
/// Hits only bump an atomic counter, so lookups need `&self`. Removed keys leave stale
/// queue positions that are skipped when popped.
pub struct S3Fifo<K, V, S = DefaultHashBuilder> {
    map: HashMap<K, Slot<V>, S>,
    small: VecDeque<(K, u64)>,
    main: VecDeque<(K, u64)>,
    ghost: Ghost,
    small_bytes: usize,
    main_bytes: usize,
    next_seq: u64,
    config: S3FifoConfig,
}

impl<K: Hash + Eq + Clone, V> S3Fifo<K, V> {
    pub fn new(config: S3FifoConfig) -> Self {
        Self::with_hasher(config, DefaultHashBuilder::default())
    }
}

impl<K: Hash + Eq + Clone, V, S: BuildHasher> S3Fifo<K, V, S> {
    pub fn with_hasher(config: S3FifoConfig, hasher: S) -> Self {
        S3Fifo {
            map: HashMap::with_hasher(hasher),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: Ghost::new(config.ghost_entries),
            small_bytes: 0,
            main_bytes: 0,
            next_seq: 0,
            config,
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        let slot = self.map.get(key)?;
        let freq = slot.freq.load(Ordering::Relaxed);
        if freq < MAX_FREQ {
            slot.freq.store(freq + 1, Ordering::Relaxed);
        }
        Some(&slot.value)
    }

    pub fn peek<Q>(&self, key: &Q) -> Option<&V>
    where Q: Hash + Equivalent<K> + ?Sized {
        self.map.get(key).map(|slot| &slot.value)
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where Q: Hash + Equivalent<K> + ?Sized {
        self.map.contains_key(key)
    }

    /// Read-path insertion, subject to the `admit_after` threshold.
    pub fn admit(&mut self, key: K, value: V, bytes: usize) -> Admission<V> {
        if self.map.contains_key(&key) {
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
        let hash = self.map.hasher().hash_one(key);
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
        let placement = if self.map.contains_key(&key) {
            Placement::Small
        } else {
            let hash = self.map.hasher().hash_one(&key);
            match self.ghost.take(hash) {
                Some(_) => Placement::Main,
                None => Placement::Small,
            }
        };
        self.insert_at(key, value, bytes, placement)
    }

    /// Inserts with a given placement; an existing entry keeps its position.
    pub fn insert_at(&mut self, key: K, value: V, bytes: usize, placement: Placement) -> Option<V> {
        if let Some(slot) = self.map.get_mut(&key) {
            if slot.in_main {
                self.main_bytes = self.main_bytes - slot.bytes + bytes;
            } else {
                self.small_bytes = self.small_bytes - slot.bytes + bytes;
            }
            slot.bytes = bytes;
            return Some(std::mem::replace(&mut slot.value, value));
        }
        let in_main = placement == Placement::Main || bytes >= self.config.large_entry_bytes;
        self.push_new(key, value, bytes, in_main);
        None
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where Q: Hash + Equivalent<K> + ?Sized {
        let slot = self.map.remove(key)?;
        self.untrack(&slot);
        Some(slot.value)
    }

    /// Pops the next victim, rotating entries for which `keep` returns true.
    /// Gives up after examining `max_checks` live entries.
    pub fn pop_evictable(
        &mut self,
        keep: impl Fn(&V) -> bool,
        max_checks: usize,
    ) -> Option<(K, V, usize)> {
        let mut checks = 0;
        let mut flip = false;
        while checks < max_checks {
            let from_small = self.pick_small(flip);
            let queue = if from_small {
                &mut self.small
            } else {
                &mut self.main
            };
            let (key, seq) = queue.pop_front()?;
            let Some(slot) = self.map.get_mut(&key) else {
                continue;
            };
            if slot.seq != seq {
                continue;
            }
            checks += 1;

            let freq = slot.freq.load(Ordering::Relaxed);
            if from_small {
                if freq > 0 || keep(&slot.value) {
                    slot.in_main = true;
                    slot.freq.store(0, Ordering::Relaxed);
                    self.small_bytes -= slot.bytes;
                    self.main_bytes += slot.bytes;
                    self.main.push_back((key, seq));
                    continue;
                }
                let hash = self.map.hasher().hash_one(&key);
                self.ghost
                    .record(hash, self.config.admit_after.saturating_sub(1));
            } else if freq > 0 {
                slot.freq.store(freq - 1, Ordering::Relaxed);
                self.main.push_back((key, seq));
                continue;
            } else if keep(&slot.value) {
                self.main.push_back((key, seq));
                flip = !flip;
                continue;
            }

            let slot = self.map.remove(&key)?;
            self.untrack(&slot);
            return Some((key, slot.value, slot.bytes));
        }
        None
    }

    pub fn remove_if(&mut self, mut pred: impl FnMut(&K, &V) -> bool) -> Vec<(K, V, usize)> {
        let removed: Vec<_> = self
            .map
            .extract_if(|k, slot| pred(k, &slot.value))
            .collect();
        removed
            .into_iter()
            .map(|(k, slot)| {
                self.untrack(&slot);
                (k, slot.value, slot.bytes)
            })
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter().map(|(k, slot)| (k, &slot.value))
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.small_bytes + self.main_bytes
    }

    pub fn ghost_len(&self) -> usize {
        self.ghost.len()
    }

    fn pick_small(&self, flip: bool) -> bool {
        match (self.small.is_empty(), self.main.is_empty()) {
            (true, _) => false,
            (false, true) => true,
            (false, false) => {
                let over = self.small_bytes as f64 > self.bytes() as f64 * self.config.small_ratio;
                over != flip
            }
        }
    }

    fn push_new(&mut self, key: K, value: V, bytes: usize, in_main: bool) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if in_main {
            self.main_bytes += bytes;
            self.main.push_back((key.clone(), seq));
        } else {
            self.small_bytes += bytes;
            self.small.push_back((key.clone(), seq));
        }
        self.map.insert(key, Slot {
            value,
            bytes,
            freq: AtomicU8::new(0),
            seq,
            in_main,
        });
    }

    fn untrack(&mut self, slot: &Slot<V>) {
        if slot.in_main {
            self.main_bytes -= slot.bytes;
        } else {
            self.small_bytes -= slot.bytes;
        }
    }
}
