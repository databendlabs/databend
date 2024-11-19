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

//! A cache that holds a limited number of key-value pairs. When the
//! capacity of the cache is exceeded, the least-recently-used
//! (where "used" means a look-up or putting the pair into the cache)
//! pair is automatically removed.
//!
//! # Examples
//!
//! ```rust,ignore
//! use databend_common_cache::{Cache, TtlCache};
//!
//! let mut cache = TtlCache::new(2);
//!
//! cache.put(1, 10);
//! cache.put(2, 20);
//! cache.put(3, 30);
//! assert!(cache.get(&1).is_none());
//! assert_eq!(*cache.get(&2).unwrap(), 20);
//! assert_eq!(*cache.get(&3).unwrap(), 30);
//!
//! cache.put(2, 22);
//! assert_eq!(*cache.get(&2).unwrap(), 22);
//!
//! cache.put(6, 60);
//! assert!(cache.get(&3).is_none());
//!
//! cache.set_capacity(1);
//! assert!(cache.get(&2).is_none());
//! ```
//!
//! The cache can also be limited by an arbitrary metric calculated from its key-value pairs, see
//! [`TtlCache::with_meter`][with_meter] for more information. Custom metrics can be written by
//! implementing the [`Meter`][meter] trait.
//!
//! [with_meter]: struct.TtlCache.html#method.with_meter
//! [meter]: trait.Meter.html

use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;

use hashlink::LinkedHashMap;

use crate::cache::Cache;
use crate::mem_sized::MemSized;

#[derive(Clone)]
struct InternalEntry<V> {
    value: V,
    expiration: Instant,
}

impl<V> InternalEntry<V> {
    #[inline]
    fn new(v: V, duration: Duration) -> Self {
        InternalEntry {
            value: v,
            expiration: Instant::now() + duration,
        }
    }

    #[inline]
    fn is_expired(&self) -> bool {
        Instant::now() > self.expiration
    }
}

/// A TTL cache.
#[derive(Clone)]
pub struct TtlCache<K: Eq + Hash + MemSized, V: MemSized> {
    map: LinkedHashMap<K, InternalEntry<V>>,
    max_items: usize,
    max_bytes: usize,
    bytes: usize,
    ttl: Duration,
}

impl<K: Eq + Hash + MemSized, V: MemSized> TtlCache<K, V> {
    /// Creates an empty cache that can hold at most `capacity` items.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    /// let mut cache: TtlCache<i32, &str> = TtlCache::new(10);
    /// ```
    pub fn with_items_capacity(items_capacity: usize, ttl: Duration) -> Self {
        TtlCache {
            map: LinkedHashMap::new(),
            max_items: items_capacity,
            max_bytes: usize::MAX,
            bytes: 0,
            ttl,
        }
    }

    pub fn with_bytes_capacity(bytes_capacity: usize, ttl: Duration) -> Self {
        TtlCache {
            map: LinkedHashMap::new(),
            max_items: usize::MAX,
            max_bytes: bytes_capacity,
            bytes: 0,
            ttl,
        }
    }
}

impl<K: Eq + Hash + MemSized, V: MemSized> Cache<K, V> for TtlCache<K, V> {
    /// Returns a reference to the value corresponding to the given key in the cache, if
    /// any.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    ///
    /// let mut cache = TtlCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// cache.put(2, "c");
    /// cache.put(3, "d");
    ///
    /// assert_eq!(cache.get(&1), None);
    /// assert_eq!(cache.get(&2), Some(&"c"));
    /// ```
    fn get<Q>(&mut self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map
            .get(k)
            .and_then(|x| if x.is_expired() { None } else { Some(&x.value) })
    }

    /// Returns a reference to the value corresponding to the key in the cache or `None` if it is
    /// not present in the cache. Unlike `get`, `peek` does not update the LRU list so the key's
    /// position will be unchanged.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.peek(&1), Some(&"a"));
    /// assert_eq!(cache.peek(&2), Some(&"b"));
    /// ```
    fn peek<Q>(&self, _k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unimplemented!()
    }

    /// Returns the value corresponding to the least recently used item or `None` if the
    /// cache is empty. Like `peek`, `peek_by_policy` does not update the LRU list so the item's
    /// position will be unchanged.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.peek_by_policy(), Some((&1, &"a")));
    /// ```
    fn peek_by_policy(&self) -> Option<(&K, &V)> {
        unimplemented!()
    }

    /// Inserts a key-value pair into the cache. If the key already existed, the old value is
    /// returned.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    ///
    /// let mut cache = TtlCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// assert_eq!(cache.get(&1), Some(&"a"));
    /// assert_eq!(cache.get(&2), Some(&"b"));
    /// ```
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        let to_insert = InternalEntry::new(v, self.ttl);
        let old_val = self.map.insert(k, to_insert);
        if self.len() > self.max_items {
            self.pop_by_policy();
        }
        old_val.and_then(|x| Some(x.value))
    }

    /// Removes the given key from the cache and returns its corresponding value.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    ///
    /// let mut cache = TtlCache::new(2);
    ///
    /// cache.put(2, "a");
    ///
    /// assert_eq!(cache.pop(&1), None);
    /// assert_eq!(cache.pop(&2), Some("a"));
    /// assert_eq!(cache.pop(&2), None);
    /// assert_eq!(cache.len(), 0);
    /// ```
    fn pop<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.remove(k).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value)
            }
        })
    }

    /// Removes and returns the least recently used key-value pair as a tuple.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    ///
    /// let mut cache = TtlCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.pop_by_policy(), Some((1, "a")));
    /// assert_eq!(cache.len(), 1);
    /// ```
    #[inline]
    fn pop_by_policy(&mut self) -> Option<(K, V)> {
        self.cleanup_all_expired();
        if self.len() > self.max_items {
            self.map.pop_front().map(|(k, v)| (k, v.value))
        } else {
            None
        }
    }

    #[inline]
    fn cleanup_all_expired(&mut self) {
        self.map.retain(|_, entry| !entry.is_expired());
    }

    /// Checks if the map contains the given key.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    ///
    /// let mut cache = TtlCache::new(1);
    ///
    /// cache.put(1, "a");
    /// assert!(cache.contains(&1));
    /// ```
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(key)
    }

    /// Returns the number of key-value pairs in the cache.
    fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the cache contains no key-value pairs.
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the maximum bytes size of the key-value pairs the cache can hold.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, TtlCache};
    /// let mut cache: TtlCache<i32, &str> = TtlCache::new(2);
    /// assert_eq!(cache.capacity(), 2);
    /// ```
    fn bytes_capacity(&self) -> u64 {
        self.max_bytes as u64
    }

    fn items_capacity(&self) -> u64 {
        self.max_items as u64
    }

    /// Returns the bytes size of all the key-value pairs in the cache.
    fn bytes_size(&self) -> u64 {
        self.bytes as u64
    }

    /// Removes all key-value pairs from the cache.
    fn clear(&mut self) {
        self.map.clear();
        self.bytes = 0;
    }
}
