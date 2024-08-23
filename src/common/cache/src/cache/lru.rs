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
//! use databend_common_cache::{Cache, LruCache};
//!
//! let mut cache = LruCache::new(2);
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
//! [`LruCache::with_meter`][with_meter] for more information. Custom metrics can be written by
//! implementing the [`Meter`][meter] trait.
//!
//! [with_meter]: struct.LruCache.html#method.with_meter
//! [meter]: trait.Meter.html

use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;

use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;

use crate::cache::Cache;
use crate::mem_sized::MemSized;

/// An LRU cache.
#[derive(Clone)]
pub struct LruCache<K: Eq + Hash + MemSized, V: MemSized> {
    map: LinkedHashMap<K, V>,
    max_items: usize,
    max_bytes: usize,
    bytes: usize,
}

impl<K: Eq + Hash + MemSized, V: MemSized> LruCache<K, V> {
    /// Creates an empty cache that can hold at most `capacity` items.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    /// let mut cache: LruCache<i32, &str> = LruCache::new(10);
    /// ```
    pub fn with_items_capacity(items_capacity: usize) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            max_items: items_capacity,
            max_bytes: usize::MAX,
            bytes: 0,
        }
    }

    pub fn with_bytes_capacity(bytes_capacity: usize) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            max_items: usize::MAX,
            max_bytes: bytes_capacity,
            bytes: 0,
        }
    }
}

impl<K: Eq + Hash + MemSized, V: MemSized> Cache<K, V> for LruCache<K, V> {
    /// Returns a reference to the value corresponding to the given key in the cache, if
    /// any.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
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
        match self.map.raw_entry_mut().from_key(k) {
            linked_hash_map::RawEntryMut::Occupied(mut occupied) => {
                occupied.to_back();
                Some(occupied.into_mut())
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    /// Inserts a key-value pair into the cache. If the key already existed, the old value is
    /// returned.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// assert_eq!(cache.get(&1), Some(&"a"));
    /// assert_eq!(cache.get(&2), Some(&"b"));
    /// ```
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        // self.bytes += k.mem_bytes();
        self.bytes += v.mem_bytes();

        if let Some(old) = self.map.get(&k) {
            // self.bytes -= k.mem_bytes();
            self.bytes -= old.mem_bytes();
        }

        let old_val = self.map.insert(k, v);
        while self.bytes > self.max_bytes || self.map.len() > self.max_items {
            self.pop_by_policy();
        }
        old_val
    }

    /// Removes the given key from the cache and returns its corresponding value.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
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
        self.map.remove_entry(k).map(|(_k, v)| {
            // self.bytes -= k.mem_bytes();
            self.bytes -= v.mem_bytes();
            v
        })
    }

    /// Removes and returns the least recently used key-value pair as a tuple.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.pop_by_policy(), Some((1, "a")));
    /// assert_eq!(cache.len(), 1);
    /// ```
    #[inline]
    fn pop_by_policy(&mut self) -> Option<(K, V)> {
        self.map.pop_front().map(|(k, v)| {
            // self.bytes -= k.mem_bytes();
            self.bytes -= v.mem_bytes();
            (k, v)
        })
    }

    /// Checks if the map contains the given key.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(1);
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
    /// use databend_common_cache::{Cache, LruCache};
    /// let mut cache: LruCache<i32, &str> = LruCache::new(2);
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

impl<K: Eq + Hash + MemSized, V: MemSized> LruCache<K, V> {
    /// Returns an iterator over the cache's key-value pairs in least- to most-recently-used order.
    ///
    /// Accessing the cache through the iterator does _not_ affect the cache's LRU state.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, 10);
    /// cache.put(2, 20);
    /// cache.put(3, 30);
    ///
    /// let kvs: Vec<_> = cache.iter().collect();
    /// assert_eq!(kvs, [(&2, &20), (&3, &30)]);
    /// ```
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter(self.map.iter())
    }

    /// Returns an iterator over the cache's key-value pairs in least- to most-recently-used order,
    /// with mutable references to the values.
    ///
    /// Accessing the cache through the iterator does _not_ affect the cache's LRU state.
    /// Note that this method is not available for cache objects using `Meter` implementations.
    /// other than `Count`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use databend_common_cache::{Cache, LruCache};
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, 10);
    /// cache.put(2, 20);
    /// cache.put(3, 30);
    ///
    /// let mut n = 2;
    ///
    /// for (k, v) in cache.iter_mut() {
    ///     assert_eq!(*k, n);
    ///     assert_eq!(*v, n * 10);
    ///     *v *= 10;
    ///     n += 1;
    /// }
    ///
    /// assert_eq!(n, 4);
    /// assert_eq!(cache.gett(&2), Some(&200));
    /// assert_eq!(cache.gett(&3), Some(&300));
    /// ```
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut(self.map.iter_mut())
    }
}

impl<K: Eq + Hash + MemSized, V: MemSized> Extend<(K, V)> for LruCache<K, V> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

impl<K: fmt::Debug + Eq + Hash + MemSized, V: fmt::Debug + MemSized> fmt::Debug for LruCache<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map().entries(self.iter().rev()).finish()
    }
}

impl<K: Eq + Hash + MemSized, V: MemSized> IntoIterator for LruCache<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter(self.map.into_iter())
    }
}

impl<'a, K: Eq + Hash + MemSized, V: MemSized> IntoIterator for &'a LruCache<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<'a, K: Eq + Hash + MemSized, V: MemSized> IntoIterator for &'a mut LruCache<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;
    fn into_iter(self) -> IterMut<'a, K, V> {
        self.iter_mut()
    }
}

/// An iterator over a cache's key-value pairs in least- to most-recently-used order.
///
/// # Examples
///
/// ```rust,ignore
/// use databend_common_cache::{Cache, LruCache};
///
/// let mut cache = LruCache::new(2);
///
/// cache.put(1, 10);
/// cache.put(2, 20);
/// cache.put(3, 30);
///
/// let mut n = 2;
///
/// for (k, v) in cache {
///     assert_eq!(k, n);
///     assert_eq!(v, n * 10);
///     n += 1;
/// }
///
/// assert_eq!(n, 4);
/// ```
pub struct IntoIter<K, V>(linked_hash_map::IntoIter<K, V>);

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        self.0.next_back()
    }
}

impl<K, V> ExactSizeIterator for IntoIter<K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// An iterator over a cache's key-value pairs in least- to most-recently-used order.
///
/// Accessing a cache through the iterator does _not_ affect the cache's LRU state.
pub struct Iter<'a, K, V>(linked_hash_map::Iter<'a, K, V>);

impl<'a, K, V> Clone for Iter<'a, K, V> {
    fn clone(&self) -> Iter<'a, K, V> {
        Iter(self.0.clone())
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> {
        self.0.next_back()
    }
}

impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// An iterator over a cache's key-value pairs in least- to most-recently-used order with mutable
/// references to the values.
///
/// Accessing a cache through the iterator does _not_ affect the cache's LRU state.
pub struct IterMut<'a, K, V>(linked_hash_map::IterMut<'a, K, V>);

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);
    fn next(&mut self) -> Option<(&'a K, &'a mut V)> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, K, V> DoubleEndedIterator for IterMut<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a mut V)> {
        self.0.next_back()
    }
}

impl<'a, K, V> ExactSizeIterator for IterMut<'a, K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}
