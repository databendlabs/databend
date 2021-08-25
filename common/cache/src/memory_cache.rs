// Copyright 2021 Datafuse Labs.
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
//! use common_cache::LruCache;
//!
//! let mut cache = LruCache::new(2);
//!
//! cache.insert(1, 10);
//! cache.insert(2, 20);
//! cache.insert(3, 30);
//! assert!(cache.get_mut(&1).is_none());
//! assert_eq!(*cache.get_mut(&2).unwrap(), 20);
//! assert_eq!(*cache.get_mut(&3).unwrap(), 30);
//!
//! cache.insert(2, 22);
//! assert_eq!(*cache.get_mut(&2).unwrap(), 22);
//!
//! cache.insert(6, 60);
//! assert!(cache.get_mut(&3).is_none());
//!
//! cache.set_capacity(1);
//! assert!(cache.get_mut(&2).is_none());
//! ```
//!
//! The cache can also be limited by an arbitrary metric calculated from its key-value pairs, see
//! [`LruCache::with_meter`][with_meter] for more information. If the `heapsize` feature is enabled,
//! this crate provides one such alternate metric&mdash;`HeapSize`. Custom metrics can be written by
//! implementing the [`Meter`][meter] trait.
//!
//! [with_meter]: struct.LruCache.html#method.with_meter
//! [meter]: trait.Meter.html

#[cfg(feature = "heapsize")]
extern crate heapsize_;

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;

use ritelinked::linked_hash_map;
use ritelinked::DefaultHashBuilder;
use ritelinked::LinkedHashMap;

/// A trait for measuring the size of a cache entry.
///
/// If you implement this trait, you should use `usize` as the `Measure` type, otherwise you will
/// also have to implement [`CountableMeter`][countablemeter].
///
/// [countablemeter]: trait.Meter.html
pub trait Meter<K, V> {
    /// The type used to store measurements.
    type Measure: Default + Copy;
    /// Calculate the size of `key` and `value`.
    fn measure<Q: ?Sized>(&self, key: &Q, value: &V) -> Self::Measure
    where K: Borrow<Q>;
}

/// Size limit based on a simple count of cache items.
pub struct Count;

impl<K, V> Meter<K, V> for Count {
    /// Don't store anything, the measurement can be derived from the map.
    type Measure = ();

    /// Don't actually count anything either.
    fn measure<Q: ?Sized>(&self, _: &Q, _: &V)
    where K: Borrow<Q> {
    }
}

/// A trait to allow the default `Count` measurement to not store an
/// extraneous counter.
pub trait CountableMeter<K, V>: Meter<K, V> {
    /// Add `amount` to `current` and return the sum.
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;
    /// Subtract `amount` from `current` and return the difference.
    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;
    /// Return `current` as a `usize` if possible, otherwise return `None`.
    ///
    /// If this method returns `None` the cache will use the number of cache entries as
    /// its size.
    fn size(&self, current: Self::Measure) -> Option<u64>;
}

/// `Count` is all no-ops, the number of entries in the map is the size.
impl<K, V, T: Meter<K, V>> CountableMeter<K, V> for T
where T: CountableMeterWithMeasure<K, V, <T as Meter<K, V>>::Measure>
{
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableMeterWithMeasure::meter_add(self, current, amount)
    }
    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableMeterWithMeasure::meter_sub(self, current, amount)
    }
    fn size(&self, current: Self::Measure) -> Option<u64> {
        CountableMeterWithMeasure::meter_size(self, current)
    }
}

pub trait CountableMeterWithMeasure<K, V, M> {
    /// Add `amount` to `current` and return the sum.
    fn meter_add(&self, current: M, amount: M) -> M;
    /// Subtract `amount` from `current` and return the difference.
    fn meter_sub(&self, current: M, amount: M) -> M;
    /// Return `current` as a `usize` if possible, otherwise return `None`.
    ///
    /// If this method returns `None` the cache will use the number of cache entries as
    /// its size.
    fn meter_size(&self, current: M) -> Option<u64>;
}

/// For any other `Meter` with `Measure=usize`, just do the simple math.
impl<K, V, T> CountableMeterWithMeasure<K, V, usize> for T
where T: Meter<K, V>
{
    fn meter_add(&self, current: usize, amount: usize) -> usize {
        current + amount
    }
    fn meter_sub(&self, current: usize, amount: usize) -> usize {
        current - amount
    }
    fn meter_size(&self, current: usize) -> Option<u64> {
        Some(current as u64)
    }
}

impl<K, V> CountableMeterWithMeasure<K, V, ()> for Count {
    fn meter_add(&self, _current: (), _amount: ()) {}
    fn meter_sub(&self, _current: (), _amount: ()) {}
    fn meter_size(&self, _current: ()) -> Option<u64> {
        None
    }
}

#[cfg(feature = "heapsize")]
mod heap_meter {
    use std::borrow::Borrow;

    use heapsize_::HeapSizeOf;

    /// Size limit based on the heap size of each cache item.
    ///
    /// Requires cache entries that implement [`HeapSizeOf`][1].
    ///
    /// [1]: https://doc.servo.org/heapsize/trait.HeapSizeOf.html
    pub struct HeapSize;

    impl<K, V: HeapSizeOf> super::Meter<K, V> for HeapSize {
        type Measure = usize;

        fn measure<Q: ?Sized>(&self, _: &Q, item: &V) -> usize
        where K: Borrow<Q> {
            item.heap_size_of_children() + ::std::mem::size_of::<V>()
        }
    }
}

#[cfg(feature = "heapsize")]
pub use heap_meter::*;

/// An LRU cache.
#[derive(Clone)]
pub struct LruCache<
    K: Eq + Hash,
    V,
    S: BuildHasher = DefaultHashBuilder,
    M: CountableMeter<K, V> = Count,
> {
    map: LinkedHashMap<K, V, S>,
    current_measure: M::Measure,
    max_capacity: u64,
    meter: M,
}

impl<K: Eq + Hash, V> LruCache<K, V> {
    /// Creates an empty cache that can hold at most `capacity` items.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    /// let mut cache: LruCache<i32, &str> = LruCache::new(10);
    /// ```
    pub fn new(capacity: u64) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            current_measure: (),
            max_capacity: capacity,
            meter: Count,
        }
    }
}

impl<K: Eq + Hash, V, M: CountableMeter<K, V>> LruCache<K, V, DefaultHashBuilder, M> {
    /// Creates an empty cache that can hold at most `capacity` as measured by `meter`.
    ///
    /// You can implement the [`Meter`][meter] trait to allow custom metrics.
    ///
    /// [meter]: trait.Meter.html
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::{LruCache, Meter};
    /// use std::borrow::Borrow;
    ///
    /// /// Measure Vec items by their length
    /// struct VecLen;
    ///
    /// impl<K, T> Meter<K, Vec<T>> for VecLen {
    ///     // Use `Measure = usize` or implement `CountableMeter` as well.
    ///     type Measure = usize;
    ///     fn measure<Q: ?Sized>(&self, _: &Q, v: &Vec<T>) -> usize
    ///         where K: Borrow<Q>
    ///     {
    ///         v.len()
    ///     }
    /// }
    ///
    /// let mut cache = LruCache::with_meter(5, VecLen);
    /// cache.insert(1, vec![1, 2]);
    /// assert_eq!(cache.size(), 2);
    /// cache.insert(2, vec![3, 4]);
    /// cache.insert(3, vec![5, 6]);
    /// assert_eq!(cache.size(), 4);
    /// assert_eq!(cache.len(), 2);
    /// ```
    pub fn with_meter(capacity: u64, meter: M) -> LruCache<K, V, DefaultHashBuilder, M> {
        LruCache {
            map: LinkedHashMap::new(),
            current_measure: Default::default(),
            max_capacity: capacity,
            meter,
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> LruCache<K, V, S, Count> {
    /// Creates an empty cache that can hold at most `capacity` items with the given hash builder.
    pub fn with_hasher(capacity: u64, hash_builder: S) -> LruCache<K, V, S, Count> {
        LruCache {
            map: LinkedHashMap::with_hasher(hash_builder),
            current_measure: (),
            max_capacity: capacity,
            meter: Count,
        }
    }

    /// Returns a mutable reference to the value corresponding to the given key in the cache, if
    /// any.
    ///
    /// Note that this method is not available for cache objects using `Meter` implementations
    /// other than `Count`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, "a");
    /// cache.insert(2, "b");
    /// cache.insert(2, "c");
    /// cache.insert(3, "d");
    ///
    /// assert_eq!(cache.get_mut(&1), None);
    /// assert_eq!(cache.get_mut(&2), Some(&mut "c"));
    /// ```
    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get_refresh(k)
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
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, 10);
    /// cache.insert(2, 20);
    /// cache.insert(3, 30);
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
    /// assert_eq!(cache.get_mut(&2), Some(&mut 200));
    /// assert_eq!(cache.get_mut(&3), Some(&mut 300));
    /// ```
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        self.internal_iter_mut()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, M: CountableMeter<K, V>> LruCache<K, V, S, M> {
    /// Creates an empty cache that can hold at most `capacity` as measured by `meter` with the
    /// given hash builder.
    pub fn with_meter_and_hasher(capacity: u64, meter: M, hash_builder: S) -> Self {
        LruCache {
            map: LinkedHashMap::with_hasher(hash_builder),
            current_measure: Default::default(),
            max_capacity: capacity,
            meter,
        }
    }

    /// Returns the maximum size of the key-value pairs the cache can hold, as measured by the
    /// `Meter` used by the cache.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    /// let mut cache: LruCache<i32, &str> = LruCache::new(2);
    /// assert_eq!(cache.capacity(), 2);
    /// ```
    pub fn capacity(&self) -> u64 {
        self.max_capacity
    }

    /// Checks if the map contains the given key.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(1);
    ///
    /// cache.insert(1, "a");
    /// assert!(cache.contains_key(&1));
    /// ```
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.contains_key(key)
    }

    pub fn get<Q: ?Sized>(&mut self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get_refresh(k).map(|v| v as &V)
    }

    /// Inserts a key-value pair into the cache. If the key already existed, the old value is
    /// returned.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, "a");
    /// cache.insert(2, "b");
    /// assert_eq!(cache.get_mut(&1), Some(&mut "a"));
    /// assert_eq!(cache.get_mut(&2), Some(&mut "b"));
    /// ```
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let new_size = self.meter.measure(&k, &v);
        self.current_measure = self.meter.add(self.current_measure, new_size);
        if let Some(old) = self.map.get(&k) {
            self.current_measure = self
                .meter
                .sub(self.current_measure, self.meter.measure(&k, old));
        }
        let old_val = self.map.insert(k, v);
        while self.size() > self.capacity() {
            self.remove_lru();
        }
        old_val
    }

    /// Removes the given key from the cache and returns its corresponding value.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(2, "a");
    ///
    /// assert_eq!(cache.remove(&1), None);
    /// assert_eq!(cache.remove(&2), Some("a"));
    /// assert_eq!(cache.remove(&2), None);
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.remove(k).map(|v| {
            self.current_measure = self
                .meter
                .sub(self.current_measure, self.meter.measure(k, &v));
            v
        })
    }

    /// Sets the size of the key-value pairs the cache can hold, as measured by the `Meter` used by
    /// the cache.
    ///
    /// Removes least-recently-used key-value pairs if necessary.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, "a");
    /// cache.insert(2, "b");
    /// cache.insert(3, "c");
    ///
    /// assert_eq!(cache.get_mut(&1), None);
    /// assert_eq!(cache.get_mut(&2), Some(&mut "b"));
    /// assert_eq!(cache.get_mut(&3), Some(&mut "c"));
    ///
    /// cache.set_capacity(3);
    /// cache.insert(1, "a");
    /// cache.insert(2, "b");
    ///
    /// assert_eq!(cache.get_mut(&1), Some(&mut "a"));
    /// assert_eq!(cache.get_mut(&2), Some(&mut "b"));
    /// assert_eq!(cache.get_mut(&3), Some(&mut "c"));
    ///
    /// cache.set_capacity(1);
    ///
    /// assert_eq!(cache.get_mut(&1), None);
    /// assert_eq!(cache.get_mut(&2), None);
    /// assert_eq!(cache.get_mut(&3), Some(&mut "c"));
    /// ```
    pub fn set_capacity(&mut self, capacity: u64) {
        while self.size() > capacity {
            self.remove_lru();
        }
        self.max_capacity = capacity;
    }

    /// Removes and returns the least recently used key-value pair as a tuple.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, "a");
    /// cache.insert(2, "b");
    ///
    /// assert_eq!(cache.remove_lru(), Some((1, "a")));
    /// assert_eq!(cache.len(), 1);
    /// ```
    #[inline]
    pub fn remove_lru(&mut self) -> Option<(K, V)> {
        self.map.pop_front().map(|(k, v)| {
            self.current_measure = self
                .meter
                .sub(self.current_measure, self.meter.measure(&k, &v));
            (k, v)
        })
    }

    /// Returns the number of key-value pairs in the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns the size of all the key-value pairs in the cache, as measured by the `Meter` used
    /// by the cache.
    pub fn size(&self) -> u64 {
        self.meter
            .size(self.current_measure)
            .unwrap_or_else(|| self.map.len() as u64)
    }

    /// Returns `true` if the cache contains no key-value pairs.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Removes all key-value pairs from the cache.
    pub fn clear(&mut self) {
        self.map.clear();
        self.current_measure = Default::default();
    }

    /// Returns an iterator over the cache's key-value pairs in least- to most-recently-used order.
    ///
    /// Accessing the cache through the iterator does _not_ affect the cache's LRU state.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use common_cache::LruCache;
    ///
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.insert(1, 10);
    /// cache.insert(2, 20);
    /// cache.insert(3, 30);
    ///
    /// let kvs: Vec<_> = cache.iter().collect();
    /// assert_eq!(kvs, [(&2, &20), (&3, &30)]);
    /// ```
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter(self.map.iter())
    }

    fn internal_iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut(self.map.iter_mut())
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, M: CountableMeter<K, V>> Extend<(K, V)>
    for LruCache<K, V, S, M>
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

impl<K: fmt::Debug + Eq + Hash, V: fmt::Debug, S: BuildHasher, M: CountableMeter<K, V>> fmt::Debug
    for LruCache<K, V, S, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter().rev()).finish()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, M: CountableMeter<K, V>> IntoIterator
    for LruCache<K, V, S, M>
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter(self.map.into_iter())
    }
}

impl<'a, K: Eq + Hash, V, S: BuildHasher, M: CountableMeter<K, V>> IntoIterator
    for &'a LruCache<K, V, S, M>
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<'a, K: Eq + Hash, V, S: BuildHasher, M: CountableMeter<K, V>> IntoIterator
    for &'a mut LruCache<K, V, S, M>
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;
    fn into_iter(self) -> IterMut<'a, K, V> {
        self.internal_iter_mut()
    }
}

/// An iterator over a cache's key-value pairs in least- to most-recently-used order.
///
/// # Examples
///
/// ```rust,ignore
/// use common_cache::LruCache;
///
/// let mut cache = LruCache::new(2);
///
/// cache.insert(1, 10);
/// cache.insert(2, 20);
/// cache.insert(3, 30);
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
