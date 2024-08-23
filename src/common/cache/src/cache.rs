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

pub mod lru;

use std::borrow::Borrow;
use std::hash::Hash;

use crate::mem_sized::MemSized;

/// A trait for a cache.
pub trait Cache<K: Eq + Hash + MemSized, V: MemSized> {
    /// Returns a reference to the value corresponding to the given key in the cache, if
    /// any.
    fn get<Q>(&mut self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Inserts a key-value pair into the cache. If the key already existed, the old value is
    /// returned.
    fn insert(&mut self, k: K, v: V) -> Option<V>;

    /// Removes the given key from the cache and returns its corresponding value.
    fn pop<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Removes and returns the key-value pair as a tuple by policy (Lru, Lfu, etc.).
    fn pop_by_policy(&mut self) -> Option<(K, V)>;

    /// Checks if the map contains the given key.
    fn contains<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Returns the number of key-value pairs in the cache.
    fn len(&self) -> usize;

    /// Returns `true` if the cache contains no key-value pairs.
    fn is_empty(&self) -> bool;

    /// Returns the maximum bytes size of the key-value pairs the cache can hold.
    fn bytes_capacity(&self) -> u64;

    fn items_capacity(&self) -> u64;

    /// Returns the bytes size of all the key-value pairs in the cache.
    fn bytes_size(&self) -> u64;

    /// Removes all key-value pairs from the cache.
    fn clear(&mut self);
}
