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

pub mod lru;
#[cfg(test)]
mod lru_test;

use std::borrow::Borrow;
use std::hash::Hash;

/// A trait for a cache.
pub trait Cache<K, V>
where K: Eq + Hash
{
    fn get<'a, Q>(&'a mut self, k: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn get_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn peek<'a, Q>(&'a self, k: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn peek_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn peek_by_policy<'a>(&self) -> Option<(&'a K, &'a V)>;
    fn put(&mut self, k: K, v: V) -> Option<V>;
    fn pop<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn pop_by_policy(&mut self) -> Option<(K, V)>;
    fn contains<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn capacity(&self) -> u64;
    fn set_capacity(&mut self, cap: u64);
    fn size(&self) -> u64;
    fn clear(&mut self);
}
