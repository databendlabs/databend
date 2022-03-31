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

use core::cmp::Ordering;
use core::fmt::Debug;
use core::fmt::{self};
use core::ops::Bound;
use core::ops::Range;
use std::collections::BTreeMap;

use super::range_wrapper::RangeWrapper;

#[derive(Clone)]
pub struct RangeMap<K, V> {
    // Wrap ranges so that they are `Ord`.
    // See `range_wrapper.rs` for explanation.
    pub(crate) btm: BTreeMap<RangeWrapper<K>, V>,
}

impl<K, V> Default for RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug,
    V: Eq + Clone + std::fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct Iter<'a, K, V> {
    inner: std::collections::btree_map::Iter<'a, RangeWrapper<K>, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    type Item = (&'a Range<K>, &'a V);

    fn next(&mut self) -> Option<(&'a Range<K>, &'a V)> {
        self.inner.next().map(|(by_start, v)| (&by_start.range, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K: Debug, V: Debug> Debug for RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug,
    V: Eq + Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K, V> RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug,
    V: Eq + Clone + std::fmt::Debug,
{
    /// Makes a new empty `RangeMap`.
    pub fn new() -> Self {
        RangeMap {
            btm: BTreeMap::new(),
        }
    }

    pub fn contain_key(&self, key: &K) -> Vec<(&RangeWrapper<K>, &V)> {
        let key_as_start = RangeWrapper::new(key.clone()..key.clone());

        self.btm
            .range((Bound::Included(key_as_start.clone()), Bound::Unbounded))
            .filter_map(|e| {
                let start = e.0.range.start.cmp(&key_as_start.range.start);
                let end = e.0.range.end.cmp(&key_as_start.range.start);

                if start == Ordering::Greater {
                    return None;
                }
                if end == Ordering::Less {
                    return None;
                }

                Some(e)
            })
            .collect()
    }

    pub fn insert(&mut self, range: Range<K>, value: V) {
        // We don't want to have to make empty ranges make sense;
        // they don't represent anything meaningful in this structure.
        assert!(range.start < range.end);

        // Wrap up the given range so that we can "borrow"
        // it as a wrapper reference to either its start or end.
        // See `range_wrapper.rs` for explanation of these hacks.
        let new_range_start_wrapper: RangeWrapper<K> = RangeWrapper::new(range);
        let new_value = value;

        // Insert the (possibly expanded) new range, and we're done!
        self.btm.insert(new_range_start_wrapper, new_value);
    }

    pub fn upinsert<F>(&mut self, range: Range<K>, value: V, f: &mut F)
    where F: for<'a> FnMut(&'a mut V, &'a V) -> V {
        // We don't want to have to make empty ranges make sense;
        // they don't represent anything meaningful in this structure.
        assert!(range.start < range.end);

        // Wrap up the given range so that we can "borrow"
        // it as a wrapper reference to either its start or end.
        // See `range_wrapper.rs` for explanation of these hacks.
        let new_range_start_wrapper: RangeWrapper<K> = RangeWrapper::new(range);
        let new_value = value;

        // Insert the (possibly expanded) new range, and we're done!
        let old_value = self.btm.get_mut(&new_range_start_wrapper);
        match old_value {
            None => {
                self.btm.insert(new_range_start_wrapper, new_value);
            }
            Some(old_value) => {
                let new_value = f(old_value, &new_value);
                self.btm.insert(new_range_start_wrapper, new_value);
            }
        };
    }

    pub fn remove(&mut self, range: Range<K>) {
        self.btm.remove(&RangeWrapper::new(range));
    }

    pub fn get(&mut self, range: Range<K>) -> Option<&V> {
        self.btm.get(&RangeWrapper::new(range))
    }

    pub fn get_mut(&mut self, range: Range<K>) -> Option<&mut V> {
        self.btm.get_mut(&RangeWrapper::new(range))
    }

    /// Gets an iterator over all pairs of key range and value,
    /// ordered by key range.
    ///
    /// The iterator element type is `(&'a Range<K>, &'a V)`.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            inner: self.btm.iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vec_rangewrapper<K, V>(
        ret1: Vec<(&RangeWrapper<K>, &V)>,
        ret2: Vec<(RangeWrapper<K>, V)>,
    ) -> bool
    where
        K: Ord + Clone + std::fmt::Debug,
        V: Eq + Clone + std::fmt::Debug,
    {
        if ret1.len() != ret2.len() {
            return false;
        }

        for i in 0..ret1.len() {
            if *ret1[i].0 != ret2[i].0 {
                return false;
            }

            if *ret1[i].1 != ret2[i].1 {
                return false;
            }
        }

        true
    }

    #[test]
    fn test_range_map() {
        // test contain_key
        {
            let mut a = RangeMap::new();

            a.insert(1..5, 15);
            a.insert(2..4, 24);
            a.insert(2..6, 26);

            let ret1 = vec![(RangeWrapper::new(1..5), 15)];
            assert_eq!(test_vec_rangewrapper(a.contain_key(&1), ret1), true); // [(1..5, 15)]

            let ret2 = vec![
                (RangeWrapper::new(2..4), 24),
                (RangeWrapper::new(1..5), 15),
                (RangeWrapper::new(2..6), 26),
            ];
            assert_eq!(test_vec_rangewrapper(a.contain_key(&2), ret2), true); // [(1..5, 15), ((2..4), 24), ((2..6), 26)]

            let ret3 = vec![(RangeWrapper::new(2..6), 26)];
            assert_eq!(test_vec_rangewrapper(a.contain_key(&5), ret3), true); // [((2..6), 26)]
        }

        // test get/get_mut/remove
        {
            let mut a = RangeMap::new();

            a.insert(1..5, 15);

            {
                assert!(a.get(1..5).is_some());
                assert!(a.get_mut(1..5).is_some());
                assert!(a.get_mut(1..1).is_none());
            }

            let ret1 = vec![(RangeWrapper::new(1..5), 15)];
            assert_eq!(test_vec_rangewrapper(a.contain_key(&1), ret1), true); // [(1..5, 15)]

            a.remove(1..5);
            let ret = a.contain_key(&1);
            assert_eq!(ret.is_empty(), true);
        }

        // test upinsert
        {
            let mut a = RangeMap::new();
            a.insert(1..5, vec![1, 2]);
            a.upinsert(1..5, vec![3, 4], &mut |old, new| {
                for i in new.iter() {
                    old.push(*i);
                }
                old.to_vec()
            });

            let ret = vec![(RangeWrapper::new(1..5), vec![1, 2, 3, 4])];
            assert_eq!(test_vec_rangewrapper(a.contain_key(&1), ret), true); // [(1..5, 15)]
        }
    }
}
