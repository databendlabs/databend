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

use core::fmt::Debug;
use core::ops::Bound;
use core::ops::Range;
use std::collections::BTreeMap;

use super::range_key::RangeKey;

#[derive(Clone, Debug, Default)]
pub struct RangeMap<T, K, V> {
    pub(crate) map: BTreeMap<RangeKey<T, K>, V>,
}

impl<T, K, V> RangeMap<T, K, V>
where
    T: Ord + Clone + Debug,
    K: Ord + Clone + Debug + Default,
{
    pub fn new() -> Self {
        RangeMap {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, range: Range<T>, key: K, val: V) {
        assert!(range.start <= range.end);

        let range_key: RangeKey<T, K> = RangeKey::new(range, key);

        self.map.insert(range_key, val);
    }

    // Return a vector of `RangeKey` which contain the point in the [start, end).
    // If we have range [1,5],[2,4],[2,6], then:
    // 1. `get_by_point(1)` return [1,5]
    // 2. `get_by_point(2)` return [1,5],[2,4],[2,6]
    // 3. `get_by_point(5)` return [2,4],[2,6]
    // Use the default key when construct `RangeKey::key` for search.
    pub fn get_by_point(&self, point: &T) -> Vec<(&RangeKey<T, K>, &V)> {
        let key = point.clone();
        let range_key = RangeKey::new(key.clone()..key.clone(), K::default());

        self.map
            .range((Bound::Included(range_key), Bound::Unbounded))
            .filter(|e| e.0.range.start <= key)
            .collect()
    }

    pub fn remove(&mut self, range: Range<T>, k: K) {
        self.map.remove(&RangeKey::new(range, k));
    }

    pub fn remove_by_key(&mut self, key: &RangeKey<T, K>) {
        self.map.remove(key);
    }
}
