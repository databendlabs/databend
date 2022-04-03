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
use core::ops::Bound;
use core::ops::Range;
use std::collections::BTreeSet;

use super::range_key::RangeKey;

#[derive(Clone, Debug, Default)]
pub struct RangeSet<T, K> {
    pub(crate) set: BTreeSet<RangeKey<T, K>>,
}

impl<T, K> RangeSet<T, K>
where
    T: Ord + Clone + std::fmt::Debug + Copy,
    K: Ord + Clone + std::fmt::Debug + Copy + Default,
{
    pub fn new() -> Self {
        RangeSet {
            set: BTreeSet::new(),
        }
    }

    pub fn insert(&mut self, range: Range<T>, key: K) {
        assert!(range.start <= range.end);

        let range_key: RangeKey<T, K> = RangeKey::new(range, key);

        self.set.insert(range_key);
    }

    // Return a vector of (range,value) which contain the key in the [start, end).
    // If we have range [1,5],[2,4],[2,6], then:
    // 1. `get_by_key_range(1)` return [1,5]
    // 2. `get_by_key_range(2)` return [1,5],[2,4],[2,6]
    // 3. `get_by_key_range(5)` return [2,4],[2,6]
    // Use the default key when construct `RangeKey::key` for search.
    pub fn get_by_key_range(&self, key: &T) -> Vec<&RangeKey<T, K>> {
        let key_as_start = RangeKey::new(*key..*key, K::default());

        self.set
            .range((Bound::Included(key_as_start), Bound::Unbounded))
            .filter(|e| e.range.start.cmp(key) != Ordering::Greater)
            .collect()
    }

    pub fn remove(&mut self, range: Range<T>, k: K) {
        self.set.remove(&RangeKey::new(range, k));
    }
}
