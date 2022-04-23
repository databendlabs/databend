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
use core::ops::Range;
use std::fmt::Debug;

/// `RangeKey` is a wrapper of `range` and `user defined key`
#[derive(Eq, Debug, Clone, PartialEq)]
pub struct RangeKey<T, K> {
    // range
    pub range: Range<T>,
    // user defined key
    pub key: K,
}

impl<T, K> RangeKey<T, K>
where
    T: Eq + Ord,
    K: Eq + Ord + Default,
{
    pub fn new(range: Range<T>, key: K) -> RangeKey<T, K> {
        RangeKey { range, key }
    }
}

impl<T, K> ToString for RangeKey<T, K>
where
    T: Debug,
    K: Debug,
{
    fn to_string(&self) -> String {
        format!("{:?}-{:?}-{:?}", self.range.start, self.range.end, self.key)
    }
}

impl<T, K> Ord for RangeKey<T, K>
where
    T: Ord + Debug + Clone,
    K: Ord + Debug + Clone,
{
    /// the compare weight is: range.end > range.start > key
    /// example: ((2,3),5) < ((5,1),3) since 2 < 5
    fn cmp(&self, other: &RangeKey<T, K>) -> Ordering {
        let ret = self.range.end.cmp(&other.range.end);
        if !ret.is_eq() {
            return ret;
        }
        let ret = self.range.start.cmp(&other.range.start);
        if !ret.is_eq() {
            return ret;
        }
        self.key.cmp(&other.key)
    }
}

impl<T, K> PartialOrd for RangeKey<T, K>
where
    T: Ord + Debug + Clone,
    K: Ord + Debug + Clone,
{
    fn partial_cmp(&self, other: &RangeKey<T, K>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
