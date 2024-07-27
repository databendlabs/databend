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

use core::cmp::Ordering;
use core::ops::Range;
use std::fmt::Debug;

/// `RangeMapKey` is a wrapper of `range` and `user defined key`
#[derive(Eq, Debug, Clone, PartialEq)]
pub struct RangeMapKey<RV, ID> {
    // range
    pub range: Range<RV>,
    // user defined key
    pub key: ID,
}

impl<RV, ID> RangeMapKey<RV, ID>
where
    RV: Eq + Ord,
    ID: Eq + Ord + Default,
{
    pub fn new(range: Range<RV>, key: ID) -> RangeMapKey<RV, ID> {
        RangeMapKey { range, key }
    }
}

impl<RV, ID> ToString for RangeMapKey<RV, ID>
where
    RV: Debug,
    ID: Debug,
{
    fn to_string(&self) -> String {
        format!("{:?}-{:?}-{:?}", self.range.start, self.range.end, self.key)
    }
}

impl<RV, ID> Ord for RangeMapKey<RV, ID>
where
    RV: Ord + Debug + Clone,
    ID: Ord + Debug + Clone,
{
    /// the compare weight is: range.end > range.start > key
    /// example: ((2,3),5) < ((5,1),3) since 2 < 5
    fn cmp(&self, other: &RangeMapKey<RV, ID>) -> Ordering {
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

impl<RV, ID> PartialOrd for RangeMapKey<RV, ID>
where
    RV: Ord + Debug + Clone,
    ID: Ord + Debug + Clone,
{
    fn partial_cmp(&self, other: &RangeMapKey<RV, ID>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {

    use core::cmp::Ordering;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use crate::rangemap::RangeMapKey;

    fn upsert_cmp_map(map: &mut BTreeMap<String, BTreeSet<String>>, k: String, v: String) {
        if map.get(&k).is_none() {
            map.insert(k.clone(), BTreeSet::new());
        }
        if let Some(set) = map.get_mut(&k) {
            set.insert(v);
        }
    }

    /// test if or not RangeKey satisfy reflexive property
    #[test]
    fn test_range_wrapper_reflexive_property() {
        let mut tests = vec![];
        for i in 0..10 {
            tests.push(RangeMapKey::new(0..i, ()));
        }

        let mut less_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut greater_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        // test antisymmetric property and construct {less|greater}_map
        // antisymmetric property: if a > b then b > a.
        for i in tests.iter() {
            for j in tests.clone().iter() {
                let ret_i_j = i.cmp(j);
                let ret_j_i = j.cmp(i);

                match ret_i_j {
                    Ordering::Equal => {
                        assert_eq!(ret_j_i, Ordering::Equal);
                    }
                    Ordering::Less => {
                        assert_eq!(ret_j_i, Ordering::Greater);
                        upsert_cmp_map(&mut less_map, i.to_string(), j.to_string());
                    }
                    Ordering::Greater => {
                        assert_eq!(ret_j_i, Ordering::Less);
                        upsert_cmp_map(&mut greater_map, i.to_string(), j.to_string());
                    }
                }
            }
        }

        // prove transitive property: if a<b and b<c, then a<c
        for (k, v) in less_map.iter() {
            for g in v.iter() {
                assert!(greater_map.contains_key(g));
                if let Some(set) = greater_map.get_mut(g) {
                    assert!(set.get(k).is_some());
                }
            }
        }
    }
}
