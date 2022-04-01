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
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use common_range_map::RangeWrapper;

#[cfg(test)]

fn upsert_cmp_map(map: &mut BTreeMap<String, BTreeSet<String>>, k: String, v: String) {
    if map.get(&k).is_none() {
        map.insert(k.clone(), BTreeSet::new());
    }
    if let Some(set) = map.get_mut(&k) {
        set.insert(v);
    }
}

/// test if or not RangeWrapper satisfy reflexive property
#[test]
fn test_range_wrapper_reflexive_property() {
    println!(
        "11 ret: {:?} {:?} {:?}",
        (2, 2).cmp(&(2, 4)),
        (2, 2).cmp(&(2, 6)),
        (2, 2).cmp(&(1, 5))
    );

    let tests = vec![
        RangeWrapper::new(2..4),
        RangeWrapper::new(0..1),
        RangeWrapper::new(1..2),
        RangeWrapper::new(2..3),
        RangeWrapper::new(3..4),
        RangeWrapper::new(1..4),
        RangeWrapper::new(1..5),
        RangeWrapper::new(2..6),
        RangeWrapper::new(3..6),
        RangeWrapper::new(4..6),
        RangeWrapper::new(5..6),
    ];
    let mut less_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut greater_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    // test antisymmetric propery and construct {less|greater}_map
    // antisymmetric propery: if a > b then b > a.
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
            assert!(greater_map.get(g).is_some());
            if let Some(set) = greater_map.get_mut(g) {
                assert!(set.get(k).is_some());
            }
        }
    }
}
