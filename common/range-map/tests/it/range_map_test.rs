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

use common_range_map::RangeMap;
use common_range_map::RangeWrapper;

#[cfg(test)]

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
    // test get_by_key
    {
        let mut a = RangeMap::new();

        a.insert(1..5, 15);
        a.insert(2..4, 24);
        a.insert(2..6, 26);

        let ret1 = vec![(RangeWrapper::new(1..5), 15)];
        assert!(test_vec_rangewrapper(a.get_by_key(&1), ret1)); // [(1..5, 15)]

        let ret2 = vec![
            (RangeWrapper::new(1..5), 15),
            (RangeWrapper::new(2..4), 24),
            (RangeWrapper::new(2..6), 26),
        ];
        assert!(test_vec_rangewrapper(a.get_by_key(&2), ret2)); // [(1..5, 15), ((2..4), 24), ((2..6), 26)]

        let ret3 = vec![(RangeWrapper::new(2..6), 26)];
        assert!(test_vec_rangewrapper(a.get_by_key(&5), ret3)); // [((2..6), 26)]
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
        assert!(test_vec_rangewrapper(a.get_by_key(&1), ret1)); // [(1..5, 15)]

        a.remove(1..5);
        let ret = a.get_by_key(&1);
        assert!(ret.is_empty());
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
        assert!(test_vec_rangewrapper(a.get_by_key(&1), ret)); // [(1..5, 15)]
    }
}
