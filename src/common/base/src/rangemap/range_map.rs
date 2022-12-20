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

use super::range_map_key::RangeMapKey;

#[derive(Clone, Debug, Default)]
pub struct RangeMap<T, K, V> {
    pub(crate) map: BTreeMap<RangeMapKey<T, K>, V>,
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

        let range_key: RangeMapKey<T, K> = RangeMapKey::new(range, key);

        self.map.insert(range_key, val);
    }

    // Return a vector of `RangeKey` which contain the point in the [start, end).
    // If we have range [1,5],[2,4],[2,6], then:
    // 1. `get_by_point(1)` return [1,5]
    // 2. `get_by_point(2)` return [1,5],[2,4],[2,6]
    // 3. `get_by_point(5)` return [2,4],[2,6]
    // Use the default key when construct `RangeKey::key` for search.
    pub fn get_by_point(&self, point: &T) -> Vec<(&RangeMapKey<T, K>, &V)> {
        let key = point.clone();
        let range_key = RangeMapKey::new(key.clone()..key.clone(), K::default());

        self.map
            .range((Bound::Included(range_key), Bound::Unbounded))
            .filter(|e| e.0.range.start <= key)
            .collect()
    }

    pub fn remove(&mut self, range: Range<T>, k: K) {
        self.map.remove(&RangeMapKey::new(range, k));
    }

    pub fn remove_by_key(&mut self, key: &RangeMapKey<T, K>) {
        self.map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::rangemap::RangeMap;
    use crate::rangemap::RangeMapKey;

    //
    #[test]
    fn test_range_set() {
        // test get_by_point for i32
        {
            let mut a = RangeMap::new();

            let r11 = (&RangeMapKey::new(1..1, 11), &11);
            let r15 = (&RangeMapKey::new(1..5, 15), &15);
            let r24 = (&RangeMapKey::new(2..4, 24), &24);
            let r26 = (&RangeMapKey::new(2..6, 26), &26);

            a.insert(1..1, 11, 11);
            a.insert(1..5, 15, 15);
            a.insert(2..4, 24, 24);
            a.insert(2..6, 26, 26);

            assert_eq!(a.get_by_point(&1), vec![r11, r15]);
            assert_eq!(a.get_by_point(&2), vec![r24, r15, r26]);
            assert_eq!(a.get_by_point(&5), vec![r26]);

            a.remove(1..5, 15);
            assert_eq!(a.get_by_point(&1), vec![r11]);
            assert_eq!(a.get_by_point(&2), vec![r24, r26]);
        }
        // test get_by_point for String
        {
            let mut a = RangeMap::new();

            let a1 = "1".to_string();
            let a2 = "2".to_string();
            let a4 = "4".to_string();
            let a5 = "5".to_string();
            let a6 = "6".to_string();

            let r11 = (&RangeMapKey::new(a1.clone()..a1.clone(), 11), &11);
            let r15 = (&RangeMapKey::new(a1.clone()..a5.clone(), 15), &15);
            let r24 = (&RangeMapKey::new(a2.clone()..a4.clone(), 24), &24);
            let r26 = (&RangeMapKey::new(a2.clone()..a6.clone(), 26), &26);

            a.insert(a1.clone()..a1.clone(), 11, 11);
            a.insert(a1.clone()..a5.clone(), 15, 15);
            a.insert(a2.clone()..a4, 24, 24);
            a.insert(a2.clone()..a6, 26, 26);

            assert_eq!(a.get_by_point(&a1), vec![r11, r15]);
            assert_eq!(a.get_by_point(&a2), vec![r24, r15, r26]);
            assert_eq!(a.get_by_point(&a5), vec![r26]);

            a.remove(a1.clone()..a5, 15);
            assert_eq!(a.get_by_point(&a1), vec![r11]);
            assert_eq!(a.get_by_point(&a2), vec![r24, r26]);
        }
        // test get_by_point for string prefix
        {
            let mut a = RangeMap::new();

            let a1 = "11".to_string();
            let a2 = "12".to_string();

            a.insert(a1..a2, 11, 11);
            assert!(!a.get_by_point(&"11".to_string()).is_empty());
            assert!(!a.get_by_point(&"111".to_string()).is_empty());
            assert!(!a.get_by_point(&"11z".to_string()).is_empty());
            assert!(!a.get_by_point(&"11/".to_string()).is_empty());
            assert!(!a.get_by_point(&"11*".to_string()).is_empty());
            assert!(a.get_by_point(&"12".to_string()).is_empty());
        }
        // test get_by_point for char upbound limit string prefix
        {
            let mut a = RangeMap::new();

            let a1 = format!("{}", 255 as char);
            let a2 = format!("{}{}", 255 as char, 255 as char);

            a.insert(a1..a2, 11, 11);
            assert!(!a.get_by_point(&format!("{}", 255 as char)).is_empty());
            assert!(!a.get_by_point(&format!("{}z", 255 as char)).is_empty());
            assert!(!a.get_by_point(&format!("{}/", 255 as char)).is_empty());
            assert!(!a.get_by_point(&format!("{}*", 255 as char)).is_empty());
        }
        // test get_by_point for char upbound limit string prefix
        {
            let mut a = RangeMap::new();

            let a1 = "1".to_string();
            let a2 = format!("{}{}", a1, 255 as char);

            a.insert(a1.clone()..a2, 11, 11);
            assert!(!a.get_by_point(&a1).is_empty());
            assert!(!a.get_by_point(&format!("{}z", a1)).is_empty());
            assert!(!a.get_by_point(&format!("{}*", a1)).is_empty());
            assert!(!a.get_by_point(&format!("{}/", a1)).is_empty());
        }
    }
}
