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

use common_base::rangemap::RangeMap;
use common_base::rangemap::RangeMapKey;

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
}
