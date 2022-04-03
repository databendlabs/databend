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

use common_range_set::RangeKey;
use common_range_set::RangeSet;

#[cfg(test)]
#[test]
fn test_range_set() {
    // test get_by_key_range
    {
        let mut a = RangeSet::new();

        let r11 = &RangeKey::new(1..1, 11);
        let r15 = &RangeKey::new(1..5, 15);
        let r24 = &RangeKey::new(2..4, 24);
        let r26 = &RangeKey::new(2..6, 26);

        a.insert(1..1, 11);
        a.insert(1..5, 15);
        a.insert(2..4, 24);
        a.insert(2..6, 26);

        assert_eq!(a.get_by_key_range(&1), vec![r11, r15]);
        assert_eq!(a.get_by_key_range(&2), vec![r24, r15, r26]);
        assert_eq!(a.get_by_key_range(&5), vec![r26]);

        a.remove(1..5, 15);
        assert_eq!(a.get_by_key_range(&1), vec![r11]);
        assert_eq!(a.get_by_key_range(&2), vec![r24, r26]);
    }
}
