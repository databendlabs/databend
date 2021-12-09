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

use databend_query::common::Grower;

#[test]
fn test_hash_table_grower() {
    let mut grower = Grower::default();

    assert_eq!(grower.max_size(), 256);

    assert!(grower.overflow(129));
    assert!(!grower.overflow(128));

    assert_eq!(grower.place(1), 1);
    assert_eq!(grower.place(255), 255);
    assert_eq!(grower.place(256), 0);
    assert_eq!(grower.place(257), 1);

    assert_eq!(grower.next_place(1), 2);
    assert_eq!(grower.next_place(2), 3);
    assert_eq!(grower.next_place(254), 255);
    assert_eq!(grower.next_place(255), 0);

    grower.increase_size();
    assert_eq!(grower.max_size(), 1024);
}
