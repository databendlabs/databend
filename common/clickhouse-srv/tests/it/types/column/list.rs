// Copyright 2020 Datafuse Labs.
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

use std::f64::EPSILON;

use common_clickhouse_srv::types::column::*;
use rand::random;

#[test]
fn test_push_and_len() {
    let mut list = List::with_capacity(100_500);

    for i in 0..100_500 {
        assert_eq!(list.len(), i as usize);
        list.push(i);
    }
}

#[test]
fn test_push_and_get() {
    let mut list = List::<f64>::new();
    let mut vs = vec![0.0_f64; 100];

    for (count, _) in (0..100).enumerate() {
        assert_eq!(list.len(), count);

        for (i, v) in vs.iter().take(count).enumerate() {
            assert!((list.at(i) - *v).abs() < EPSILON);
        }

        let k = random();
        list.push(k);
        vs[count] = k;
    }
}
