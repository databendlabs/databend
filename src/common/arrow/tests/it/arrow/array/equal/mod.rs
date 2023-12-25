// Copyright 2020-2022 Jorge C. Leitão
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

use databend_common_arrow::arrow::array::*;

mod dictionary;
mod fixed_size_list;
mod list;
mod primitive;
mod utf8;

pub fn test_equal(lhs: &dyn Array, rhs: &dyn Array, expected: bool) {
    // equality is symmetric
    assert!(equal(lhs, lhs), "\n{lhs:?}\n{lhs:?}");
    assert!(equal(rhs, rhs), "\n{rhs:?}\n{rhs:?}");

    assert_eq!(equal(lhs, rhs), expected, "\n{lhs:?}\n{rhs:?}");
    assert_eq!(equal(rhs, lhs), expected, "\n{rhs:?}\n{lhs:?}");
}

#[allow(clippy::type_complexity)]
fn binary_cases() -> Vec<(Vec<Option<String>>, Vec<Option<String>>, bool)> {
    let base = vec![
        Some("hello".to_owned()),
        None,
        None,
        Some("world".to_owned()),
        None,
        None,
    ];
    let not_base = vec![
        Some("hello".to_owned()),
        Some("foo".to_owned()),
        None,
        Some("world".to_owned()),
        None,
        None,
    ];
    vec![
        (
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            true,
        ),
        (
            vec![Some("hello".to_owned()), Some("world".to_owned())],
            vec![Some("hello".to_owned()), Some("arrow".to_owned())],
            false,
        ),
        (base.clone(), base.clone(), true),
        (base, not_base, false),
    ]
}
