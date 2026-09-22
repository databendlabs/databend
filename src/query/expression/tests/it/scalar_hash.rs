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

//! `Scalar`/`ScalarRef` are used as `HashSet`/`HashMap` keys, so `Hash` must
//! agree with `PartialEq`: equal values hash identically, and values that only
//! differ by NULL position must not collide by construction.

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt8Type;

fn hash_of(scalar: &Scalar) -> u64 {
    let mut hasher = DefaultHasher::new();
    scalar.hash(&mut hasher);
    hasher.finish()
}

fn assert_pairwise_distinct(scalars: &[Scalar]) {
    for (i, left) in scalars.iter().enumerate() {
        for right in &scalars[i + 1..] {
            assert_ne!(left, right);
            assert_ne!(hash_of(left), hash_of(right), "{left:?} vs {right:?}");
        }
    }
}

#[test]
fn test_null_position_hashes_differ() {
    let columns = [
        UInt8Type::from_data_with_validity(vec![0, 1], vec![false, true]),
        UInt8Type::from_data_with_validity(vec![1, 0], vec![true, false]),
        UInt8Type::from_data_with_validity(vec![0, 1], vec![true, true]),
    ];
    let wrappers: [fn(Column) -> Scalar; 3] = [
        Scalar::Array,
        |column| Scalar::Tuple(column.iter().map(|value| value.to_owned()).collect()),
        |column| {
            Scalar::Map(Column::Tuple(vec![
                StringType::from_data(vec!["a", "b"]),
                column,
            ]))
        },
    ];
    for wrap in wrappers {
        let scalars = columns.iter().cloned().map(wrap).collect::<Vec<_>>();
        assert_pairwise_distinct(&scalars);
    }

    assert_pairwise_distinct(&[Scalar::Null, Scalar::EmptyArray, Scalar::EmptyMap]);
}
