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
use databend_common_io::HybridBitmap;
use roaring::RoaringTreemap;

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

fn assert_equal_class(scalars: &[Scalar]) {
    let (first, rest) = scalars.split_first().unwrap();
    for other in rest {
        assert_eq!(first, other);
        assert_eq!(hash_of(first), hash_of(other), "{first:?} vs {other:?}");
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

#[test]
fn test_bitmap_hash_ignores_encoding() {
    let members = [1u64, 7, 1 << 40];

    let mut legacy = Vec::new();
    members
        .iter()
        .copied()
        .collect::<RoaringTreemap>()
        .serialize_into(&mut legacy)
        .unwrap();

    let mut hybrid = Vec::new();
    members
        .iter()
        .copied()
        .collect::<HybridBitmap>()
        .serialize_into(&mut hybrid)
        .unwrap();
    assert_ne!(legacy, hybrid);

    assert_equal_class(&[Scalar::Bitmap(legacy.clone()), Scalar::Bitmap(hybrid)]);

    let mut other = Vec::new();
    [1u64, 8]
        .into_iter()
        .collect::<HybridBitmap>()
        .serialize_into(&mut other)
        .unwrap();
    assert_pairwise_distinct(&[Scalar::Bitmap(legacy), Scalar::Bitmap(other)]);
}

#[test]
fn test_geometry_hash_ignores_byte_order() {
    // WKB POINT(1 2) in little-endian and big-endian byte order.
    let mut little = vec![1u8];
    little.extend_from_slice(&1u32.to_le_bytes());
    little.extend_from_slice(&1.0f64.to_le_bytes());
    little.extend_from_slice(&2.0f64.to_le_bytes());

    let mut big = vec![0u8];
    big.extend_from_slice(&1u32.to_be_bytes());
    big.extend_from_slice(&1.0f64.to_be_bytes());
    big.extend_from_slice(&2.0f64.to_be_bytes());
    assert_ne!(little, big);

    assert_equal_class(&[Scalar::Geometry(little.clone()), Scalar::Geometry(big)]);

    let mut other = vec![1u8];
    other.extend_from_slice(&1u32.to_le_bytes());
    other.extend_from_slice(&1.0f64.to_le_bytes());
    other.extend_from_slice(&3.0f64.to_le_bytes());
    assert_pairwise_distinct(&[Scalar::Geometry(little), Scalar::Geometry(other)]);
}
