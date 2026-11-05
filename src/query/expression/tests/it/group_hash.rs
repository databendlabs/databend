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

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::*;
use databend_common_io::HybridBitmap;
use roaring::RoaringTreemap;

fn repeated(scalar: Scalar, rows: usize) -> BlockEntry {
    BlockEntry::new_const_column(scalar.as_ref().infer_data_type(), scalar, rows)
}

fn scalar_hash(scalar: &Scalar) -> u64 {
    let mut hasher = DefaultHasher::new();
    scalar.hash(&mut hasher);
    hasher.finish()
}

// All rows in a class have the same logical value. No expected hash is derived
// from the hash implementation: compare representations and layouts instead.
fn assert_hash_class(scalars: &[Scalar]) -> databend_common_exception::Result<()> {
    let first = &scalars[0];
    let data_type = first.as_ref().infer_data_type();
    let mut builder = ColumnBuilder::with_capacity(&data_type, scalars.len() + 2);
    builder.push(first.as_ref());
    for scalar in scalars {
        assert_eq!(scalar, first);
        assert_eq!(scalar_hash(scalar), scalar_hash(first));
        builder.push(scalar.as_ref());
    }
    builder.push(first.as_ref());
    // Slice off padding so binary/array buffers and validity can have offsets.
    let column = builder.build().slice(1..scalars.len() + 1);
    let rows = scalars.len();
    let nullable = NullableColumn::new_column(column.clone(), vec![true; rows].into());
    let dictionary = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        NarrowBuffer::pack((0..rows).rev().map(|index| index as u64), (rows - 1) as u64),
        column.clone(),
    ))));
    let suffix = Int16Type::from_data(vec![4_i16; rows]);
    let entries = [
        BlockEntry::Column(column),
        BlockEntry::Column(nullable),
        BlockEntry::Column(dictionary),
    ]
    .into_iter()
    .chain(scalars.iter().cloned().flat_map(|value| {
        let entry = repeated(value.clone(), rows);
        let nullable = BlockEntry::new_const_column(data_type.clone().wrap_nullable(), value, rows);
        [entry, nullable]
    }))
    .collect::<Vec<_>>();

    for projection in [vec![0], vec![0, 1], vec![1, 0]] {
        let mut expected = None;
        for entry in &entries {
            let entry = entry.clone();
            let block = DataBlock::new(vec![entry, suffix.clone().into()], rows);
            let mut hashes = vec![u64::MAX; rows];
            group_hash_entries(ProjectedBlock::project(&projection, &block), &mut hashes);
            assert!(hashes.iter().all(|hash| *hash == hashes[0]));
            if let Some(expected) = &expected {
                assert_eq!(&hashes, expected);
            } else {
                expected = Some(hashes.clone());
            }

            // Hash each chunk independently, including an empty chunk.
            for range in [0..0, 0..1, 1..rows] {
                let chunk = block.slice(range.clone());
                let mut chunk_hashes = vec![17; range.len()];
                group_hash_entries(
                    ProjectedBlock::project(&projection, &chunk),
                    &mut chunk_hashes,
                );
                assert_eq!(chunk_hashes, hashes[range]);
            }

            let indices = [0_u32, (rows - 1) as u32];
            let mut spread = vec![u64::MAX; rows];
            for (key, &index) in projection.iter().enumerate() {
                group_hash_value_spread(
                    &indices,
                    block.get_by_offset(index).value(),
                    key == 0,
                    &mut spread,
                )?;
            }
            assert_eq!(spread[0], hashes[0]);
            assert_eq!(spread[rows - 1], hashes[rows - 1]);
            assert!(spread[1..rows - 1].iter().all(|hash| *hash == u64::MAX));
        }
    }
    Ok(())
}

fn assert_nested_hash_class(scalars: &[Scalar]) -> databend_common_exception::Result<()> {
    assert_hash_class(scalars)?;
    let arrays = scalars
        .iter()
        .cloned()
        .map(|scalar| Scalar::Array(repeated(scalar, 2).to_column()))
        .collect::<Vec<_>>();
    assert_hash_class(&arrays)?;
    let tuples = arrays
        .iter()
        .cloned()
        .map(|array| Scalar::Tuple(vec![Scalar::Null, array]))
        .collect::<Vec<_>>();
    assert_hash_class(&tuples)?;
    let maps = scalars
        .iter()
        .cloned()
        .map(|scalar| {
            Scalar::Map(Column::Tuple(vec![
                StringType::from_data(vec!["key"]),
                repeated(scalar, 1).to_column(),
            ]))
        })
        .collect::<Vec<_>>();
    assert_hash_class(&maps)
}

#[test]
fn bitmap_hashes_logical_members() -> databend_common_exception::Result<()> {
    for members in [vec![], vec![2_u64, 9], (0..40).collect()] {
        let tree = members.iter().copied().collect::<RoaringTreemap>();
        let hybrid = members.iter().copied().collect::<HybridBitmap>();
        let mut legacy = Vec::new();
        tree.serialize_into(&mut legacy).unwrap();
        let mut current = Vec::new();
        hybrid.serialize_into(&mut current).unwrap();
        // Both hybrid representations are valid even for a small member set.
        let mut large = Vec::new();
        HybridBitmap::Large(tree)
            .serialize_into(&mut large)
            .unwrap();
        let scalars = [
            Scalar::Bitmap(legacy),
            Scalar::Bitmap(current),
            Scalar::Bitmap(large),
        ];
        assert_hash_class(&scalars)?;
        // Nested bitmap comparisons currently use the inner column's bytes.
        // Check representation invariance without assuming semantic equality there.
        for scalar in scalars {
            assert_nested_hash_class(&[scalar.clone(), scalar])?;
        }
    }
    // Undecodable payloads retain bytewise hashing instead of becoming NULL.
    assert_hash_class(&[Scalar::Bitmap(vec![7]), Scalar::Bitmap(vec![7])])
}

#[test]
fn geometry_hashes_ignore_wkb_byte_order() -> databend_common_exception::Result<()> {
    // Independently construct WKB POINT(3 6), not a stored fixture.
    let mut little = vec![1_u8];
    little.extend_from_slice(&1_u32.to_le_bytes());
    little.extend_from_slice(&3_f64.to_le_bytes());
    little.extend_from_slice(&6_f64.to_le_bytes());
    let mut big = vec![0_u8];
    big.extend_from_slice(&1_u32.to_be_bytes());
    big.extend_from_slice(&3_f64.to_be_bytes());
    big.extend_from_slice(&6_f64.to_be_bytes());
    assert_ne!(little, big);
    assert_nested_hash_class(&[Scalar::Geometry(little), Scalar::Geometry(big)])
}

#[test]
fn interval_hashes_equal_durations() -> databend_common_exception::Result<()> {
    // The native interval equality contract treats a month as thirty days.
    assert_nested_hash_class(&[
        Scalar::Interval(months_days_micros::new(1, 0, 0)),
        Scalar::Interval(months_days_micros::new(0, 30, 0)),
        Scalar::Interval(months_days_micros::new(0, 29, 86_400_000_000)),
    ])
}

#[test]
fn nested_hashes_ignore_null_payloads() -> databend_common_exception::Result<()> {
    assert_nested_hash_class(&[
        Scalar::Array(Int16Type::from_data_with_validity(vec![12_i16, 5], vec![
            false, true,
        ])),
        Scalar::Array(Int16Type::from_data_with_validity(vec![-12_i16, 5], vec![
            false, true,
        ])),
    ])
}

#[test]
fn timestamp_tz_hashes_instants_not_offsets() -> databend_common_exception::Result<()> {
    assert_nested_hash_class(&[
        Scalar::TimestampTz(timestamp_tz::new(24, 0)),
        Scalar::TimestampTz(timestamp_tz::new(24, 7_200)),
        Scalar::TimestampTz(timestamp_tz::new(24, -7_200)),
    ])
}
