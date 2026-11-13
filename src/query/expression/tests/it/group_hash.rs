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

use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::*;
use databend_common_io::HybridBitmap;
use roaring::RoaringBitmap;
use roaring::RoaringTreemap;

fn repeated(scalar: Scalar, rows: usize) -> BlockEntry {
    BlockEntry::new_const_column(scalar.as_ref().infer_data_type(), scalar, rows)
}

fn scalar_hash(scalar: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    scalar.hash(&mut hasher);
    hasher.finish()
}

// All rows in a class have the same logical value. No expected hash is derived
// from the hash implementation: compare representations and layouts instead.
fn assert_hash_class(scalars: &[Scalar]) -> databend_common_exception::Result<()> {
    let first = &scalars[0];
    let data_type = first.as_ref().infer_data_type();
    let mut map = HashMap::new();
    let mut builder = ColumnBuilder::with_capacity(&data_type, scalars.len() + 2);
    builder.push(first.as_ref());
    for scalar in scalars {
        // Check equality in both directions before diagnosing HashMap hashing.
        assert_eq!(scalar, first);
        assert_eq!(first, scalar);
        assert_eq!(scalar_hash(scalar), scalar_hash(first));
        map.insert(scalar.clone(), ());
        builder.push(scalar.as_ref());
    }
    assert_eq!(map.len(), 1);
    builder.push(first.as_ref());
    // Slice off padding so binary/array buffers and validity can have offsets.
    let column = builder.build().slice(1..scalars.len() + 1);
    let rows = scalars.len();
    let dictionary = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        NarrowBuffer::pack((0..rows).rev().map(|index| index as u64), (rows - 1) as u64),
        column.clone(),
    ))));
    let suffix = Int16Type::from_data(vec![4_i16; rows]);
    let entries = [BlockEntry::Column(column), BlockEntry::Column(dictionary)]
        .into_iter()
        .chain(scalars.iter().cloned().flat_map(|value| {
            let entry = repeated(value.clone(), rows);
            let nullable =
                BlockEntry::new_const_column(data_type.clone().wrap_nullable(), value, rows);
            [entry, nullable]
        }))
        .collect::<Vec<_>>();

    for projection in [vec![0], vec![0, 1], vec![1, 0]] {
        let null_block = DataBlock::new(
            vec![
                BlockEntry::new_const_column(data_type.clone().wrap_nullable(), Scalar::Null, rows),
                suffix.clone().into(),
            ],
            rows,
        );
        let mut null_hashes = vec![0; rows];
        group_hash_entries(
            ProjectedBlock::project(&projection, &null_block),
            &mut null_hashes,
        );
        // Compare the same layouts both without NULLs and with actual masked
        // payloads. Indexed hashing selects a valid row and a NULL row.
        for nullable in [false, true] {
            let mut expected = None;
            for entry in &entries {
                let entry = if nullable {
                    NullableColumn::new_column(
                        entry.to_column().remove_nullable(),
                        (0..rows).map(|row| row != rows - 1).collect(),
                    )
                    .into()
                } else {
                    entry.clone()
                };
                let block = DataBlock::new(vec![entry, suffix.clone().into()], rows);
                let mut hashes = vec![u64::MAX; rows];
                group_hash_entries(ProjectedBlock::project(&projection, &block), &mut hashes);
                let valid_rows = if nullable { rows - 1 } else { rows };
                assert!(hashes[..valid_rows].iter().all(|hash| *hash == hashes[0]));
                if nullable {
                    assert_eq!(hashes[rows - 1], null_hashes[rows - 1]);
                }
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
    // Include the hybrid Small/Large boundary and multiple Roaring high words.
    for members in [
        vec![],
        vec![2_u64, 9],
        (0..31).collect(),
        (0..32).collect(),
        (0..33).collect(),
        vec![0, 65_535, 65_536, u32::MAX as u64, 1_u64 << 32, u64::MAX],
    ] {
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
    }
    Ok(())
}

#[test]
fn invalid_payloads_hash_bytes() -> databend_common_exception::Result<()> {
    for bytes in [vec![7], b"HB\x01\x00".to_vec(), b"HB\x01\x01".to_vec()] {
        assert!(databend_common_io::deserialize_bitmap(&bytes).is_err());
        for scalar in [
            Scalar::Bitmap(bytes.clone()),
            Scalar::Geometry(bytes.clone()),
        ] {
            // Check the promised bytewise fallback, not merely repeatability.
            assert_eq!(scalar_hash(&scalar), scalar_hash(&bytes));
            assert_hash_class(&[scalar.clone(), scalar])?;
        }
    }
    Ok(())
}

// Standard Roaring treemap for [0, cardinality), assembled from the wire
// layout. Every container has one run; four or more containers need offsets.
fn run_bitmap(cardinality: u32) -> Vec<u8> {
    assert!((1..=(1 << 20)).contains(&cardinality));
    let containers = cardinality.div_ceil(1 << 16);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1_u64.to_le_bytes()); // one high-word bitmap
    bytes.extend_from_slice(&0_u32.to_le_bytes()); // high word
    bytes.extend_from_slice(&(12_347 | ((containers - 1) << 16)).to_le_bytes());
    let mut run_flags = vec![0_u8; containers.div_ceil(8) as usize];
    for key in 0..containers {
        run_flags[key as usize / 8] |= 1 << (key % 8);
    }
    bytes.extend_from_slice(&run_flags);
    for key in 0..containers {
        let len = (cardinality - (key << 16)).min(1 << 16);
        bytes.extend_from_slice(&(key as u16).to_le_bytes());
        bytes.extend_from_slice(&((len - 1) as u16).to_le_bytes());
    }
    if containers >= 4 {
        let header_len = 4 + containers.div_ceil(8) + containers * 8;
        for key in 0..containers {
            bytes.extend_from_slice(&(header_len + key * 6).to_le_bytes());
        }
    }
    for key in 0..containers {
        let len = (cardinality - (key << 16)).min(1 << 16);
        bytes.extend_from_slice(&1_u16.to_le_bytes()); // one run
        bytes.extend_from_slice(&0_u16.to_le_bytes()); // run start
        bytes.extend_from_slice(&((len - 1) as u16).to_le_bytes());
    }
    bytes
}

#[test]
fn bitmap_hashes_canonicalize_run_containers() -> databend_common_exception::Result<()> {
    for cardinality in [4_095_u32, 4_096, 4_097, 65_536, 1 << 20] {
        let mut tree = RoaringTreemap::new();
        tree.insert_range(0..u64::from(cardinality));
        let mut legacy = Vec::new();
        tree.serialize_into(&mut legacy).unwrap();
        let mut hybrid = Vec::new();
        HybridBitmap::Large(tree)
            .serialize_into(&mut hybrid)
            .unwrap();
        let run = Scalar::Bitmap(run_bitmap(cardinality));
        assert_hash_class(&[run.clone(), Scalar::Bitmap(legacy), Scalar::Bitmap(hybrid)])?;

        // Equal cardinalities alone are insufficient, and subset containment
        // alone must not equate sets with different cardinalities.
        for range in [1..u64::from(cardinality) + 1, 0..u64::from(cardinality) + 1] {
            let mut different = RoaringTreemap::new();
            different.insert_range(range);
            let mut bytes = Vec::new();
            different.serialize_into(&mut bytes).unwrap();
            let different = Scalar::Bitmap(bytes);
            assert_ne!(run, different);
            assert_ne!(different, run);
        }
    }
    assert_hash_class(&[Scalar::Bitmap(Vec::new()), {
        let mut bytes = Vec::new();
        HybridBitmap::new().serialize_into(&mut bytes).unwrap();
        Scalar::Bitmap(bytes)
    }])
}

#[test]
fn bitmap_hashes_ignore_empty_high_words() -> databend_common_exception::Result<()> {
    // Two high-word entries, one with no members. Ordinary constructors do not
    // retain this valid but non-canonical serialized representation.
    let mut with_empty = 2_u64.to_le_bytes().to_vec();
    with_empty.extend_from_slice(&0_u32.to_le_bytes());
    RoaringBitmap::new()
        .serialize_into(&mut with_empty)
        .unwrap();
    with_empty.extend_from_slice(&1_u32.to_le_bytes());
    RoaringBitmap::from_iter(0..40)
        .serialize_into(&mut with_empty)
        .unwrap();
    let mut canonical = Vec::new();
    RoaringTreemap::from_iter((0..40).map(|value| (1_u64 << 32) + value))
        .serialize_into(&mut canonical)
        .unwrap();
    assert_hash_class(&[Scalar::Bitmap(with_empty), Scalar::Bitmap(canonical)])
}

#[test]
fn bitmap_hashing_does_not_write_every_dense_member() {
    #[derive(Default)]
    struct CountingHasher {
        bytes: usize,
    }
    impl Hasher for CountingHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, bytes: &[u8]) {
            self.bytes += bytes.len();
        }
    }

    for cardinality in [65_536_u32, 1 << 20] {
        let mut tree = RoaringTreemap::new();
        tree.insert_range(0..u64::from(cardinality));
        let canonical_size = tree.serialized_size();
        let scalar = Scalar::Bitmap(run_bitmap(cardinality));
        let mut hasher = CountingHasher::default();
        scalar.hash(&mut hasher);
        // This is a deterministic work bound, not a wall-clock benchmark:
        // visit at most the canonical compressed bytes, not each u64 member.
        assert!(
            hasher.bytes <= canonical_size,
            "hashed {} bytes",
            hasher.bytes
        );
        assert!(hasher.bytes < cardinality as usize * 8 / 32);
    }
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
