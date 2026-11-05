// Copyright 2022 Datafuse Labs.
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

use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::*;
use ethnum::u256;

/// Every key boundary must map `-0.0`/`+0.0` and every NaN payload to the same
/// key while keeping other values distinct. Rows 0 and 1 of each column hold
/// two members of one equality class; row 2 holds an unrelated value.
#[test]
fn test_float_equality_class_keys() -> anyhow::Result<()> {
    let classes = [
        (
            Float32Type::from_data(vec![0.0f32, -0.0, 1.0]),
            Float64Type::from_data(vec![0.0f64, -0.0, 1.0]),
        ),
        (
            Float32Type::from_data(vec![f32::NAN, f32::from_bits(0xffc0_0001), 1.0]),
            Float64Type::from_data(vec![f64::NAN, f64::from_bits(0xfff8_0000_0000_0001), 1.0]),
        ),
        (
            Float32Type::from_data(vec![f32::NAN, -f32::NAN, 0.0]),
            Float64Type::from_data(vec![f64::NAN, (-1.0f64).sqrt(), -0.0]),
        ),
    ];
    for floats in classes.into_iter().flat_map(|(f32s, f64s)| [f32s, f64s]) {
        let nullable = NullableColumn::new_column(floats.clone(), Bitmap::from([true; 3]));
        let array = Column::Array(Box::new(ArrayColumn::new(
            floats.clone(),
            vec![0, 1, 2, 3].into(),
        )));
        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![StringType::from_data(vec!["k"; 3]), floats.clone()]),
            vec![0, 1, 2, 3].into(),
        )));
        let tuple = Column::Tuple(vec![array.clone(), nullable.clone()]);

        let mut columns = vec![floats.clone(), nullable, array, map, tuple];
        if let Column::Number(NumberColumn::Float32(values)) = &floats {
            columns.push(Column::Vector(VectorColumn::Float32((values.clone(), 1))));
        }
        for column in columns {
            let scalar_hashes = column
                .iter()
                .map(|value| {
                    let mut hasher = DefaultHasher::new();
                    value.hash(&mut hasher);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            assert_eq!(scalar_hashes[0], scalar_hashes[1], "{column:?}");
            assert_ne!(scalar_hashes[0], scalar_hashes[2], "{column:?}");

            let block = DataBlock::new_from_columns(vec![
                column.clone(),
                Int8Type::from_data(vec![1; 3]),
                StringType::from_data(vec!["k"; 3]),
            ]);
            for projection in [vec![0], vec![0, 1], vec![0, 2]] {
                let entries = ProjectedBlock::project(&projection, &block);
                let method = DataBlock::choose_hash_method(&block, &projection)?;
                with_hash_method!(|M| match &method {
                    HashMethodKind::M(method) => {
                        let state = method.build_keys_state(entries, 3)?;
                        let keys = method.build_keys_iter(&state)?.collect::<Vec<_>>();
                        assert_eq!(keys[0], keys[1], "{column:?} {projection:?}");
                        assert_ne!(keys[0], keys[2], "{column:?} {projection:?}");
                    }
                });

                let mut hashes = vec![0; 3];
                group_hash_entries(entries, &mut hashes);
                assert_eq!(hashes[0], hashes[1], "{column:?} {projection:?}");
                assert_ne!(hashes[0], hashes[2], "{column:?} {projection:?}");
            }
        }
    }
    Ok(())
}

fn serialize_keys(entries: &[BlockEntry], rows: usize) -> BinaryColumn {
    let state = HashMethodSerializer::default()
        .build_keys_state(entries.into(), rows)
        .unwrap();
    match state {
        KeysState::Column(Column::Binary(keys)) => keys,
        _ => unreachable!(),
    }
}

#[test]
fn test_serialized_key_lengths_and_offsets() {
    let entries = vec![
        UInt16Type::from_data(vec![9, 256, 9, 0]).into(),
        NullableColumn::new_column(
            StringType::from_data(vec!["", "unused", "reef", "q"]),
            Bitmap::from([true, false, true, true]),
        )
        .into(),
        BinaryType::from_data(vec![vec![], vec![0, 254], vec![b'x'], vec![]]).into(),
    ];
    // Hand-computed native-endian layout: number, validity, optional string
    // length/payload, then binary length/payload. Invalid string bytes are absent.
    let expected = [
        [
            9_u16.to_ne_bytes().to_vec(),
            vec![1],
            0_u64.to_ne_bytes().to_vec(),
            0_u64.to_ne_bytes().to_vec(),
        ]
        .concat(),
        [
            256_u16.to_ne_bytes().to_vec(),
            vec![0],
            2_u64.to_ne_bytes().to_vec(),
            vec![0, 254],
        ]
        .concat(),
        [
            9_u16.to_ne_bytes().to_vec(),
            vec![1],
            4_u64.to_ne_bytes().to_vec(),
            b"reef".to_vec(),
            1_u64.to_ne_bytes().to_vec(),
            b"x".to_vec(),
        ]
        .concat(),
        [
            0_u16.to_ne_bytes().to_vec(),
            vec![1],
            1_u64.to_ne_bytes().to_vec(),
            b"q".to_vec(),
            0_u64.to_ne_bytes().to_vec(),
        ]
        .concat(),
    ];
    let keys = serialize_keys(&entries, 4);
    assert_eq!(keys.offsets().as_slice(), &[0, 19, 32, 56, 76]);
    assert_eq!(keys.data().len(), 76);
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
    );

    let sliced = entries
        .iter()
        .map(|entry| entry.to_column().slice(1..3).into())
        .collect::<Vec<_>>();
    let keys = serialize_keys(&sliced, 2);
    assert_eq!(keys.offsets().as_slice(), &[0, 13, 37]);
    assert_eq!(keys.data().len(), 37);
    assert_eq!(keys.index(0), Some(expected[1].as_slice()));
    assert_eq!(keys.index(1), Some(expected[2].as_slice()));
}

#[test]
fn test_serialized_constant_keys_grow_from_zero_capacity() {
    let text = "v".repeat(97);
    let entries = vec![
        BlockEntry::new_const_column_arg::<Int16Type>(-2, 3),
        BlockEntry::new_const_column_arg::<StringType>(text.clone(), 3),
        BlockEntry::new_const_column_arg::<BooleanType>(true, 3),
    ];
    // With only constants the initial capacity hint is zero. Both the fixed
    // prefix and the long payload must survive subsequent buffer growth.
    let expected = [
        (-2_i16).to_ne_bytes().to_vec(),
        97_u64.to_ne_bytes().to_vec(),
        text.into_bytes(),
        vec![1],
    ]
    .concat();
    let keys = serialize_keys(&entries, 3);
    assert_eq!(keys.offsets().as_slice(), &[0, 108, 216, 324]);
    assert_eq!(keys.data().len(), 324);
    assert!(keys.iter().all(|key| key == expected));
}

#[test]
fn test_serialized_dictionary_keys_outgrow_size_estimate() {
    let text = "long".repeat(24);
    let values = StringType::from_data(vec!["", text.as_str()]);
    let indices = NarrowBuffer::pack([1; 5].into_iter(), 1);
    let column = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        indices, values,
    ))));
    // Every row selects the long value, not the dictionary's average length.
    assert!(column.serialize_size() < 520);
    let keys = serialize_keys(&[column.into()], 5);
    let expected = [96_u64.to_ne_bytes().to_vec(), text.into_bytes()].concat();
    assert_eq!(keys.offsets().as_slice(), &[0, 104, 208, 312, 416, 520]);
    assert_eq!(keys.data().len(), 520);
    assert!(keys.iter().all(|key| key == expected));
}

#[test]
fn test_serialized_decimal_widths() {
    let entries = vec![
        Column::Decimal(DecimalColumn::Decimal64(
            vec![0, -1].into(),
            DecimalSize::new_unchecked(12, 0),
        ))
        .into(),
        Column::Decimal(DecimalColumn::Decimal128(
            vec![0, -1].into(),
            DecimalSize::new_unchecked(25, 0),
        ))
        .into(),
        Column::Decimal(DecimalColumn::Decimal256(
            vec![i256::ZERO, i256::MINUS_ONE].into(),
            DecimalSize::new_unchecked(50, 0),
        ))
        .into(),
        UInt8Type::from_data(vec![6, 7]).into(),
    ];
    // Zero and minus one have endian-independent all-zero/all-one bytes.
    // Decimal64/128/256 contribute 8 + 16 + 32 bytes, then the marker byte.
    let first = [vec![0; 56], vec![6]].concat();
    let second = [vec![255; 56], vec![7]].concat();
    let keys = serialize_keys(&entries, 2);
    assert_eq!(keys.offsets().as_slice(), &[0, 57, 114]);
    assert_eq!(keys.data().len(), 114);
    assert_eq!(keys.index(0), Some(first.as_slice()));
    assert_eq!(keys.index(1), Some(second.as_slice()));
}

#[test]
fn test_serialized_nested_nullable_keys() {
    let arrays = Column::Array(Box::new(ArrayColumn::new(
        Int16Type::from_data(vec![4, -1, 8]),
        vec![0, 2, 2, 3].into(),
    )));
    let tuple = Column::Tuple(vec![
        NullableColumn::new_column(arrays, Bitmap::from([true, false, true])),
        UInt8Type::from_data(vec![2, 3, 4]),
    ]);
    let expected = [
        [
            vec![1],
            2_u64.to_ne_bytes().to_vec(),
            4_i16.to_ne_bytes().to_vec(),
            (-1_i16).to_ne_bytes().to_vec(),
            vec![2],
        ]
        .concat(),
        vec![0, 3],
        [
            vec![1],
            1_u64.to_ne_bytes().to_vec(),
            8_i16.to_ne_bytes().to_vec(),
            vec![4],
        ]
        .concat(),
    ];
    let keys = serialize_keys(&[tuple.into()], 3);
    assert_eq!(keys.offsets().as_slice(), &[0, 14, 16, 28]);
    assert_eq!(keys.data().len(), 28);
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
    );
}

#[test]
fn test_serialized_empty_and_null_keys() {
    for entries in [vec![], vec![
        Column::Null { len: 3 }.into(),
        Column::EmptyArray { len: 3 }.into(),
        Column::EmptyMap { len: 3 }.into(),
    ]] {
        let keys = serialize_keys(&entries, 3);
        assert_eq!(keys.offsets().as_slice(), &[0, 0, 0, 0]);
        assert!(keys.data().is_empty());
        assert_eq!(keys.iter().count(), 3);
    }
    let all_null = NullableColumn::new_column(
        StringType::from_data(vec!["hidden", "", "also hidden"]),
        Bitmap::from([false; 3]),
    );
    let keys = serialize_keys(&[all_null.into()], 3);
    assert_eq!(keys.offsets().as_slice(), &[0, 1, 2, 3]);
    assert_eq!(keys.data().as_slice(), &[0, 0, 0]);

    let entries = [BlockEntry::new_const_column_arg::<StringType>(
        String::new(),
        0,
    )];
    let keys = serialize_keys(&entries, 0);
    assert_eq!(keys.offsets().as_slice(), &[0]);
    assert!(keys.data().is_empty());
    assert_eq!(keys.iter().count(), 0);
}

#[test]
#[should_panic(expected = "too many group keys")]
fn test_serialized_key_offset_count_overflow() {
    serialize_keys(&[], usize::MAX);
}

#[test]
#[should_panic(expected = "column.len() >= num_rows")]
fn test_serialized_key_row_count_mismatch() {
    serialize_keys(&[Int8Type::from_data(vec![1]).into()], 2);
}

#[test]
fn test_group_by_hash() -> anyhow::Result<()> {
    let schema = TableSchemaRefExt::create(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("b", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("c", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("x", TableDataType::String),
    ]);

    let block = DataBlock::new_from_columns(vec![
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        StringType::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let method = DataBlock::choose_hash_method(&block, &[0, 3])?;
    assert_eq!(method.name(), HashMethodSerializer::default().name(),);

    let method = DataBlock::choose_hash_method(&block, &[0, 1, 2])?;

    assert_eq!(method.name(), HashMethodKeysU32::default().name());

    let args = vec!["a", "b", "c"]
        .into_iter()
        .map(|col| schema.index_of(col).unwrap())
        .collect::<Vec<_>>();
    let group_columns = ProjectedBlock::project(&args, &block);

    let hash = HashMethodKeysU32::default();
    let state = hash.build_keys_state(group_columns, block.num_rows())?;
    let keys_iter = hash.build_keys_iter(&state)?;
    let keys: Vec<u32> = keys_iter.copied().collect();
    assert_eq!(keys, vec![
        0x10101, 0x10101, 0x20202, 0x10101, 0x20202, 0x30303
    ]);
    Ok(())
}

#[test]
fn test_group_by_hash_decimal() -> anyhow::Result<()> {
    let size_128 = DecimalSize::new_unchecked(20, 2);
    let size_256 = DecimalSize::new_unchecked(40, 2);

    let decimal_128_values = [123456789_i128, 987654_i128, 123456789_i128];
    let decimal_256_values = [
        i256::from(123456789),
        i256::from(987654),
        i256::from(123456789),
    ];

    let block = DataBlock::new_from_columns(vec![
        Decimal128Type::from_data_with_size(decimal_128_values, Some(size_128)),
        Decimal256Type::from_data_with_size(decimal_256_values, Some(size_128)),
        Decimal128Type::from_data_with_size(decimal_128_values, Some(size_256)),
        Decimal256Type::from_data_with_size(decimal_256_values, Some(size_256)),
    ]);

    let method = DataBlock::choose_hash_method(&block, &[0])?;
    assert_eq!(method.name(), HashMethodKeysU128::default().name());
    let method = DataBlock::choose_hash_method(&block, &[1])?;
    assert_eq!(method.name(), HashMethodKeysU128::default().name());
    let method = DataBlock::choose_hash_method(&block, &[2])?;
    assert_eq!(method.name(), HashMethodKeysU256::default().name());
    let method = DataBlock::choose_hash_method(&block, &[3])?;
    assert_eq!(method.name(), HashMethodKeysU256::default().name());

    for i in [0, 1] {
        let args = &[i];
        let group_columns = ProjectedBlock::project(args, &block);
        let hash = HashMethodKeysU128::default();
        let state = hash.build_keys_state(group_columns, block.num_rows())?;
        let keys_iter = hash.build_keys_iter(&state)?;
        let keys = keys_iter.copied().collect::<Vec<_>>();

        assert_eq!(keys.len(), 3);
        assert_eq!(&keys, &[123456789_u128, 987654_u128, 123456789_u128]);
    }

    for i in [2, 3] {
        let args = &[i];
        let group_columns = ProjectedBlock::project(args, &block);
        let hash = HashMethodKeysU256::default();
        let state = hash.build_keys_state(group_columns, block.num_rows())?;
        let keys_iter = hash.build_keys_iter(&state)?;
        let keys = keys_iter.copied().collect::<Vec<_>>();

        assert_eq!(keys.len(), 3);
        assert_eq!(&keys, &[
            u256::from(123456789_u128),
            u256::from(987654_u128),
            u256::from(123456789_u128)
        ]);
    }

    Ok(())
}
