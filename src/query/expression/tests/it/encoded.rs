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

use std::borrow::Cow;

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnView;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::KeysState;
use databend_common_expression::Scalar;
use databend_common_expression::SelectOp;
use databend_common_expression::Value;
use databend_common_expression::filter::generate_like_pattern;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::ForColumn;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;

fn decimal_size() -> DecimalSize {
    DecimalSize::new_unchecked(15, 2)
}

fn decimal_column(values: &[i64]) -> Column {
    Column::Decimal(DecimalColumn::Decimal64(
        values.to_vec().into(),
        decimal_size(),
    ))
}

fn encode(column: &Column) -> Column {
    let encoded = ForColumn::try_encode(column).unwrap();
    Column::Encoded(Box::new(EncodedColumn::For(encoded)))
}

fn for_column(values: &[i64]) -> Column {
    encode(&decimal_column(values))
}

fn dict_column(values: &[&str], keys: &[u32]) -> Column {
    let values = StringType::from_data(values.to_vec());
    let max = values.len() as u64 - 1;
    let keys = NarrowBuffer::pack(keys.iter().map(|k| *k as u64), max);
    Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(keys, values))))
}

fn build_dict(column: &Column) -> Column {
    let dict = DictColumn::try_encode(column, 1 << 16).unwrap();
    Column::Encoded(Box::new(EncodedColumn::Dict(dict)))
}

#[test]
fn test_narrow_buffer_shared_storage() {
    for max in [u8::MAX as u64, u16::MAX as u64, u32::MAX as u64, u64::MAX] {
        let keys = NarrowBuffer::pack((0..8usize).map(|i| i as u64), max);
        assert!(keys.shares_storage(&keys.clone()));
        assert!(!keys.shares_storage(&NarrowBuffer::pack((0..8usize).map(|i| i as u64), max)));
        assert!(keys.slice(1..7).shares_storage(&keys.clone().slice(1..7)));
        assert!(!keys.slice(0..6).shares_storage(&keys.slice(1..7)));
        assert!(!keys.slice(0..6).shares_storage(&keys.slice(0..7)));
    }
}

/// Block-level dictionaries keep first-seen order and give up beyond `max_ndv`.
#[test]
fn test_dict_column_try_encode() {
    let column = Int64Type::from_data(vec![30, 10, 30, 20, 10]);
    let dict = DictColumn::try_encode(&column, 3).unwrap();
    assert_eq!(dict.values, Int64Type::from_data(vec![30, 10, 20]));
    assert_eq!(dict.keys, NarrowBuffer::U8(vec![0, 1, 0, 2, 1].into()));
    assert_eq!(dict.decode(), column);

    assert!(DictColumn::try_encode(&column, 2).is_none());
    assert!(DictColumn::try_encode(&Int64Type::from_data(Vec::<i64>::new()), 3).is_none());

    let dates = DateType::from_data(vec![5, 5, 9]);
    assert_eq!(DictColumn::try_encode(&dates, 8).unwrap().decode(), dates);
}

fn assert_same(encoded: &Column, decoded: &Column) {
    let Column::Encoded(e) = encoded else {
        panic!("expected an encoded column, got {encoded:?}");
    };
    assert_eq!(&e.decode(), decoded);
    assert_eq!(encoded.len(), decoded.len());
    assert_eq!(encoded.data_type(), decoded.data_type());
    for i in 0..decoded.len() {
        assert_eq!(encoded.index(i), decoded.index(i));
    }
}

#[test]
fn test_for_column_width() {
    let values = [100, 105, 355];
    let Column::Encoded(e) = for_column(&values) else {
        unreachable!()
    };
    let c = e.as_for().unwrap();
    assert_eq!(c.base(), 100);
    // range 255 fits in u8
    assert!(matches!(c.deltas(), NarrowBuffer::U8(_)));

    let c = ForColumn::try_encode(&decimal_column(&[-5, 70_000])).unwrap();
    assert!(matches!(c.deltas(), NarrowBuffer::U32(_)));
    assert_eq!(c.decode(), decimal_column(&[-5, 70_000]));

    // the deltas must be narrower than the values
    assert!(ForColumn::try_encode(&decimal_column(&[0, u32::MAX as i64 + 1])).is_none());
    assert!(ForColumn::try_encode(&decimal_column(&[i64::MIN, i64::MAX])).is_none());
    assert!(ForColumn::try_encode(&decimal_column(&[])).is_none());
    assert!(ForColumn::try_encode(&Int32Type::from_data(vec![0, 70_000])).is_none());
    assert!(ForColumn::try_encode(&StringType::from_data(vec!["a"])).is_none());
}

#[test]
fn test_encoded_column_kernels() {
    let values = (0..1000)
        .map(|i| 1_000_000 + (i * 37) % 9973)
        .collect::<Vec<i64>>();
    let keys = (0..1000).map(|i| (i * 7 % 3) as u32).collect::<Vec<_>>();
    let dict_values = ["A", "N", "R"];
    let strings = keys
        .iter()
        .map(|k| dict_values[*k as usize])
        .collect::<Vec<_>>();

    let dates = (0..1000)
        .map(|i| 8000 + (i * 13) % 2500)
        .collect::<Vec<i32>>();
    let timestamps = (0..1000)
        .map(|i| 1_700_000_000_000_000 + (i * 997) % 100_000)
        .collect::<Vec<i64>>();
    let large = (0..1000)
        .map(|i| u64::MAX - (i % 300))
        .collect::<Vec<u64>>();
    let negative = (0..1000)
        .map(|i| -40_000 + (i * 7) % 300)
        .collect::<Vec<i32>>();

    let cases = [
        (for_column(&values), decimal_column(&values)),
        (
            dict_column(&dict_values, &keys),
            StringType::from_data(strings),
        ),
        (
            encode(&DateType::from_data(dates.clone())),
            DateType::from_data(dates),
        ),
        (
            encode(&TimestampType::from_data(timestamps.clone())),
            TimestampType::from_data(timestamps),
        ),
        (
            encode(&UInt64Type::from_data(large.clone())),
            UInt64Type::from_data(large),
        ),
        (
            encode(&Int32Type::from_data(negative.clone())),
            Int32Type::from_data(negative),
        ),
    ];
    // Dictionaries built from regular columns of any supported type.
    let codes = (0..1000)
        .map(|i| [7, 1_000_000_007, -3][i % 3])
        .collect::<Vec<i64>>();
    let floats = (0..1000).map(|i| [0.5, -2.25][i % 2]).collect::<Vec<f64>>();
    let names = (0..1000)
        .map(|i| ["x", "long value"][i % 2])
        .collect::<Vec<_>>();
    let prices = (0..1000).map(|i| 100 * (i % 5)).collect::<Vec<i64>>();
    let cases = cases.into_iter().chain([
        (
            build_dict(&Int64Type::from_data(codes.clone())),
            Int64Type::from_data(codes),
        ),
        (
            build_dict(&Float64Type::from_data(floats.clone())),
            Float64Type::from_data(floats),
        ),
        (
            build_dict(&StringType::from_data(names.clone())),
            StringType::from_data(names),
        ),
        (
            build_dict(&decimal_column(&prices)),
            decimal_column(&prices),
        ),
    ]);

    for (encoded, decoded) in cases {
        assert_same(&encoded, &decoded);
        assert_same(&encoded.slice(10..500), &decoded.slice(10..500));

        // both filter strategies keep the encoding
        for bitmap in [
            Bitmap::from_iter((0..1000).map(|i| i % 3 == 0)),
            Bitmap::from_iter((0..1000).map(|i| i % 50 != 0)),
        ] {
            assert_same(&encoded.filter(&bitmap), &decoded.filter(&bitmap));
        }

        let indices = [999u32, 0, 5, 5, 123];
        let taken = BlockEntry::from(encoded.clone())
            .take(&indices[..])
            .unwrap();
        let expected = BlockEntry::from(decoded.clone())
            .take(&indices[..])
            .unwrap();
        assert_same(&taken.to_column(), &expected.to_column());

        // generic kernels see the decoded values
        let concat = DataBlock::concat(&[
            DataBlock::new(vec![encoded.clone().into()], encoded.len()),
            DataBlock::new(vec![decoded.clone().into()], decoded.len()),
        ])
        .unwrap();
        let expected = DataBlock::concat(&[
            DataBlock::new(vec![decoded.clone().into()], decoded.len()),
            DataBlock::new(vec![decoded.clone().into()], decoded.len()),
        ])
        .unwrap();
        assert_eq!(
            concat.get_by_offset(0).to_column(),
            expected.get_by_offset(0).to_column()
        );

        assert_eq!(
            Value::Column(encoded.clone()).decode_owned(),
            Value::Column(decoded.clone())
        );
        assert!(encoded.memory_size(false) < decoded.memory_size(false));
        assert_eq!(encoded.domain(), decoded.domain());
    }
}

#[test]
fn test_encoded_column_downcast() {
    let values = [7, 8, 9];
    let encoded = for_column(&values);
    assert_eq!(
        Decimal64Type::try_downcast_column(&encoded)
            .unwrap()
            .as_slice(),
        &values
    );

    let encoded = dict_column(&["x", "yy"], &[1, 0, 1]);
    let strings = StringType::try_downcast_column(&encoded).unwrap();
    assert_eq!(strings.iter().collect::<Vec<_>>(), vec!["yy", "x", "yy"]);

    let encoded = encode(&DateType::from_data(vec![10, 20, 15]));
    let dates = DateType::try_downcast_column(&encoded).unwrap();
    assert_eq!(dates.as_slice(), &[10, 20, 15]);
}

#[test]
fn test_encoded_column_view() {
    fn check<T: AccessType>(encoded: &Column, encoded_view: bool) {
        let decoded = BlockEntry::Column(encoded.as_encoded().unwrap().decode())
            .downcast::<T>()
            .unwrap();
        let view = BlockEntry::Column(encoded.clone()).downcast::<T>().unwrap();
        assert_eq!(matches!(view, ColumnView::Encoded(_)), encoded_view);
        assert_eq!(view.len(), decoded.len());
        assert!(view.iter().eq(decoded.iter()));
        for i in 0..decoded.len() {
            assert_eq!(view.index(i), decoded.index(i));
            assert_eq!(
                unsafe { view.index_unchecked(i) },
                decoded.index(i).unwrap()
            );
        }
        assert_eq!(view.index(decoded.len()), None);
    }

    check::<Decimal64Type>(&for_column(&[700, -8, 9_000]), true);
    check::<Int32Type>(
        &encode(&Int32Type::from_data(vec![-30_000, 5, 30_000])),
        true,
    );
    check::<DateType>(&encode(&DateType::from_data(vec![10, 20, 15])), true);
    check::<TimestampType>(
        &encode(&TimestampType::from_data(vec![
            1 << 40,
            (1 << 40) + 3,
            (1 << 40) + 70_000,
        ])),
        true,
    );
    check::<StringType>(&dict_column(&["x", "yy", "unused"], &[1, 0, 1]), true);
    let codes = Int64Type::from_data(vec![5, -1, 1 << 40, 5]);
    check::<Int64Type>(&build_dict(&codes), true);
    let floats = Float64Type::from_data(vec![0.5, -2.0, 0.5]);
    check::<Float64Type>(&build_dict(&floats), true);

    // Types that do not read the encoding get the decoded column.
    let decimal = for_column(&[1, 2, 3]);
    let view = BlockEntry::Column(decimal).downcast::<AnyType>().unwrap();
    assert!(matches!(view, ColumnView::Column(_)));
}

/// Comparing the deltas with a constant gives the same result as comparing the values.
#[test]
fn test_for_column_compare() {
    let ops = [
        SelectOp::Equal,
        SelectOp::NotEqual,
        SelectOp::Gt,
        SelectOp::Gte,
        SelectOp::Lt,
        SelectOp::Lte,
    ];
    let cmp = |op: &SelectOp, v: i128, c: i128| match op {
        SelectOp::Equal => v == c,
        SelectOp::NotEqual => v != c,
        SelectOp::Gt => v > c,
        SelectOp::Gte => v >= c,
        SelectOp::Lt => v < c,
        SelectOp::Lte => v <= c,
    };

    let dates = (0..300).map(|i| 9000 + (i * 7) % 200).collect::<Vec<i32>>();
    let Column::Encoded(encoded) = encode(&DateType::from_data(dates.clone())) else {
        unreachable!()
    };
    let column = encoded.as_for().unwrap();
    // A filtered column keeps the bounds of the original deltas.
    let filtered = column.with_deltas(column.deltas().slice(0..3));
    for constant in [
        i32::MIN,
        0,
        8999,
        9000,
        9001,
        9100,
        9198,
        9199,
        9200,
        i32::MAX,
    ] {
        let raw = column.raw_scalar(&Scalar::Date(constant)).unwrap();
        for op in &ops {
            let expected = Bitmap::from_iter(dates.iter().map(|v| cmp(op, *v as i128, raw)));
            assert_eq!(column.compare_raw(op, raw), expected, "{op:?} {constant}");
            let expected = Bitmap::from_iter(dates[..3].iter().map(|v| cmp(op, *v as i128, raw)));
            assert_eq!(filtered.compare_raw(op, raw), expected, "{op:?} {constant}");
        }
    }

    // Scalars of the logical type compare with the raw values; decimals of a smaller scale are
    // rescaled to the scale of the column (2), a larger scale is not.
    let Column::Encoded(encoded) = for_column(&[100, 250]) else {
        unreachable!()
    };
    let column = encoded.as_for().unwrap();
    let decimal = |v: i64, scale: u8| {
        Scalar::Decimal(DecimalScalar::Decimal64(
            v,
            DecimalSize::new_unchecked(10, scale),
        ))
    };
    assert_eq!(column.raw_scalar(&decimal(150, 2)), Some(150));
    assert_eq!(column.raw_scalar(&decimal(15, 1)), Some(150));
    assert_eq!(column.raw_scalar(&decimal(1, 0)), Some(100));
    assert_eq!(column.raw_scalar(&decimal(-3, 0)), Some(-300));
    assert_eq!(column.raw_scalar(&decimal(1500, 3)), None);
    let huge = Scalar::Decimal(DecimalScalar::Decimal128(
        i128::MAX / 10,
        DecimalSize::new_unchecked(38, 0),
    ));
    assert_eq!(column.raw_scalar(&huge), None);
    assert_eq!(column.raw_scalar(&Scalar::Date(150)), None);
    assert_eq!(column.data_type(), &DataType::Decimal(decimal_size()));
}

/// `a <op> b` over two frame of reference columns, on the deltas, gives the same rows as
/// comparing the values.
#[test]
fn test_for_column_compare_columns() {
    let ops = [
        SelectOp::Equal,
        SelectOp::NotEqual,
        SelectOp::Gt,
        SelectOp::Gte,
        SelectOp::Lt,
        SelectOp::Lte,
    ];
    let cmp = |op: &SelectOp, a: i32, b: i32| match op {
        SelectOp::Equal => a == b,
        SelectOp::NotEqual => a != b,
        SelectOp::Gt => a > b,
        SelectOp::Gte => a >= b,
        SelectOp::Lt => a < b,
        SelectOp::Lte => a <= b,
    };
    let for_of = |column: &Column| match encode(column) {
        Column::Encoded(encoded) => encoded.as_for().unwrap().clone(),
        _ => unreachable!(),
    };

    // Different bases and delta widths (u8 / u16), overlapping ranges, ties.
    let a = (0..500).map(|i| 9000 + (i * 7) % 200).collect::<Vec<i32>>();
    let b = (0..500)
        .map(|i| 8950 + (i * 13) % 3000)
        .collect::<Vec<i32>>();
    let (fa, fb) = (
        for_of(&DateType::from_data(a.clone())),
        for_of(&DateType::from_data(b.clone())),
    );
    for op in &ops {
        let expected = Bitmap::from_iter(a.iter().zip(&b).map(|(a, b)| cmp(op, *a, *b)));
        assert_eq!(fa.compare_for(op, &fb), Some(expected), "{op:?}");
        let expected = Bitmap::from_iter(b.iter().zip(&a).map(|(b, a)| cmp(op, *b, *a)));
        assert_eq!(fb.compare_for(op, &fa), Some(expected), "{op:?}");
    }

    // Columns of different types or lengths do not compare by their raw values.
    let int = for_of(&Int32Type::from_data(a.clone()));
    assert_eq!(fa.compare_for(&SelectOp::Lt, &int), None);
    assert_eq!(fa.compare_for(&SelectOp::Lt, &fa.slice(0..10)), None);
}

/// LIKE matches each dictionary entry once and maps results through keys of any width.
#[test]
fn test_dict_column_like() {
    let values = StringType::from_data(vec![
        "yyy/b", "xxx/a", "", "xxx_100%", "中文/a", "xxx/c", "unused",
    ]);
    let keys = [1u64, 0, 1, 5, 2, 3, 4, 0, 5];
    for keys in [
        NarrowBuffer::U8(keys.iter().map(|k| *k as u8).collect::<Vec<_>>().into()),
        NarrowBuffer::U16(keys.iter().map(|k| *k as u16).collect::<Vec<_>>().into()),
        NarrowBuffer::U32(keys.iter().map(|k| *k as u32).collect::<Vec<_>>().into()),
        NarrowBuffer::U64(keys.to_vec().into()),
    ] {
        // Sliced keys need not start at the buffer's original offset; the dictionary may
        // contain unused entries (as Parquet dictionaries and filtered columns do).
        for keys in [keys.clone(), keys.slice(1..8), keys.slice(0..0)] {
            let dict = DictColumn::new(keys, values.clone());
            let Column::String(decoded) = dict.decode() else {
                unreachable!()
            };
            for text in [
                "xxx/a",
                "xxx%",
                "%/a",
                "%xx%",
                "x%/%",
                "%x%/%",
                "xxx/_",
                "",
                "%",
                "no match",
                "unused",
                r"xxx\_100\%",
                "中文%",
            ] {
                let pattern = generate_like_pattern(Cow::Borrowed(text.as_bytes()), 0);
                for not in [false, true] {
                    let expected = decoded
                        .iter()
                        .map(|value| pattern.compare(value.as_bytes()) ^ not)
                        .collect::<Bitmap>();
                    assert_eq!(dict.like(&pattern, not), Some(expected), "{text} {not}");
                }
            }
        }
    }

    let pattern = generate_like_pattern(b"%".as_slice(), 0);
    let empty = DictColumn::new(
        NarrowBuffer::U8(Vec::<u8>::new().into()),
        StringType::from_data(Vec::<String>::new()),
    );
    assert_eq!(empty.like(&pattern, false), Some(Bitmap::new()));
    let numbers = DictColumn::try_encode(&Int32Type::from_data(vec![1, 2, 1]), 2).unwrap();
    assert_eq!(numbers.like(&pattern, false), None);
}

/// `value <op> constant` on a dictionary column, evaluated once per dictionary value, gives the
/// same rows as comparing the decoded values.
#[test]
fn test_dict_column_compare() {
    let ops = [
        SelectOp::Equal,
        SelectOp::NotEqual,
        SelectOp::Gt,
        SelectOp::Gte,
        SelectOp::Lt,
        SelectOp::Lte,
    ];
    let matches = |op: &SelectOp, ordering: std::cmp::Ordering| match op {
        SelectOp::Equal => ordering.is_eq(),
        SelectOp::NotEqual => ordering.is_ne(),
        SelectOp::Gt => ordering.is_gt(),
        SelectOp::Gte => ordering.is_ge(),
        SelectOp::Lt => ordering.is_lt(),
        SelectOp::Lte => ordering.is_le(),
    };

    // Parquet style dictionary with an unreferenced value ("Z"), and a filtered column.
    let strings = ["N", "R", "N", "A", "N", "R", "A", "N"];
    let keys = [1, 2, 1, 0, 1, 2, 0, 1];
    let column = dict_column(&["A", "N", "R", "Z"], &keys);
    let Column::Encoded(encoded) = &column else {
        unreachable!()
    };
    for constant in ["", "A", "B", "N", "R", "Z", "ZZ"] {
        let scalar = Scalar::String(constant.to_string());
        for op in &ops {
            let expected =
                Bitmap::from_iter(strings.iter().map(|v| matches(op, (*v).cmp(constant))));
            assert_eq!(
                encoded.compare_scalar(op, &scalar),
                Some(expected),
                "{op:?} {constant}"
            );
        }
    }

    // Numeric block dictionary.
    let ints = [5i64, 9_000_000, 5, 5, 9_000_000, -3];
    let Column::Encoded(encoded) = build_dict(&Int64Type::from_data(ints.to_vec())) else {
        unreachable!()
    };
    for constant in [i64::MIN, -3, 0, 5, 6, 9_000_000, i64::MAX] {
        let scalar = Scalar::Number(constant.into());
        for op in &ops {
            let expected = Bitmap::from_iter(ints.iter().map(|v| matches(op, v.cmp(&constant))));
            assert_eq!(
                encoded.compare_scalar(op, &scalar),
                Some(expected),
                "{op:?} {constant}"
            );
        }
    }

    // Constants of another type are not compared on the dictionary.
    assert_eq!(
        encoded.compare_scalar(&SelectOp::Equal, &Scalar::Null),
        None
    );
    assert_eq!(
        encoded.compare_scalar(&SelectOp::Equal, &Scalar::Number(5i32.into())),
        None
    );
}

/// Nullable encodings keep the same NULL semantics through generic consumer boundaries.
#[test]
fn test_nullable_encoded_consumers() {
    for encoded in [
        dict_column(&["xxx/a", "yyy/b", ""], &[0, 1, 0, 2, 1, 0, 2, 0]),
        encode(&Int32Type::from_data(vec![7, 9, 7, 8, 9, 7, 8, 7])),
    ] {
        let decoded = encoded.as_encoded().unwrap().decode();
        for validity in [
            Bitmap::new_constant(true, 8),
            Bitmap::new_constant(false, 8),
            Bitmap::from_iter([true, false, true, false, true, true, false, true]),
        ] {
            let column = NullableColumn::new_column(encoded.clone(), validity.clone());
            let plain = NullableColumn::new_column(decoded.clone(), validity);
            let block = DataBlock::new_from_columns(vec![column.clone()]);
            let expected = DataBlock::new_from_columns(vec![plain.clone()]);
            let mut hashes = vec![0; 8];
            let mut expected_hashes = vec![0; 8];
            databend_common_expression::group_hash_entries(block.columns().into(), &mut hashes);
            databend_common_expression::group_hash_entries(
                expected.columns().into(),
                &mut expected_hashes,
            );
            assert_eq!(hashes, expected_hashes);

            let filtered = column.filter(&Bitmap::from_iter((0..8).map(|i| i % 3 != 0)));
            let plain_filtered = plain.filter(&Bitmap::from_iter((0..8).map(|i| i % 3 != 0)));
            assert!(filtered.remove_nullable().is_encoded());
            assert_eq!(
                BlockEntry::Column(filtered).decoded_value(),
                BlockEntry::Column(plain_filtered).decoded_value()
            );

            let taken = block.take_ranges(&[0..3, 4..8], 7).unwrap();
            let plain_taken = expected.take_ranges(&[0..3, 4..8], 7).unwrap();
            assert!(
                taken
                    .get_by_offset(0)
                    .to_column()
                    .remove_nullable()
                    .is_encoded()
            );
            assert_eq!(
                taken.get_by_offset(0).decoded_value(),
                plain_taken.get_by_offset(0).value()
            );

            let concat = DataBlock::concat(&[
                DataBlock::new_from_columns(vec![column]),
                DataBlock::new_from_columns(vec![plain.clone()]),
            ])
            .unwrap();
            let expected_concat = DataBlock::concat(&[
                DataBlock::new_from_columns(vec![plain.clone()]),
                DataBlock::new_from_columns(vec![plain]),
            ])
            .unwrap();
            assert_eq!(
                concat.get_by_offset(0).decoded_value(),
                expected_concat.get_by_offset(0).value()
            );
        }
    }
}

/// Nullable wrappers keep the encoding; evaluator boundaries decode the inner column.
#[test]
fn test_nullable_wraps_encoded_column() {
    let plain = Int32Type::from_data(vec![7, 9, 8]);
    let validity = Bitmap::from_iter([true, false, true]);
    for column in [
        encode(&plain),
        NullableColumn::new_column(dict_column(&["a", "b"], &[1, 0, 1]), validity.clone()),
    ] {
        let wrapped = match column {
            Column::Nullable(c) => *c,
            column => NullableColumn::new(column, validity.clone()),
        };
        assert!(matches!(wrapped.column, Column::Encoded(_)));
    }
    let Column::Nullable(wrapped) = encode(&plain).wrap_nullable(None) else {
        unreachable!()
    };
    assert!(wrapped.column.is_encoded());
    let expected = NullableColumn::new_column(plain, wrapped.validity.clone());
    let value = Value::Column(Column::Nullable(wrapped));
    assert!(value.is_encoded());
    assert_eq!(*value.decoded(), Value::Column(expected.clone()));
    assert_eq!(value.decode_owned(), Value::Column(expected));
}

#[test]
fn test_encoded_row_serialization() {
    let n = 5;
    let validity = Bitmap::from_iter([true, false, true, true, false]);
    let plain = [
        Int64Type::from_data(vec![-3, 1_000_000, 7, 7, 1_000_200]),
        Int32Type::from_data(vec![5, -2, 9, 5, 0]),
        DateType::from_data(vec![19000, 19005, 19000, 19090, 19001]),
        TimestampType::from_data(vec![10, 1_000_000_000, 10, 7, 999]),
        decimal_column(&[100, -250, 90_000, 100, 0]),
        StringType::from_data(vec!["x", "yy", "x", "", "yy"]),
    ];
    let encoded = |column: &Column| match column {
        Column::String(_) => build_dict(column),
        column => encode(column),
    };
    let mut decoded = plain.to_vec();
    let mut columns = plain.iter().map(encoded).collect::<Vec<_>>();
    decoded.push(NullableColumn::new_column(
        plain[0].clone(),
        validity.clone(),
    ));
    columns.push(NullableColumn::new_column(encoded(&plain[0]), validity));

    let keys = |columns: &[Column]| {
        let entries = columns
            .iter()
            .cloned()
            .map(BlockEntry::from)
            .collect::<Vec<_>>();
        match HashMethodSerializer::default()
            .build_keys_state((&entries).into(), n)
            .unwrap()
        {
            KeysState::Column(column) => column,
            _ => unreachable!(),
        }
    };
    assert_eq!(keys(&columns), keys(&decoded));
    for (column, expected) in columns.into_iter().zip(decoded) {
        assert!(
            column.is_encoded() || matches!(&column, Column::Nullable(c) if c.column.is_encoded())
        );
        assert_eq!(column.decode_owned(), expected);
    }
}
