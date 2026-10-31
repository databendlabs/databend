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

use std::borrow::Cow;

use arrow_schema::Schema;
use chrono_tz::Tz;
use databend_common_column::bitmap::Bitmap;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RandomOptions;
use databend_common_expression::Scalar;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::types::timestamp::timestamp_to_rfc3339_utc;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::timestamp_tz::string_to_timestamp_tz;

use crate::DataTypeFilter;
use crate::get_all_test_data_types;
use crate::rand_block_for_all_types;

#[test]
fn test_aggregate_state_physical_type() {
    let ordinary_tuple = DataType::Tuple(vec![DataType::String, DataType::Boolean]);
    assert!(matches!(ordinary_tuple.physical_type(), Cow::Borrowed(_)));

    let state_type = DataType::Tuple(vec![
        DataType::Number(NumberDataType::UInt64),
        DataType::Boolean,
    ]);
    let aggregate_state = DataType::AggregateState(Box::new(AggregateStateDataType {
        function_name: "sum".to_string(),
        params: vec![],
        argument_types: vec![DataType::Number(NumberDataType::UInt64)],
        state_type: Box::new(state_type.clone()),
        state_version: 0,
    }));

    assert_eq!(aggregate_state.physical_type().as_ref(), &state_type);
    assert_eq!(aggregate_state.sql_name(), "AGGREGATESTATE(SUM, UINT64)");
    assert_eq!(
        DataType::Nullable(Box::new(aggregate_state.clone()))
            .physical_type()
            .into_owned(),
        DataType::Nullable(Box::new(state_type.clone()))
    );
    assert_eq!(
        DataType::Tuple(vec![DataType::String, aggregate_state.clone()])
            .physical_type()
            .into_owned(),
        DataType::Tuple(vec![DataType::String, state_type.clone()])
    );

    let DataType::AggregateState(mut future_state) = aggregate_state.clone() else {
        unreachable!()
    };
    future_state.state_version = 1;
    let future_state = DataType::AggregateState(future_state);
    let expr = ColumnRef {
        span: None,
        id: 0,
        data_type: aggregate_state.clone(),
        display_name: "state".to_string(),
    };
    let error = check_cast(
        None,
        false,
        expr.into(),
        &future_state,
        &FunctionRegistry::empty(),
    )
    .unwrap_err();
    assert!(error.message().contains("source sum version 0"));
    assert!(error.message().contains("destination sum version 1"));

    let logical_container = DataType::Tuple(vec![
        DataType::Array(Box::new(aggregate_state.clone())),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::String,
            aggregate_state.clone(),
        ]))),
    ]);
    let physical_container = DataType::Tuple(vec![
        DataType::Array(Box::new(state_type.clone())),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::String,
            state_type.clone(),
        ]))),
    ]);
    assert!(logical_container.matches_physical_type(&physical_container));

    let scalar = Scalar::Tuple(vec![
        Scalar::Number(NumberScalar::UInt64(1)),
        Scalar::Boolean(true),
    ]);
    assert!(
        scalar
            .as_ref()
            .is_value_of_type(&DataType::Nullable(Box::new(aggregate_state)))
    );
}

#[test]
fn test_aggregate_function_param_scalar_conversion() {
    let scalar = databend_common_expression::Scalar::Tuple(vec![
        databend_common_expression::Scalar::String("param".to_string()),
        databend_common_expression::Scalar::Number(NumberScalar::UInt64(0)),
    ]);
    let param = AggregateFunctionParam::try_from(scalar.clone()).unwrap();

    assert_eq!(databend_common_expression::Scalar::from(param), scalar);
}

#[test]
fn test_timestamp_to_string_formats() {
    // Unix timestamp for "2024-01-01 01:02:03" UTC
    let ts = 1_704_070_923_000_000;
    let tz = Tz::UTC;

    assert_eq!(
        timestamp_to_string(ts, &tz).to_string(),
        "2024-01-01 01:02:03.000000"
    );
    assert_eq!(
        timestamp_to_rfc3339_utc(253_402_300_799_999_999),
        "9999-12-31T23:59:59.999999Z"
    );
}

#[test]
fn test_timestamp_display_clamps_bounds() {
    for tz in [Tz::UTC, Tz::Asia__Shanghai, Tz::America__New_York] {
        for (input, expected) in [
            (i64::MIN, TIMESTAMP_MIN),
            (TIMESTAMP_MIN - 1, TIMESTAMP_MIN),
            (TIMESTAMP_MIN, TIMESTAMP_MIN),
            (-1_000_001, -1_000_001),
            (-1, -1),
            (0, 0),
            (TIMESTAMP_MAX, TIMESTAMP_MAX),
            (TIMESTAMP_MAX + 1, TIMESTAMP_MAX),
            (i64::MAX, TIMESTAMP_MAX),
        ] {
            let value = timestamp_from_micros(input, &tz);
            assert_eq!(value.timestamp_micros(), expected, "{input} in {tz}");
            assert_eq!(
                timestamp_to_string(input, &tz).to_string(),
                timestamp_to_string(expected, &tz).to_string(),
            );
        }
    }
    assert_eq!(
        timestamp_to_string(-1, &Tz::UTC).to_string(),
        "1969-12-31 23:59:59.999999"
    );
    assert_eq!(
        timestamp_to_rfc3339_utc(i64::MAX),
        "9999-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        timestamp_to_rfc3339_utc(i64::MIN),
        "0000-12-31T00:00:00.000000Z"
    );
}

#[test]
fn test_timestamp_bounds_and_text_roundtrip() {
    assert_eq!(TIMESTAMP_MIN, -62_135_683_200_000_000);
    assert_eq!(TIMESTAMP_MAX, 253_402_300_799_999_999);
    for tz in chrono_tz::TZ_VARIANTS {
        let input = "0001-01-01 00:00:00.000000";
        let micros = string_to_timestamp(input, &tz).unwrap();
        assert_eq!(timestamp_to_string(micros, &tz).to_string(), input, "{tz}");
        for micros in [
            TIMESTAMP_MIN,
            TIMESTAMP_MIN + 1,
            -1,
            0,
            TIMESTAMP_MAX - 1,
            TIMESTAMP_MAX,
        ] {
            let text = timestamp_to_string(micros, &tz).to_string();
            assert_eq!(
                string_to_timestamp(&text, &tz).unwrap(),
                micros,
                "{text} in {tz}"
            );
        }
    }
    assert_eq!(
        string_to_timestamp("0001-01-01 00:00:00", &Tz::Asia__Shanghai).unwrap(),
        -62_135_625_943_000_000,
    );
    for text in [
        "0000-12-31",
        "0000-12-31 00:00:00.000001Z",
        "0001-01-01 00:00:00 +14:00",
        "+10000-01-01 13:59:59.999999 +14:00",
    ] {
        let micros = string_to_timestamp(text, &Tz::UTC).unwrap();
        assert_eq!(check_timestamp(micros).unwrap(), micros);
    }
    for text in [
        "0000-12-30 23:59:59.999999Z",
        "+10000-01-01",
        "-0001-12-31",
        "9999-12-31 23:59:59.999999 -00:01",
    ] {
        assert!(string_to_timestamp(text, &Tz::UTC).is_err(), "{text}");
        assert!(
            string_to_timestamp_tz(text.as_bytes(), || &Tz::UTC).is_err(),
            "{text}"
        );
    }
    for text in [
        "0000-12-31 00:00:00.000000 +0000",
        "0000-12-30 10:00:00.000000 -1400",
        "+10000-01-01 13:59:59.999999 +1400",
    ] {
        let value = string_to_timestamp_tz(text.as_bytes(), || &Tz::UTC).unwrap();
        let parsed = string_to_timestamp_tz(value.to_string().as_bytes(), || &Tz::UTC).unwrap();
        assert_eq!(parsed, value, "{text}");
    }

    assert_eq!(check_timestamp(TIMESTAMP_MIN).unwrap(), TIMESTAMP_MIN);
    assert_eq!(check_timestamp(TIMESTAMP_MAX).unwrap(), TIMESTAMP_MAX);
    assert!(check_timestamp(TIMESTAMP_MIN - 1).is_err());
    assert!(check_timestamp(TIMESTAMP_MAX + 1).is_err());

    // The wider UTC timestamp range is not an extension of SQL DATE.
    assert_eq!(DATE_MIN, -719_162);
    assert_eq!(DATE_MAX, 2_932_896);
    assert_eq!(check_date(i64::from(DATE_MIN)).unwrap(), DATE_MIN);
    assert_eq!(check_date(i64::from(DATE_MAX)).unwrap(), DATE_MAX);
    assert!(check_date(i64::from(DATE_MIN) - 1).is_err());
    assert!(check_date(i64::from(DATE_MAX) + 1).is_err());
}

#[test]
fn test_random_timestamps_keep_nonnegative_utc_years() {
    // Random tables feed tests with UInt32 calendar clustering keys. Pin the
    // product requirement independently of TIMESTAMP_MIN so extending the
    // storage range into negative years cannot silently change those fixtures.
    let column = Column::random(
        &DataType::Timestamp,
        4096,
        Some(RandomOptions {
            seed: Some(42),
            ..Default::default()
        }),
    );
    let Column::Timestamp(values) = column else {
        panic!("expected timestamp column");
    };
    assert_eq!(values.len(), 4096);
    for &micros in values.iter() {
        assert!(
            (-62_135_683_200_000_000..=253_402_300_799_999_999).contains(&micros),
            "random timestamp {micros} is outside the SQL UTC bounds"
        );
    }
}

#[test]
fn test_date_and_timestamp_clamp_to_minimum() {
    for value in [
        i64::MIN,
        DATE_MIN as i64 - 1,
        DATE_MIN as i64,
        -1,
        0,
        DATE_MAX as i64,
        DATE_MAX as i64 + 1,
        i64::MAX,
    ] {
        let expected = if (DATE_MIN as i64..=DATE_MAX as i64).contains(&value) {
            value as i32
        } else {
            DATE_MIN
        };
        assert_eq!(clamp_date(value), expected);
    }

    for value in [
        i64::MIN,
        TIMESTAMP_MIN - 1,
        TIMESTAMP_MIN,
        -1,
        0,
        TIMESTAMP_MAX,
        TIMESTAMP_MAX + 1,
        i64::MAX,
    ] {
        let expected = if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&value) {
            value
        } else {
            TIMESTAMP_MIN
        };
        let mut clamped = value;
        clamp_timestamp(&mut clamped);
        assert_eq!(clamped, expected);
        let encoded = value.to_le_bytes();
        let mut scalar_builder = ColumnBuilder::with_capacity(&DataType::Timestamp, 1);
        let mut batch_builder = ColumnBuilder::with_capacity(&DataType::Timestamp, 1);
        scalar_builder.push_binary(&mut encoded.as_slice()).unwrap();
        batch_builder
            .push_fix_len_binaries(&encoded, size_of::<i64>(), 1)
            .unwrap();
        for column in [scalar_builder.build(), batch_builder.build()] {
            let Column::Timestamp(values) = column else {
                panic!("expected timestamp column");
            };
            assert_eq!(values.as_slice(), &[expected]);
        }
    }
}

#[test]
fn test_convert_types() {
    let all_types = get_all_test_data_types(DataTypeFilter::All);
    let all_fields = all_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| DataField::new(&format!("column_{idx}"), data_type.clone()))
        .collect::<Vec<_>>();

    let schema = DataSchema::new(all_fields);
    let arrow_schema = Schema::from(&schema);
    let schema2 = DataSchema::try_from(&arrow_schema).unwrap();
    assert_eq!(schema, schema2);

    let random_block = rand_block_for_all_types(1024, DataTypeFilter::All);
    for (idx, c) in random_block.columns().iter().enumerate() {
        let c = c.as_column().unwrap().clone();

        let data = serialize_column(&c);
        let c2 = deserialize_column(&data).unwrap();
        assert_eq!(c, c2, "in {idx} | datatype: {}", c.data_type());
    }
}

#[test]
fn test_wrap_nullable_non_nullable() {
    for len in [0, 4] {
        let column = Int64Type::from_data(vec![1; len]);
        for validity in [
            None,
            Some(Bitmap::new_constant(true, len)),
            Some(Bitmap::new_constant(false, len)),
            Some((0..len).map(|i| i % 2 == 0).collect::<Bitmap>()),
        ] {
            let wrapped = column.clone().wrap_nullable(validity.clone());
            assert_eq!(wrapped.data_type(), column.data_type().wrap_nullable());
            let Column::Nullable(wrapped) = wrapped else {
                panic!("expected nullable column");
            };
            assert_eq!(wrapped.column(), &column);
            assert_eq!(
                wrapped.validity(),
                &validity.unwrap_or_else(|| Bitmap::new_constant(true, len)),
            );
        }
    }
}

#[test]
fn test_wrap_nullable_merges_validity() {
    for len in [0, 4] {
        let column = Int64Type::from_data(vec![1; len]);
        let bitmaps = [
            Bitmap::new_constant(true, len),
            Bitmap::new_constant(false, len),
            (0..len).map(|i| i % 2 == 0).collect::<Bitmap>(),
            (0..len).map(|i| i < 2).collect::<Bitmap>(),
        ];
        for inner in &bitmaps {
            let nullable = column.clone().wrap_nullable(Some(inner.clone()));
            assert_eq!(nullable.clone().wrap_nullable(None), nullable);
            for outer in &bitmaps {
                let wrapped = nullable.clone().wrap_nullable(Some(outer.clone()));
                assert_eq!(wrapped.data_type(), nullable.data_type());
                // Wrapping repeatedly must not add nullable layers or revive null rows.
                assert_eq!(wrapped.clone().wrap_nullable(Some(outer.clone())), wrapped);
                let Column::Nullable(wrapped) = wrapped else {
                    panic!("expected nullable column");
                };
                assert_eq!(wrapped.column(), &column);
                assert_eq!(wrapped.validity(), &(inner & outer));
            }
        }
    }
}

#[test]
fn test_wrap_nullable_null() {
    for len in [0, 4] {
        let column = Column::Null { len };
        for validity in [
            None,
            Some(Bitmap::new_constant(true, len)),
            Some(Bitmap::new_constant(false, len)),
        ] {
            assert_eq!(column.clone().wrap_nullable(validity), column);
        }
    }
}

#[test]
fn test_wrap_nullable_rejects_mismatched_validity() {
    let column = Int64Type::from_data(vec![1_i64, 2]);
    for column in [column.clone(), column.wrap_nullable(None), Column::Null {
        len: 2,
    }] {
        assert!(
            databend_common_base::runtime::catch_unwind(|| {
                column.wrap_nullable(Some(Bitmap::new_constant(true, 1)))
            })
            .is_err()
        );
    }
}
