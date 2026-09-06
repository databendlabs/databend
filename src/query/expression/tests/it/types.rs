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
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::types::timestamp::timestamp_to_rfc3339_utc;
use databend_common_expression::types::timestamp::timestamp_to_string;

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
            (253_402_300_799_999_999, 253_402_300_799_999_999),
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
        "+11000-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        timestamp_to_rfc3339_utc(i64::MIN),
        "0001-01-01T00:00:00.000000Z"
    );
}

#[test]
fn test_datetime_clamp_to_minimum() {
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

    let values = [
        i64::MIN,
        TIMESTAMP_MIN - 1,
        TIMESTAMP_MIN,
        -1,
        0,
        TIMESTAMP_MAX,
        TIMESTAMP_MAX + 1,
        i64::MAX,
    ];
    let mut expected = Vec::new();
    let mut bytes = Vec::new();
    let mut scalar_builder = ColumnBuilder::with_capacity(&DataType::Timestamp, values.len());
    for value in values {
        let mut clamped = value;
        clamp_timestamp(&mut clamped);
        let expected_value = if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&value) {
            value
        } else {
            TIMESTAMP_MIN
        };
        assert_eq!(clamped, expected_value);
        expected.push(expected_value);
        let encoded = value.to_le_bytes();
        scalar_builder.push_binary(&mut encoded.as_slice()).unwrap();
        bytes.extend(encoded);
    }
    let mut batch_builder = ColumnBuilder::with_capacity(&DataType::Timestamp, values.len());
    batch_builder
        .push_fix_len_binaries(&bytes, size_of::<i64>(), values.len())
        .unwrap();
    for column in [scalar_builder.build(), batch_builder.build()] {
        let Column::Timestamp(values) = column else {
            panic!("expected timestamp column");
        };
        assert_eq!(values.as_slice(), expected.as_slice());
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
