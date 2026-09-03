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
use databend_common_expression::types::timestamp::timestamp_to_rfc3339_utc;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_timezone::Tz;

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
