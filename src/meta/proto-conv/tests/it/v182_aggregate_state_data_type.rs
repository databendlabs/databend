// Copyright 2023 Datafuse Labs.
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

use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::OrderedFloat;
use databend_common_expression::types::decimal::i256;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v182_schema() -> anyhow::Result<()> {
    let table_schema_v182 = vec![
        10, 121, 10, 9, 115, 117, 109, 95, 115, 116, 97, 116, 101, 26, 101, 178, 3, 91, 10, 3, 115,
        117, 109, 26, 19, 154, 2, 9, 34, 0, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24,
        34, 56, 202, 2, 46, 10, 1, 49, 10, 1, 50, 18, 19, 154, 2, 9, 34, 0, 160, 6, 182, 1, 168, 6,
        24, 160, 6, 182, 1, 168, 6, 24, 18, 10, 138, 2, 0, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182,
        1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168,
        6, 24, 160, 6, 182, 1, 168, 6, 24, 24, 2, 160, 6, 182, 1, 168, 6, 24,
    ];

    let want = || {
        let state_type = TableDataType::AggregateState {
            function_name: "sum".to_string(),
            params: vec![],
            argument_types: vec![TableDataType::Number(NumberDataType::UInt64)],
            state_type: Box::new(TableDataType::Tuple {
                fields_name: vec!["1".to_string(), "2".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::UInt64),
                    TableDataType::Boolean,
                ],
            }),
        };
        TableSchema::new(vec![TableField::new("sum_state", state_type)])
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v182.as_slice(), 182, want())?;
    Ok(())
}

#[test]
fn test_decode_v182_schema_with_parameter() -> anyhow::Result<()> {
    let table_schema_v182 = vec![
        10, 111, 10, 5, 115, 116, 97, 116, 101, 26, 95, 178, 3, 85, 10, 16, 113, 117, 97, 110, 116,
        105, 108, 101, 95, 116, 100, 105, 103, 101, 115, 116, 18, 25, 18, 16, 81, 0, 0, 0, 0, 0, 0,
        224, 63, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 26, 19, 154, 2, 9, 34, 0,
        160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 34, 10, 242, 2, 0, 160, 6, 182, 1,
        168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6, 24, 160, 6, 182, 1, 168, 6,
        24, 24, 1, 160, 6, 182, 1, 168, 6, 24,
    ];

    let want = || {
        let state_type = TableDataType::AggregateState {
            function_name: "quantile_tdigest".to_string(),
            params: vec![AggregateFunctionParam::Number(NumberScalar::Float64(
                OrderedFloat(0.5),
            ))],
            argument_types: vec![TableDataType::Number(NumberDataType::UInt64)],
            state_type: Box::new(TableDataType::Binary),
        };
        TableSchema::new(vec![TableField::new("state", state_type)])
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v182.as_slice(), 182, want())?;
    Ok(())
}

#[test]
fn test_aggregate_state_all_parameter_types_roundtrip() -> anyhow::Result<()> {
    let params = vec![
        AggregateFunctionParam::Null,
        AggregateFunctionParam::Number(NumberScalar::UInt8(u8::MAX)),
        AggregateFunctionParam::Number(NumberScalar::UInt16(u16::MAX)),
        AggregateFunctionParam::Number(NumberScalar::UInt32(u32::MAX)),
        AggregateFunctionParam::Number(NumberScalar::UInt64(u64::MAX)),
        AggregateFunctionParam::Number(NumberScalar::Int8(i8::MIN)),
        AggregateFunctionParam::Number(NumberScalar::Int16(i16::MIN)),
        AggregateFunctionParam::Number(NumberScalar::Int32(i32::MIN)),
        AggregateFunctionParam::Number(NumberScalar::Int64(i64::MIN)),
        AggregateFunctionParam::Number(NumberScalar::Float32(OrderedFloat(-0.0))),
        AggregateFunctionParam::Number(NumberScalar::Float64(OrderedFloat(f64::NAN))),
        AggregateFunctionParam::Decimal(DecimalScalar::Decimal64(
            i64::MIN,
            DecimalSize::new(18, 3)?,
        )),
        AggregateFunctionParam::Decimal(DecimalScalar::Decimal128(
            i128::MIN,
            DecimalSize::new(38, 6)?,
        )),
        AggregateFunctionParam::Decimal(DecimalScalar::Decimal256(
            i256::from_words(i128::MIN, 0),
            DecimalSize::new(76, 9)?,
        )),
        AggregateFunctionParam::Timestamp(i64::MIN),
        AggregateFunctionParam::timestamp_tz_from_parts(i64::MIN, -8 * 3600),
        AggregateFunctionParam::Date(i32::MIN),
        AggregateFunctionParam::interval_from_parts(i32::MIN, i32::MAX, i64::MIN),
        AggregateFunctionParam::Boolean(true),
        AggregateFunctionParam::Binary(vec![0, 1, 255]),
        AggregateFunctionParam::String("parameter".to_string()),
        AggregateFunctionParam::Bitmap(vec![1, 2, 3]),
        AggregateFunctionParam::Tuple(vec![
            AggregateFunctionParam::Null,
            AggregateFunctionParam::String("nested".to_string()),
        ]),
        AggregateFunctionParam::Variant(vec![4, 5, 6]),
        AggregateFunctionParam::Geometry(vec![7, 8, 9]),
    ];
    let state_type = TableDataType::AggregateState {
        function_name: "test".to_string(),
        params,
        argument_types: vec![TableDataType::Number(NumberDataType::UInt64)],
        state_type: Box::new(TableDataType::Binary),
    };
    let schema = TableSchema::new(vec![TableField::new("state", state_type)]);

    common::test_pb_from_to(func_name!(), schema)?;
    Ok(())
}
