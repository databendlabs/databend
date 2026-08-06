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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::OrderedFloat;
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
fn test_aggregate_state_parameter_roundtrip() -> anyhow::Result<()> {
    let state_type = TableDataType::AggregateState {
        function_name: "quantile_tdigest".to_string(),
        params: vec![AggregateFunctionParam::Number(NumberScalar::Float64(
            OrderedFloat(0.5),
        ))],
        argument_types: vec![TableDataType::Number(NumberDataType::UInt64)],
        state_type: Box::new(TableDataType::Binary),
    };
    let schema = TableSchema::new(vec![TableField::new("state", state_type)]);

    common::test_pb_from_to(func_name!(), schema)?;
    Ok(())
}
