// Copyright 2026 Datafuse Labs.
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
use databend_common_expression::types::NumberDataType;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v187_aggregate_state_version() -> anyhow::Result<()> {
    // These bytes are frozen at metadata version 187. Do not update them.
    let table_schema_v187 = vec![
        10, 86, 10, 9, 115, 117, 109, 95, 115, 116, 97, 116, 101, 26, 66, 178, 3, 56, 10, 3, 115,
        117, 109, 26, 19, 154, 2, 9, 34, 0, 160, 6, 187, 1, 168, 6, 24, 160, 6, 187, 1, 168, 6, 24,
        34, 19, 154, 2, 9, 34, 0, 160, 6, 187, 1, 168, 6, 24, 160, 6, 187, 1, 168, 6, 24, 40, 7,
        160, 6, 187, 1, 168, 6, 24, 160, 6, 187, 1, 168, 6, 24, 160, 6, 187, 1, 168, 6, 24, 24, 1,
        160, 6, 187, 1, 168, 6, 24,
    ];
    let want = || {
        let state_type = TableDataType::AggregateState {
            function_name: "sum".to_string(),
            params: vec![],
            argument_types: vec![TableDataType::Number(NumberDataType::UInt64)],
            state_type: Box::new(TableDataType::Number(NumberDataType::UInt64)),
            state_version: 7,
        };
        TableSchema::new(vec![TableField::new("sum_state", state_type)])
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), &table_schema_v187, 187, want())?;
    Ok(())
}
