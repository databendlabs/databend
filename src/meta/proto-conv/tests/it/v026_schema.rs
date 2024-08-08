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

use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v26_schema() -> anyhow::Result<()> {
    let schema_v26 = [
        10, 28, 10, 1, 97, 26, 17, 154, 2, 8, 34, 0, 160, 6, 26, 168, 6, 24, 160, 6, 26, 168, 6,
        24, 160, 6, 26, 168, 6, 24, 10, 104, 10, 1, 98, 26, 91, 202, 2, 82, 10, 2, 98, 49, 10, 2,
        98, 50, 18, 47, 202, 2, 38, 10, 3, 98, 49, 49, 10, 3, 98, 49, 50, 18, 9, 138, 2, 0, 160, 6,
        26, 168, 6, 24, 18, 9, 146, 2, 0, 160, 6, 26, 168, 6, 24, 160, 6, 26, 168, 6, 24, 160, 6,
        26, 168, 6, 24, 18, 17, 154, 2, 8, 66, 0, 160, 6, 26, 168, 6, 24, 160, 6, 26, 168, 6, 24,
        160, 6, 26, 168, 6, 24, 160, 6, 26, 168, 6, 24, 32, 1, 160, 6, 26, 168, 6, 24, 10, 30, 10,
        1, 99, 26, 17, 154, 2, 8, 34, 0, 160, 6, 26, 168, 6, 24, 160, 6, 26, 168, 6, 24, 32, 4,
        160, 6, 26, 168, 6, 24, 24, 5, 160, 6, 26, 168, 6, 24,
    ];

    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
    ];
    let want = || TableSchema::new(fields.clone());

    common::test_load_old(func_name!(), schema_v26.as_slice(), 26, want())?;
    common::test_pb_from_to(func_name!(), want())?;

    Ok(())
}
