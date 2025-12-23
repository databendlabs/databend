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
use databend_common_expression::types::NumberDataType;
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
fn test_decode_v145_schema() -> anyhow::Result<()> {
    let table_schema_v145 = vec![
        10, 31, 10, 1, 97, 26, 19, 154, 2, 9, 34, 0, 160, 6, 145, 1, 168, 6, 24, 160, 6, 145, 1,
        168, 6, 24, 160, 6, 145, 1, 168, 6, 24, 10, 33, 10, 1, 99, 26, 19, 154, 2, 9, 34, 0, 160,
        6, 145, 1, 168, 6, 24, 160, 6, 145, 1, 168, 6, 24, 32, 1, 160, 6, 145, 1, 168, 6, 24, 10,
        30, 10, 7, 111, 112, 97, 113, 117, 101, 49, 26, 10, 152, 3, 1, 160, 6, 145, 1, 168, 6, 24,
        32, 2, 160, 6, 145, 1, 168, 6, 24, 10, 30, 10, 7, 111, 112, 97, 113, 117, 101, 50, 26, 10,
        152, 3, 2, 160, 6, 145, 1, 168, 6, 24, 32, 3, 160, 6, 145, 1, 168, 6, 24, 10, 30, 10, 7,
        111, 112, 97, 113, 117, 101, 51, 26, 10, 152, 3, 3, 160, 6, 145, 1, 168, 6, 24, 32, 4, 160,
        6, 145, 1, 168, 6, 24, 10, 30, 10, 7, 111, 112, 97, 113, 117, 101, 52, 26, 10, 152, 3, 4,
        160, 6, 145, 1, 168, 6, 24, 32, 5, 160, 6, 145, 1, 168, 6, 24, 10, 30, 10, 7, 111, 112, 97,
        113, 117, 101, 53, 26, 10, 152, 3, 5, 160, 6, 145, 1, 168, 6, 24, 32, 6, 160, 6, 145, 1,
        168, 6, 24, 10, 30, 10, 7, 111, 112, 97, 113, 117, 101, 54, 26, 10, 152, 3, 6, 160, 6, 145,
        1, 168, 6, 24, 32, 7, 160, 6, 145, 1, 168, 6, 24, 24, 8, 160, 6, 145, 1, 168, 6, 24,
    ];

    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("opaque1", TableDataType::Opaque(1)),
        TableField::new("opaque2", TableDataType::Opaque(2)),
        TableField::new("opaque3", TableDataType::Opaque(3)),
        TableField::new("opaque4", TableDataType::Opaque(4)),
        TableField::new("opaque5", TableDataType::Opaque(5)),
        TableField::new("opaque6", TableDataType::Opaque(6)),
    ];
    let want = || TableSchema::new(fields.clone());
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v145.as_slice(), 145, want())?;
    Ok(())
}
