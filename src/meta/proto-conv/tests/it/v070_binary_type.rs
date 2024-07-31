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
// The binary_type_v70 bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v70_binary_type() -> anyhow::Result<()> {
    let binary_type_v70 = vec![
        10, 20, 10, 1, 97, 26, 9, 242, 2, 0, 160, 6, 70, 168, 6, 24, 160, 6, 70, 168, 6, 24, 24, 1,
        160, 6, 70, 168, 6, 24,
    ];
    let want = || TableSchema::new(vec![TableField::new("a", TableDataType::Binary)]);
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), binary_type_v70.as_slice(), 70, want())?;

    Ok(())
}
