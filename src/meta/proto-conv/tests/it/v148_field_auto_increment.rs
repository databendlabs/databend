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

use databend_common_expression as ce;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::TableDataType;
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
fn test_decode_v148_field_auto_increment() -> anyhow::Result<()> {
    let table_schema_v148 = vec![
        10, 44, 10, 1, 97, 26, 19, 154, 2, 9, 42, 0, 160, 6, 148, 1, 168, 6, 24, 160, 6, 148, 1,
        168, 6, 24, 50, 11, 8, 1, 16, 2, 160, 6, 148, 1, 168, 6, 24, 160, 6, 148, 1, 168, 6, 24,
        10, 33, 10, 1, 98, 26, 19, 154, 2, 9, 42, 0, 160, 6, 148, 1, 168, 6, 24, 160, 6, 148, 1,
        168, 6, 24, 32, 1, 160, 6, 148, 1, 168, 6, 24, 24, 2, 160, 6, 148, 1, 168, 6, 24,
    ];

    let want = || {
        let mut field_a = ce::TableField::new("a", TableDataType::Number(NumberDataType::Int8))
            .with_auto_increment_expr(Some(AutoIncrementExpr {
                column_id: 0,
                start: 1,
                step: 2,
                is_ordered: false,
            }));
        let mut field_b = ce::TableField::new("b", TableDataType::Number(NumberDataType::Int8));

        field_a.column_id = 0;
        field_b.column_id = 1;

        ce::TableSchema {
            fields: vec![field_a, field_b],
            metadata: Default::default(),
            next_column_id: 2,
        }
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v148.as_slice(), 148, want())?;

    Ok(())
}
