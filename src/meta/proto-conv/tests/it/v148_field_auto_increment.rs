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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::SequenceMeta;
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
        10, 47, 10, 1, 97, 26, 19, 154, 2, 9, 42, 0, 160, 6, 148, 1, 168, 6, 24, 160, 6, 148, 1,
        168, 6, 24, 50, 14, 65, 85, 84, 79, 32, 73, 78, 67, 82, 69, 77, 69, 78, 84, 160, 6, 148, 1,
        168, 6, 24, 10, 49, 10, 1, 98, 26, 19, 154, 2, 9, 42, 0, 160, 6, 148, 1, 168, 6, 24, 160,
        6, 148, 1, 168, 6, 24, 32, 1, 50, 14, 65, 85, 84, 79, 32, 73, 78, 67, 82, 69, 77, 69, 78,
        84, 160, 6, 148, 1, 168, 6, 24, 10, 33, 10, 1, 99, 26, 19, 154, 2, 9, 42, 0, 160, 6, 148,
        1, 168, 6, 24, 160, 6, 148, 1, 168, 6, 24, 32, 2, 160, 6, 148, 1, 168, 6, 24, 24, 3, 160,
        6, 148, 1, 168, 6, 24,
    ];

    let want = || {
        let mut field_a = ce::TableField::new("a", TableDataType::Number(NumberDataType::Int8))
            .with_auto_increment_display(Some(s("AUTO INCREMENT")));
        let mut field_b = ce::TableField::new("b", TableDataType::Number(NumberDataType::Int8))
            .with_auto_increment_display(Some(s("AUTO INCREMENT")));
        let mut field_c = ce::TableField::new("c", TableDataType::Number(NumberDataType::Int8));

        field_a.column_id = 0;
        field_b.column_id = 1;
        field_c.column_id = 2;

        ce::TableSchema {
            fields: vec![field_a, field_b, field_c],
            metadata: Default::default(),
            next_column_id: 3,
        }
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v148.as_slice(), 148, want())?;

    Ok(())
}

#[test]
fn test_decode_v148_auto_increment_meta() -> anyhow::Result<()> {
    let auto_increment_meta_v148 = vec![
        10, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85,
        84, 67, 18, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57,
        32, 85, 84, 67, 26, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 40, 1, 48, 2,
        160, 6, 148, 1, 168, 6, 24,
    ];

    let want = || {
        mt::AutoIncrementMeta(SequenceMeta {
            create_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
            update_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
            comment: Some("hello world".to_string()),
            step: 1,
            current: 2,
            storage_version: 0,
        })
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        auto_increment_meta_v148.as_slice(),
        148,
        want(),
    )?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
