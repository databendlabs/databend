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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::Constraint;
use fastrace::func_name;
use maplit::btreemap;
use maplit::btreeset;

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
fn test_decode_v147_field_auto_increment() -> anyhow::Result<()> {
    let table_schema_v147 = vec![];

    let want = || ce::TableSchema {
        fields: vec![
            ce::TableField::new("a", TableDataType::Number(NumberDataType::Int8))
                .with_auto_increment_name(Some(s("sequence_1")))
                .with_auto_increment_display(Some(s("AUTO INCREMENT"))),
            ce::TableField::new("b", TableDataType::Number(NumberDataType::Int8))
                .with_auto_increment_name(Some(s("sequence_2"))),
            ce::TableField::new("c", TableDataType::Number(NumberDataType::Int8))
                .with_auto_increment_display(Some(s("AUTO INCREMENT"))),
            ce::TableField::new("d", TableDataType::Number(NumberDataType::Int8)),
        ],
        metadata: Default::default(),
        next_column_id: 0,
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v147.as_slice(), 147, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
