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
fn test_decode_v146_constraint() -> anyhow::Result<()> {
    let table_meta_v142 = vec![
        10, 7, 160, 6, 146, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 146,
        1, 168, 6, 24, 226, 1, 1, 1, 146, 2, 32, 10, 12, 99, 111, 110, 115, 116, 114, 97, 105, 110,
        116, 95, 49, 18, 16, 18, 7, 99, 49, 32, 62, 32, 49, 48, 160, 6, 146, 1, 168, 6, 24, 146, 2,
        32, 10, 12, 99, 111, 110, 115, 116, 114, 97, 105, 110, 116, 95, 50, 18, 16, 18, 7, 99, 49,
        32, 33, 61, 32, 48, 160, 6, 146, 1, 168, 6, 24, 160, 6, 146, 1, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ce::TableSchema::default()),
        engine: s(""),
        storage_params: None,
        part_prefix: s(""),
        engine_options: BTreeMap::default(),
        options: BTreeMap::default(),
        cluster_key: None,
        cluster_key_seq: 0,
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s(""),
        field_comments: vec![],
        virtual_schema: None,
        drop_on: None,
        statistics: Default::default(),
        shared_by: btreeset! {1},
        column_mask_policy: None,
        column_mask_policy_columns_ids: BTreeMap::new(),
        row_access_policy: None,
        row_access_policy_columns_ids: None,
        indexes: BTreeMap::default(),
        constraints: btreemap! {
            "constraint_1".to_string() => Constraint::Check("c1 > 10".to_string()),
            "constraint_2".to_string() => Constraint::Check("c1 != 0".to_string()),
        },
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_meta_v142.as_slice(), 146, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
