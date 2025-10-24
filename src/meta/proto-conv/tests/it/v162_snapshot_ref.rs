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

use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::SnapshotRef;
use databend_common_meta_app::schema::SnapshotRefType;
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
fn test_decode_v162_snapshot_ref() -> anyhow::Result<()> {
    let table_meta_v162 = vec![
        10, 7, 160, 6, 162, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 162,
        1, 168, 6, 24, 226, 1, 1, 1, 170, 2, 49, 10, 8, 98, 114, 97, 110, 99, 104, 95, 49, 18, 37,
        8, 1, 18, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57,
        32, 85, 84, 67, 34, 1, 97, 160, 6, 162, 1, 168, 6, 24, 170, 2, 23, 10, 5, 116, 97, 103, 95,
        49, 18, 14, 8, 2, 24, 1, 34, 1, 99, 160, 6, 162, 1, 168, 6, 24, 160, 6, 162, 1, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ce::TableSchema::default()),
        engine: s(""),
        storage_params: None,
        part_prefix: s(""),
        engine_options: btreemap! {},
        options: btreemap! {},
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
        column_mask_policy_columns_ids: btreemap! {},
        row_access_policy: None,
        row_access_policy_columns_ids: None,
        indexes: btreemap! {},
        constraints: btreemap! {},
        refs: btreemap! {
            "branch_1".to_string() => SnapshotRef {
                id: 1,
                expire_at: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
                typ: SnapshotRefType::Branch,
                loc: "a".to_string(),
            },
            "tag_1".to_string() => SnapshotRef {
                id: 2,
                expire_at: None,
                typ: SnapshotRefType::Tag,
                loc: "c".to_string(),
            }
        },
    };
    common::test_pb_from_to(func_name!(), want())?;

    common::test_load_old(func_name!(), table_meta_v162.as_slice(), 162, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
