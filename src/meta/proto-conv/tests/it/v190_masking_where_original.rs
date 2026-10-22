// Copyright 2021 Datafuse Labs
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
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::MIN_READER_VER;
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
fn test_decode_v190_masking_where_unmasked_value() -> anyhow::Result<()> {
    // Run test_pb_from_to first to get these bytes
    let table_meta_v190: Vec<u8> = vec![
        10, 7, 160, 6, 190, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 190,
        1, 168, 6, 24, 162, 2, 14, 18, 12, 8, 7, 18, 1, 0, 160, 6, 190, 1, 168, 6, 24, 200, 2, 0,
        218, 2, 1, 0, 160, 6, 190, 1, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ce::TableSchema::default()),
        engine: s(""),
        storage_params: None,
        part_prefix: s(""),
        engine_options: btreemap! {},
        options: btreemap! {},
        cluster_key: None,
        cluster_key_v2: None,
        cluster_key_seq: 0,
        partition_key_seq: 0,
        partition_key_id: None,
        ttl: None,
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s(""),
        field_comments: vec![],
        field_stats_truncate_len: btreemap! {},
        virtual_schema: None,
        drop_on: None,
        statistics: Default::default(),
        column_mask_policy: None,
        column_mask_policy_columns_ids: btreemap! {
            0 => mt::SecurityPolicyColumnMap::new(7, vec![0]),
        },
        masking_where_use_unmasked_value: btreeset! {0},
        row_access_policy: None,
        row_access_policy_columns_ids: None,
        indexes: btreemap! {},
        constraints: btreemap! {},
    };
    common::test_pb_from_to(func_name!(), want())?;

    common::test_load_old(func_name!(), table_meta_v190.as_slice(), 190, want())?;

    // The authorization must not lock older readers out of the table: an older
    // reader ignores field 43 and keeps masked WHERE semantics instead.
    assert_eq!(want().to_pb().min_reader_ver, MIN_READER_VER);

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
