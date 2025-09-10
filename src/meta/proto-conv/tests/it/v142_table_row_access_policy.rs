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
use fastrace::func_name;
use maplit::btreeset;

use crate::common;

#[test]
fn test_decode_v142_table_meta() -> anyhow::Result<()> {
    let table_meta_v142 = vec![
        10, 7, 160, 6, 142, 1, 168, 6, 24, 64, 0, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49,
        49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 186, 1, 7, 160, 6, 142,
        1, 168, 6, 24, 226, 1, 1, 1, 138, 2, 2, 112, 49, 160, 6, 142, 1, 168, 6, 24,
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
        row_access_policy: Some("p1".to_string()),
        indexes: BTreeMap::default(),
        constraints: BTreeMap::default(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_meta_v142.as_slice(), 142, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
