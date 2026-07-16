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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use databend_meta_client::types::anyerror::func_name;

use crate::common;

#[test]
fn test_decode_v180_materialized_view_meta() -> anyhow::Result<()> {
    let mv_meta_v180 = vec![
        8, 7, 18, 35, 8, 11, 18, 24, 49, 47, 50, 47, 95, 115, 115, 47, 115, 110, 97, 112, 115, 104,
        111, 116, 95, 118, 52, 46, 106, 115, 111, 110, 160, 6, 180, 1, 168, 6, 24, 34, 23, 50, 48,
        50, 54, 45, 48, 55, 45, 49, 48, 32, 48, 51, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6,
        180, 1, 168, 6, 24,
    ];

    let want = || mt::MVMeta {
        source_table_id: 7,
        source_progress: mt::SourceProgress {
            table_meta_seq: 11,
            snapshot_location: Some("1/2/_ss/snapshot_v4.json".to_string()),
        },
        state: mt::MVState::Valid,
        last_refresh_time: Some(Utc.with_ymd_and_hms(2026, 7, 10, 3, 0, 0).unwrap()),
        last_refresh_error: None,
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), mv_meta_v180.as_slice(), 180, want())?;

    Ok(())
}

#[test]
fn test_decode_v180_materialized_view_definition() -> anyhow::Result<()> {
    let mv_definition_v180 = vec![
        10, 16, 83, 69, 76, 69, 67, 84, 32, 105, 100, 32, 70, 82, 79, 77, 32, 116, 18, 32, 83, 69,
        76, 69, 67, 84, 32, 105, 100, 32, 70, 82, 79, 77, 32, 100, 101, 102, 97, 117, 108, 116, 46,
        100, 101, 102, 97, 117, 108, 116, 46, 116, 160, 6, 180, 1, 168, 6, 24,
    ];

    let want = || mt::MVDefinition {
        original_query: "SELECT id FROM t".to_string(),
        query: "SELECT id FROM default.default.t".to_string(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), mv_definition_v180.as_slice(), 180, want())?;

    Ok(())
}

#[test]
fn test_decode_v180_materialized_view_source_index() -> anyhow::Result<()> {
    let source_index_v180 = vec![10, 2, 42, 43, 160, 6, 180, 1, 168, 6, 24];

    let want =
        || mt::MVSourceIndex::try_from_ids(vec![mt::MVId::new(42), mt::MVId::new(43)]).unwrap();

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), source_index_v180.as_slice(), 180, want())?;

    Ok(())
}
