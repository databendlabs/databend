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
fn test_decode_v179_materialized_view_meta() -> anyhow::Result<()> {
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

    Ok(())
}

#[test]
fn test_decode_v179_materialized_view_definition() -> anyhow::Result<()> {
    let want = || mt::MVDefinition {
        original_query: "SELECT id FROM t".to_string(),
        query: "SELECT id FROM default.default.t".to_string(),
    };

    common::test_pb_from_to(func_name!(), want())?;

    Ok(())
}
