// Copyright 2026 Datafuse Labs.
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
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v181_lineage_detail() -> anyhow::Result<()> {
    let lineage_detail_v181 = vec![
        8, 4, 18, 2, 113, 49, 26, 23, 50, 48, 50, 54, 45, 48, 55, 45, 50, 51, 32, 48, 48, 58, 48,
        48, 58, 48, 48, 32, 85, 84, 67, 34, 8, 10, 2, 8, 1, 18, 2, 8, 2, 160, 6, 181, 1, 168, 6,
        24,
    ];

    let want = mt::LineageDetail {
        kind: mt::LineageKind::DataMovement,
        last_query_id: Some("q1".to_string()),
        updated_on: Utc.with_ymd_and_hms(2026, 7, 23, 0, 0, 0).unwrap(),
        column_lineage: vec![mt::LineageColumn {
            upstream: mt::ColumnRef::Id(1),
            downstream: mt::ColumnRef::Id(2),
        }],
    };

    common::test_pb_from_to(func_name!(), want.clone())?;
    common::test_load_old(func_name!(), lineage_detail_v181.as_slice(), 181, want)?;

    Ok(())
}
