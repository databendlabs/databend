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
use databend_common_meta_app::schema::SegmentClaimMeta;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version is introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// Encoded data of version 184 of SegmentClaimMeta.
// It is generated with common::test_pb_from_to().
#[test]
fn test_decode_v184_segment_claim_meta() -> anyhow::Result<()> {
    let segment_claim_meta_v184 = vec![
        10, 4, 114, 111, 111, 116, 18, 4, 110, 111, 100, 101, 26, 5, 113, 117, 101, 114, 121, 34,
        23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84,
        67, 42, 9, 115, 101, 103, 109, 101, 110, 116, 45, 49, 42, 9, 115, 101, 103, 109, 101, 110,
        116, 45, 50, 160, 6, 184, 1, 168, 6, 24,
    ];

    let want = || SegmentClaimMeta {
        user: "root".to_string(),
        node: "node".to_string(),
        query_id: "query".to_string(),
        created_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        segment_locations: vec!["segment-1".to_string(), "segment-2".to_string()],
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        segment_claim_meta_v184.as_slice(),
        184,
        want(),
    )?;

    Ok(())
}
