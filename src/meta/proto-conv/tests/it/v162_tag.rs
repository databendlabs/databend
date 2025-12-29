// Copyright 2025 Datafuse Labs.
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
use databend_common_meta_app::schema::ObjectTagIdRefValue;
use databend_common_meta_app::schema::TagMeta;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v162_tag_meta() -> anyhow::Result<()> {
    let tag_meta_v162 = vec![
        10, 5, 97, 108, 112, 104, 97, 10, 4, 98, 101, 116, 97, 18, 23, 116, 97, 103, 32, 100, 101,
        115, 99, 114, 105, 98, 105, 110, 103, 32, 112, 114, 105, 111, 114, 105, 116, 121, 26, 23,
        50, 48, 50, 52, 45, 49, 50, 45, 49, 50, 32, 48, 55, 58, 51, 48, 58, 48, 48, 32, 85, 84, 67,
        34, 23, 50, 48, 50, 52, 45, 49, 50, 45, 51, 49, 32, 48, 51, 58, 48, 53, 58, 48, 54, 32, 85,
        84, 67, 48, 1, 160, 6, 162, 1, 168, 6, 24,
    ];
    let want = || TagMeta {
        allowed_values: vec!["alpha".to_string(), "beta".to_string()],
        enforce_allowed_values: true,
        comment: "tag describing priority".to_string(),
        created_on: Utc.with_ymd_and_hms(2024, 12, 12, 7, 30, 0).unwrap(),
        updated_on: Some(Utc.with_ymd_and_hms(2024, 12, 31, 3, 5, 6).unwrap()),
        drop_on: None,
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), tag_meta_v162.as_slice(), 162, want())?;
    Ok(())
}

#[test]
fn test_tag_ref_value() -> anyhow::Result<()> {
    let tag_ref_value_v162 = vec![10, 3, 100, 101, 118, 160, 6, 162, 1, 168, 6, 24];
    let want = || ObjectTagIdRefValue {
        tag_allowed_value: "dev".to_string(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), tag_ref_value_v162.as_slice(), 162, want())?;
    Ok(())
}
