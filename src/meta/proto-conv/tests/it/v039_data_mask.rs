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

use chrono::TimeZone;
use chrono::Utc;
use fastrace::func_name;

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
fn test_decode_v39_data_mask() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 11, 10, 1, 97, 18, 6, 83, 116, 114, 105, 110, 103, 18, 6, 83, 116, 114, 105, 110, 103,
        26, 68, 67, 65, 83, 69, 32, 87, 72, 69, 78, 32, 99, 117, 114, 114, 101, 110, 116, 95, 114,
        111, 108, 101, 40, 41, 32, 73, 78, 40, 39, 65, 78, 65, 76, 89, 83, 84, 39, 41, 32, 84, 72,
        69, 78, 32, 86, 65, 76, 32, 69, 76, 83, 69, 32, 39, 42, 42, 42, 42, 42, 42, 42, 42, 42, 39,
        32, 69, 78, 68, 34, 12, 115, 111, 109, 101, 32, 99, 111, 109, 109, 101, 110, 116, 42, 23,
        50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67,
        50, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85,
        84, 67, 160, 6, 39, 168, 6, 24,
    ];

    let want = || databend_common_meta_app::data_mask::DatamaskMeta {
        args: vec![("a".to_string(), "String".to_string())],
        return_type: "String".to_string(),
        body: "CASE WHEN current_role() IN('ANALYST') THEN VAL ELSE '*********' END".to_string(),
        comment: Some("some comment".to_string()),
        create_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        update_on: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 39, want())
}
