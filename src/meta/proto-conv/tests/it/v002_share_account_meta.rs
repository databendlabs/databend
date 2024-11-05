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
use databend_common_meta_app::share;
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
fn test_decode_v2_share_account_meta() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 7, 97, 99, 99, 111, 117, 110, 116, 16, 4, 26, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 34, 23, 50, 48, 49, 52, 45, 49, 49,
        45, 50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 160, 6, 2, 168, 6, 1,
    ];

    let want = || share::ShareAccountMeta {
        account: "account".into(),
        share_id: 4,
        share_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        accept_on: Some(Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap()),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 2, want())
}
