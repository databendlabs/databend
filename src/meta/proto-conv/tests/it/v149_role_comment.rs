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

use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::UserGrantSet;
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

#[test]
fn test_decode_v149_role() -> anyhow::Result<()> {
    let role_info_v149 = vec![
        10, 2, 114, 49, 18, 7, 160, 6, 149, 1, 168, 6, 24, 26, 23, 50, 48, 50, 51, 45, 49, 50, 45,
        49, 53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 34, 23, 50, 48, 50, 51, 45, 49,
        50, 45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 49, 48, 32, 85, 84, 67, 42, 12, 116, 101, 115,
        116, 95, 99, 111, 109, 109, 101, 110, 116, 160, 6, 149, 1, 168, 6, 24,
    ];

    let want = || mt::principal::RoleInfo {
        name: "r1".to_string(),
        grants: UserGrantSet::new(vec![], HashSet::new()),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(1702603570, 0).unwrap(),
        comment: Some("test_comment".to_string()),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v149.as_slice(), 149, want())?;

    Ok(())
}
