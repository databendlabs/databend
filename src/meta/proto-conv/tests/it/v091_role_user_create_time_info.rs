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
use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::make_bitflags;
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
fn test_decode_v91_role() -> anyhow::Result<()> {
    let role_info_v91 = vec![
        10, 2, 114, 49, 18, 6, 160, 6, 91, 168, 6, 24, 26, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49,
        53, 32, 48, 49, 58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 34, 23, 50, 48, 50, 51, 45, 49, 50,
        45, 49, 53, 32, 48, 49, 58, 50, 54, 58, 49, 48, 32, 85, 84, 67, 160, 6, 91, 168, 6, 24,
    ];

    let want = || mt::principal::RoleInfo {
        name: "r1".to_string(),
        grants: UserGrantSet::new(vec![], HashSet::new()),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(1702603570, 0).unwrap(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v91.as_slice(), 91, want())?;

    Ok(())
}

#[test]
fn test_decode_v91_user() -> anyhow::Result<()> {
    let user_info_v91 = vec![
        10, 2, 117, 49, 18, 1, 37, 26, 8, 10, 0, 160, 6, 91, 168, 6, 24, 34, 70, 10, 31, 10, 21,
        58, 13, 10, 7, 100, 101, 102, 97, 117, 108, 116, 16, 1, 24, 10, 160, 6, 91, 168, 6, 24, 16,
        4, 160, 6, 91, 168, 6, 24, 10, 29, 10, 19, 50, 11, 10, 7, 100, 101, 102, 97, 117, 108, 116,
        16, 1, 160, 6, 91, 168, 6, 24, 16, 2, 160, 6, 91, 168, 6, 24, 160, 6, 91, 168, 6, 24, 42,
        6, 160, 6, 91, 168, 6, 24, 50, 6, 160, 6, 91, 168, 6, 24, 58, 32, 18, 24, 10, 20, 164, 182,
        21, 115, 25, 3, 135, 36, 227, 86, 8, 148, 247, 249, 50, 200, 136, 110, 191, 207, 16, 1,
        160, 6, 91, 168, 6, 24, 58, 32, 18, 24, 10, 20, 161, 84, 197, 37, 101, 233, 231, 249, 75,
        252, 8, 161, 254, 112, 38, 36, 237, 142, 255, 218, 16, 1, 160, 6, 91, 168, 6, 24, 66, 23,
        50, 48, 50, 51, 45, 49, 50, 45, 50, 53, 32, 48, 49, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67,
        66, 23, 50, 48, 50, 51, 45, 49, 50, 45, 50, 53, 32, 48, 49, 58, 48, 50, 58, 48, 51, 32, 85,
        84, 67, 74, 23, 50, 48, 50, 51, 45, 49, 50, 45, 50, 53, 32, 49, 48, 58, 48, 48, 58, 48, 48,
        32, 85, 84, 67, 82, 23, 50, 48, 50, 51, 45, 49, 50, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 90, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49, 58, 50,
        54, 58, 48, 57, 32, 85, 84, 67, 98, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32, 48, 49,
        58, 50, 54, 58, 49, 48, 32, 85, 84, 67, 160, 6, 91, 168, 6, 24,
    ];

    let want = || mt::principal::UserInfo {
        name: "u1".to_string(),
        hostname: "%".to_string(),
        auth_info: Default::default(),
        grants: UserGrantSet::new(
            vec![
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::TableById("default".to_string(), 1, 10),
                    make_bitflags!(UserPrivilegeType::{Select}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::DatabaseById("default".to_string(), 1),
                    make_bitflags!(UserPrivilegeType::{Create}),
                ),
            ],
            HashSet::new(),
        ),
        quota: Default::default(),
        option: Default::default(),
        history_auth_infos: vec![
            AuthInfo::create2(&None, &Some("1234".to_string()), false).unwrap(),
            AuthInfo::create2(&None, &Some("abcd".to_string()), false).unwrap(),
        ],
        password_fails: vec![
            Utc.with_ymd_and_hms(2023, 12, 25, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 12, 25, 1, 2, 3).unwrap(),
        ],
        password_update_on: Some(Utc.with_ymd_and_hms(2023, 12, 25, 10, 0, 0).unwrap()),
        lockout_time: Some(Utc.with_ymd_and_hms(2023, 12, 28, 12, 0, 9).unwrap()),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(1702603570, 0).unwrap(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), user_info_v91.as_slice(), 91, want())?;

    Ok(())
}
