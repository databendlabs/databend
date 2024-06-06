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
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::make_bitflags;
use minitrace::func_name;

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
// The user_info_v69 bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v69_user() -> anyhow::Result<()> {
    let user_info_v69 = vec![
        10, 2, 117, 49, 18, 1, 37, 26, 8, 10, 0, 160, 6, 69, 168, 6, 24, 34, 70, 10, 31, 10, 21,
        58, 13, 10, 7, 100, 101, 102, 97, 117, 108, 116, 16, 1, 24, 10, 160, 6, 69, 168, 6, 24, 16,
        4, 160, 6, 69, 168, 6, 24, 10, 29, 10, 19, 50, 11, 10, 7, 100, 101, 102, 97, 117, 108, 116,
        16, 1, 160, 6, 69, 168, 6, 24, 16, 2, 160, 6, 69, 168, 6, 24, 160, 6, 69, 168, 6, 24, 42,
        6, 160, 6, 69, 168, 6, 24, 50, 6, 160, 6, 69, 168, 6, 24, 160, 6, 69, 168, 6, 24,
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
        history_auth_infos: vec![],
        password_fails: vec![],
        password_update_on: None,
        lockout_time: None,
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), user_info_v69.as_slice(), 69, want())?;

    Ok(())
}
