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

use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
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
// The message bytes are built from the output of `test_build_pb_buf()`
#[test]
fn test_decode_v102_user_must_change_password() -> anyhow::Result<()> {
    // user info with password policy
    let bytes: Vec<u8> = vec![
        10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 1, 37, 26, 27, 18, 19, 10, 13, 116,
        101, 115, 116, 95, 112, 97, 115, 115, 119, 111, 114, 100, 16, 1, 24, 1, 160, 6, 101, 168,
        6, 24, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 101, 168, 6, 24, 16, 2, 160, 6, 101, 168, 6,
        24, 160, 6, 101, 168, 6, 24, 42, 15, 8, 10, 16, 128, 80, 24, 128, 160, 1, 160, 6, 101, 168,
        6, 24, 50, 48, 8, 1, 18, 5, 114, 111, 108, 101, 49, 26, 8, 109, 121, 112, 111, 108, 105,
        99, 121, 34, 19, 116, 101, 115, 116, 112, 97, 115, 115, 119, 111, 114, 100, 112, 111, 108,
        105, 99, 121, 49, 48, 1, 160, 6, 101, 168, 6, 24, 90, 23, 49, 57, 55, 48, 45, 48, 49, 45,
        48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 98, 23, 49, 57, 55, 48, 45, 48,
        49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 102, 168, 6,
        24,
    ];

    let want = || databend_common_meta_app::principal::UserInfo {
        name: "test_user".to_string(),
        hostname: "%".to_string(),
        auth_info: databend_common_meta_app::principal::AuthInfo::Password {
            hash_value: [
                116, 101, 115, 116, 95, 112, 97, 115, 115, 119, 111, 114, 100,
            ]
            .to_vec(),
            hash_method: databend_common_meta_app::principal::PasswordHashMethod::DoubleSha1,
            need_change: true,
        },
        grants: databend_common_meta_app::principal::UserGrantSet::new(
            vec![databend_common_meta_app::principal::GrantEntry::new(
                databend_common_meta_app::principal::GrantObject::Global,
                make_bitflags!(UserPrivilegeType::{Create}),
            )],
            HashSet::new(),
        ),
        quota: databend_common_meta_app::principal::UserQuota {
            max_cpu: 10,
            max_memory_in_bytes: 10240,
            max_storage_in_bytes: 20480,
        },
        option: databend_common_meta_app::principal::UserOption::default()
            .with_set_flag(databend_common_meta_app::principal::UserOptionFlag::TenantSetting)
            .with_default_role(Some("role1".into()))
            .with_network_policy(Some("mypolicy".to_string()))
            .with_password_policy(Some("testpasswordpolicy1".to_string()))
            .with_must_change_password(Some(true)),
        history_auth_infos: vec![],
        password_fails: vec![],
        password_update_on: None,
        lockout_time: None,
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 102, want())
}
