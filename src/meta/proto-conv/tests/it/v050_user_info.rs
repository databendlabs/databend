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
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v50_user_info() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 1, 37, 26, 25, 18, 17, 10, 13, 116,
        101, 115, 116, 95, 112, 97, 115, 115, 119, 111, 114, 100, 16, 1, 160, 6, 49, 168, 6, 24,
        34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 49, 168, 6, 24, 16, 2, 160, 6, 49, 168, 6, 24, 160,
        6, 49, 168, 6, 24, 42, 15, 8, 10, 16, 128, 80, 24, 128, 160, 1, 160, 6, 49, 168, 6, 24, 50,
        25, 8, 1, 18, 5, 114, 111, 108, 101, 49, 26, 8, 109, 121, 112, 111, 108, 105, 99, 121, 160,
        6, 49, 168, 6, 24, 160, 6, 50, 168, 6, 24,
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
            need_change: false,
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
            .with_network_policy(Some("mypolicy".to_string())),
        history_auth_infos: vec![],
        password_fails: vec![],
        password_update_on: None,
        lockout_time: None,
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 50, want())
}
