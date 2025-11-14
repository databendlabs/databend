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
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::make_bitflags;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v158_grant_object() -> anyhow::Result<()> {
    let role_info_v158 = vec![
        10, 9, 109, 97, 115, 107, 95, 114, 111, 108, 101, 18, 59, 10, 22, 10, 9, 10, 0, 160, 6,
        158, 1, 168, 6, 24, 16, 128, 128, 4, 160, 6, 158, 1, 168, 6, 24, 10, 26, 10, 11, 98, 2, 8,
        7, 160, 6, 158, 1, 168, 6, 24, 16, 128, 128, 128, 128, 1, 160, 6, 158, 1, 168, 6, 24, 160,
        6, 158, 1, 168, 6, 24, 26, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48,
        48, 58, 48, 48, 32, 85, 84, 67, 34, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48,
        58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 158, 1, 168, 6, 24,
    ];

    let want = || mt::principal::RoleInfo {
        name: "mask_role".to_string(),
        comment: None,
        grants: UserGrantSet::new(
            vec![
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{CreateDataMask}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::MaskingPolicy(7),
                    make_bitflags!(UserPrivilegeType::{ApplyMaskingPolicy}),
                ),
            ],
            HashSet::new(),
        ),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v158.as_slice(), 158, want())?;

    Ok(())
}

#[test]
fn test_decode_v158_masking_policy_ownership() -> anyhow::Result<()> {
    let ownership_info_v158 = vec![
        10, 10, 111, 119, 110, 101, 114, 95, 114, 111, 108, 101, 18, 11, 74, 2, 8, 42, 160, 6, 158,
        1, 168, 6, 24, 160, 6, 158, 1, 168, 6, 24,
    ];

    let want = || OwnershipInfo {
        role: "owner_role".to_string(),
        object: OwnershipObject::MaskingPolicy { policy_id: 42 },
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), ownership_info_v158.as_slice(), 158, want())?;

    Ok(())
}
