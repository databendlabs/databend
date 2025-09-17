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
use databend_common_meta_app::principal::OwnershipObject;
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
fn test_decode_v147_grant_object() -> anyhow::Result<()> {
    let role_info_v147 = vec![
        10, 2, 114, 49, 18, 84, 10, 23, 10, 9, 10, 0, 160, 6, 147, 1, 168, 6, 24, 16, 128, 128,
        128, 32, 160, 6, 147, 1, 168, 6, 24, 10, 25, 10, 11, 90, 2, 8, 1, 160, 6, 147, 1, 168, 6,
        24, 16, 128, 128, 128, 64, 160, 6, 147, 1, 168, 6, 24, 10, 23, 10, 9, 10, 0, 160, 6, 147,
        1, 168, 6, 24, 16, 128, 128, 136, 96, 160, 6, 147, 1, 168, 6, 24, 160, 6, 147, 1, 168, 6,
        24, 26, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32,
        85, 84, 67, 34, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48,
        48, 32, 85, 84, 67, 160, 6, 147, 1, 168, 6, 24,
    ];

    let want = || mt::principal::RoleInfo {
        name: "r1".to_string(),
        grants: UserGrantSet::new(
            vec![
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{CreateProcedure}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Procedure(1),
                    make_bitflags!(UserPrivilegeType::{AccessProcedure}),
                ),
                // test new global privilege CreateSequence, AccessConnection
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{Ownership | CreateProcedure | AccessProcedure }),
                ),
            ],
            HashSet::new(),
        ),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v147.as_slice(), 147, want())?;

    Ok(())
}

#[test]
fn test_decode_v147_ownership() -> anyhow::Result<()> {
    let ownership_info_v147 = vec![
        10, 2, 114, 49, 18, 11, 66, 2, 8, 2, 160, 6, 147, 1, 168, 6, 24, 160, 6, 147, 1, 168, 6, 24,
    ];

    let want = || mt::principal::OwnershipInfo {
        role: "r1".to_string(),
        object: OwnershipObject::Procedure { p_id: 2 },
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), ownership_info_v147.as_slice(), 147, want())?;

    Ok(())
}
