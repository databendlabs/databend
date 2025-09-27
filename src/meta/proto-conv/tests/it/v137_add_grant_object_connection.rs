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
fn test_decode_v137_grant_object() -> anyhow::Result<()> {
    let role_info_v137 = vec![
        10, 2, 114, 49, 18, 86, 10, 23, 10, 9, 10, 0, 160, 6, 137, 1, 168, 6, 24, 16, 128, 128,
        128, 2, 160, 6, 137, 1, 168, 6, 24, 10, 27, 10, 13, 74, 4, 10, 2, 99, 49, 160, 6, 137, 1,
        168, 6, 24, 16, 128, 128, 128, 4, 160, 6, 137, 1, 168, 6, 24, 10, 23, 10, 9, 10, 0, 160, 6,
        137, 1, 168, 6, 24, 16, 254, 255, 191, 7, 160, 6, 137, 1, 168, 6, 24, 160, 6, 137, 1, 168,
        6, 24, 26, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48,
        32, 85, 84, 67, 34, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
        48, 48, 32, 85, 84, 67, 160, 6, 137, 1, 168, 6, 24,
    ];
    let want = || mt::principal::RoleInfo {
        name: "r1".to_string(),
        comment: None,
        grants: UserGrantSet::new(
            vec![
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{CreateConnection}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Connection("c1".to_string()),
                    make_bitflags!(UserPrivilegeType::{AccessConnection}),
                ),
                // test new global privilege CreateConneciton, AccessConnection
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{Create | Select | Insert | Update | Delete | Drop | Alter | Super | CreateUser | DropUser | CreateRole | DropRole | Grant | CreateStage | Set | CreateDataMask | Ownership | Read | Write | CreateWarehouse | CreateConnection | AccessConnection }),
                ),
            ],
            HashSet::new(),
        ),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v137.as_slice(), 137, want())?;

    Ok(())
}

#[test]
fn test_decode_v137_ownership() -> anyhow::Result<()> {
    let ownership_info_v137 = vec![
        10, 2, 114, 49, 18, 13, 50, 4, 10, 2, 99, 49, 160, 6, 137, 1, 168, 6, 24, 160, 6, 137, 1,
        168, 6, 24,
    ];

    let want = || mt::principal::OwnershipInfo {
        role: "r1".to_string(),
        object: OwnershipObject::Connection {
            name: "c1".to_string(),
        },
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), ownership_info_v137.as_slice(), 137, want())?;

    Ok(())
}
