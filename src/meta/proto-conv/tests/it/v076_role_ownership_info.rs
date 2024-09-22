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
fn test_decode_v76_role() -> anyhow::Result<()> {
    let role_info_v76 = vec![
        10, 2, 114, 49, 18, 212, 1, 10, 31, 10, 21, 58, 13, 10, 7, 100, 101, 102, 97, 117, 108,
        116, 16, 1, 24, 10, 160, 6, 76, 168, 6, 24, 16, 4, 160, 6, 76, 168, 6, 24, 10, 29, 10, 19,
        50, 11, 10, 7, 100, 101, 102, 97, 117, 108, 116, 16, 1, 160, 6, 76, 168, 6, 24, 16, 2, 160,
        6, 76, 168, 6, 24, 10, 31, 10, 21, 18, 13, 10, 7, 100, 101, 102, 97, 117, 108, 116, 18, 2,
        100, 98, 160, 6, 76, 168, 6, 24, 16, 2, 160, 6, 76, 168, 6, 24, 10, 35, 10, 25, 26, 17, 10,
        7, 100, 101, 102, 97, 117, 108, 116, 18, 2, 100, 98, 26, 2, 116, 98, 160, 6, 76, 168, 6,
        24, 16, 2, 160, 6, 76, 168, 6, 24, 10, 22, 10, 12, 34, 4, 10, 2, 102, 49, 160, 6, 76, 168,
        6, 24, 16, 1, 160, 6, 76, 168, 6, 24, 10, 24, 10, 12, 42, 4, 10, 2, 115, 49, 160, 6, 76,
        168, 6, 24, 16, 128, 128, 32, 160, 6, 76, 168, 6, 24, 10, 20, 10, 8, 10, 0, 160, 6, 76,
        168, 6, 24, 16, 254, 255, 63, 160, 6, 76, 168, 6, 24, 160, 6, 76, 168, 6, 24, 160, 6, 76,
        168, 6, 24,
    ];

    let want = || mt::principal::RoleInfo {
        name: "r1".to_string(),
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
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Database("default".to_string(), "db".to_string()),
                    make_bitflags!(UserPrivilegeType::{Create}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Table(
                        "default".to_string(),
                        "db".to_string(),
                        "tb".to_string(),
                    ),
                    make_bitflags!(UserPrivilegeType::{Create}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::UDF("f1".to_string()),
                    make_bitflags!(UserPrivilegeType::{Usage}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Stage("s1".to_string()),
                    make_bitflags!(UserPrivilegeType::{Write}),
                ),
                mt::principal::GrantEntry::new(
                    mt::principal::GrantObject::Global,
                    make_bitflags!(UserPrivilegeType::{Create | Select | Insert | Update | Delete | Drop | Alter | Super | CreateUser | DropUser | CreateRole | DropRole | Grant | CreateStage | Set | CreateDataMask | Ownership | Read | Write}),
                ),
            ],
            HashSet::new(),
        ),
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), role_info_v76.as_slice(), 76, want())?;

    Ok(())
}

#[test]
fn test_decode_v76_ownership() -> anyhow::Result<()> {
    let ownership_info_v76 = vec![
        10, 2, 114, 49, 18, 19, 10, 11, 10, 7, 100, 101, 102, 97, 117, 108, 116, 16, 1, 160, 6, 76,
        168, 6, 24, 160, 6, 76, 168, 6, 24,
    ];

    let want = || mt::principal::OwnershipInfo {
        role: "r1".to_string(),
        object: OwnershipObject::Database {
            catalog_name: "default".to_string(),
            db_id: 1,
        },
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), ownership_info_v76.as_slice(), 76, want())?;

    Ok(())
}
