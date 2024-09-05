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
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::share::ShareGrantObjectPrivilege;
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
fn test_decode_v78_grant_entry() -> anyhow::Result<()> {
    let grant_entry_v78 = vec![
        10, 8, 10, 0, 160, 6, 78, 168, 6, 24, 16, 254, 255, 55, 160, 6, 78, 168, 6, 24,
    ];

    let want = || {
        mt::principal::GrantEntry::new(
            mt::principal::GrantObject::Global,
            make_bitflags!(UserPrivilegeType::{Create | Select | Insert | Update | Delete | Drop | Alter | Super | CreateUser | DropUser | CreateRole | DropRole | Grant | CreateStage | Set | CreateDataMask | Read | Write }),
        )
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), grant_entry_v78.as_slice(), 78, want())?;

    Ok(())
}

#[test]
fn test_decode_v78_share_grant_entry() -> anyhow::Result<()> {
    let share_grant_entry_v78 = vec![
        10, 8, 16, 19, 160, 6, 78, 168, 6, 24, 16, 4, 26, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 160, 6, 78, 168, 6, 24,
    ];

    let want = || mt::share::ShareGrantEntry {
        object: mt::share::ShareGrantObject::Table(19),
        privileges: make_bitflags!(ShareGrantObjectPrivilege::{Select}),
        grant_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        update_on: None,
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), share_grant_entry_v78.as_slice(), 78, want())?;

    Ok(())
}
