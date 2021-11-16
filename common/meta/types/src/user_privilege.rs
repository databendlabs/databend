// Copyright 2021 Datafuse Labs.
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

use std::ops;

use enumflags2::bitflags;
use enumflags2::make_bitflags;
use enumflags2::BitFlags;

#[bitflags]
#[repr(u64)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum UserPrivilegeType {
    // UsagePrivilege is a synonym for “no privileges”
    Usage = 1 << 0,
    // Privilege to create databases and tables.
    Create = 1 << 1,
    // Privilege to select rows from tables in a database.
    Select = 1 << 2,
    // Privilege to insert into tables in a database.
    Insert = 1 << 3,
    // Privilege to SET variables.
    Set = 1 << 4,
}

const ALL_PRIVILEGES: BitFlags<UserPrivilegeType> = make_bitflags!(
    UserPrivilegeType::{Create
        | Select
        | Insert
        | Set}
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct UserPrivilege {
    privileges: BitFlags<UserPrivilegeType>,
}

impl UserPrivilege {
    pub fn empty() -> Self {
        UserPrivilege {
            privileges: BitFlags::empty(),
        }
    }

    pub fn set_privilege(&mut self, privilege: UserPrivilegeType) {
        self.privileges |= privilege;
    }

    pub fn has_privilege(&self, privilege: UserPrivilegeType) -> bool {
        self.privileges.contains(privilege)
    }

    pub fn set_all_privileges(&mut self) {
        self.privileges |= ALL_PRIVILEGES;
    }
}

impl ops::BitOr for UserPrivilege {
    type Output = Self;
    #[inline(always)]
    fn bitor(self, other: Self) -> Self {
        Self {
            privileges: self.privileges | other.privileges,
        }
    }
}

impl ops::BitOrAssign for UserPrivilege {
    #[inline(always)]
    fn bitor_assign(&mut self, other: Self) {
        self.privileges |= other.privileges
    }
}
