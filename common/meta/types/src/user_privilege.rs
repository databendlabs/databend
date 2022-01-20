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

use std::fmt;
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
    // Privilege to select rows from tables in a database.
    Select = 1 << 2,
    // Privilege to insert into tables in a database.
    Insert = 1 << 3,
    // Privilege to update rows in a table
    Update = 1 << 5,
    // Privilege to delete rows in a table
    Delete = 1 << 6,
    // Privilege to create databases or tables.
    Create = 1 << 1,
    // Privilege to drop databases or tables.
    Drop = 1 << 7,
    // Privilege to delete rows in a table
    Alter = 1 << 8,
    // Privilege to Kill query, Set global configs, etc.
    Super = 1 << 9,
    // Privilege to Create User.
    CreateUser = 1 << 10,
    // Privilege to Create Role.
    CreateRole = 1 << 11,
    // Privilege to Grant/Revoke privileges to users or roles
    Grant = 1 << 12,
    // Privilege to Create Stage.
    CreateStage = 1 << 13,
    // TODO: remove this later
    Set = 1 << 4,
}

const ALL_PRIVILEGES: BitFlags<UserPrivilegeType> = make_bitflags!(
    UserPrivilegeType::{
        Create
        | Select
        | Insert
        | Update
        | Delete
        | Drop
        | Alter
        | Super
        | CreateUser
        | CreateRole
        | Grant
        | Set
    }
);

impl std::fmt::Display for UserPrivilegeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", match self {
            UserPrivilegeType::Usage => "USAGE",
            UserPrivilegeType::Create => "CREATE",
            UserPrivilegeType::Update => "UPDATE",
            UserPrivilegeType::Select => "SELECT",
            UserPrivilegeType::Insert => "INSERT",
            UserPrivilegeType::Delete => "DELETE",
            UserPrivilegeType::Drop => "DROP",
            UserPrivilegeType::Alter => "ALTER",
            UserPrivilegeType::Super => "SUPER",
            UserPrivilegeType::CreateUser => "CREATE USER",
            UserPrivilegeType::CreateRole => "CREATE ROLE",
            UserPrivilegeType::CreateStage => "CREATE STAGE",
            UserPrivilegeType::Grant => "GRANT",
            UserPrivilegeType::Set => "SET",
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Default, Debug, Eq, PartialEq)]
pub struct UserPrivilegeSet {
    privileges: BitFlags<UserPrivilegeType>,
}

impl UserPrivilegeSet {
    pub fn empty() -> Self {
        UserPrivilegeSet {
            privileges: BitFlags::empty(),
        }
    }

    pub fn iter(self) -> impl Iterator<Item = UserPrivilegeType> {
        BitFlags::from(self).iter()
    }

    /// The all privileges which available to the global grant object. It contains ALL the privileges
    /// on databases and tables, and has some Global only privileges.
    pub fn available_privileges_on_global() -> Self {
        let database_privs = Self::available_privileges_on_database();
        let privs =
            make_bitflags!(UserPrivilegeType::{ Usage | Super | CreateUser | CreateRole | Grant });
        (database_privs.privileges | privs).into()
    }

    /// The availabe privileges on database object contains ALL the available privileges to a table.
    /// Currently the privileges available to a database and a table are the same, it might becomes
    /// some differences in the future.
    pub fn available_privileges_on_database() -> Self {
        UserPrivilegeSet::available_privileges_on_table()
    }

    /// The all privileges global which available to the table object
    pub fn available_privileges_on_table() -> Self {
        make_bitflags!(UserPrivilegeType::{ Create | Update | Select | Insert | Delete | Drop | Alter | Grant }).into()
    }

    // TODO: remove this, as ALL has different meanings on different objects
    pub fn all_privileges() -> Self {
        ALL_PRIVILEGES.into()
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

    pub fn is_all_privileges(&self) -> bool {
        self.privileges == ALL_PRIVILEGES
    }
}

impl std::fmt::Display for UserPrivilegeSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}",
            self.privileges
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl ops::BitOr for UserPrivilegeSet {
    type Output = Self;
    #[inline(always)]
    fn bitor(self, other: Self) -> Self {
        Self {
            privileges: self.privileges | other.privileges,
        }
    }
}

impl ops::BitOrAssign for UserPrivilegeSet {
    #[inline(always)]
    fn bitor_assign(&mut self, other: Self) {
        self.privileges |= other.privileges
    }
}

impl From<UserPrivilegeSet> for BitFlags<UserPrivilegeType> {
    fn from(privilege: UserPrivilegeSet) -> BitFlags<UserPrivilegeType> {
        privilege.privileges
    }
}

impl From<BitFlags<UserPrivilegeType>> for UserPrivilegeSet {
    fn from(privileges: BitFlags<UserPrivilegeType>) -> UserPrivilegeSet {
        UserPrivilegeSet { privileges }
    }
}

impl From<Vec<UserPrivilegeType>> for UserPrivilegeSet {
    fn from(privileges: Vec<UserPrivilegeType>) -> UserPrivilegeSet {
        let mut result = UserPrivilegeSet::empty();
        for privilege in privileges {
            result.set_privilege(privilege)
        }
        result
    }
}
