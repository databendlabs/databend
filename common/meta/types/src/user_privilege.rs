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

use std::collections::HashSet;
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
            UserPrivilegeType::Grant => "GRANT",
            UserPrivilegeType::Set => "SET",
        })
    }
}

/// Each privilege has a set of contexts, which limits the grant objects which the privilege can
/// take effects on, for example, we can not grant a SUPER privilege (which is Global level)
/// to a table (in a table level).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum UserPrivilegeContext {
    /// the SystemAddmin context includes the system admin privileges, like SUPER, CREATE USER,
    /// CREATE ROLE, GRANT, etc
    SystemAdmin,
    /// the Tables context includes most of the DML and DDL privileges, like INSERT, UPDATE,
    /// ALTER, etc.
    Tables,
    /// the Database context includes CREATE/DROP DATABASE
    Databases,
}

impl UserPrivilegeType {
    // The system admin privileges only takes effect on the global grant object
    pub fn global_only(self) -> bool {
        self.contexts().contains(&UserPrivilegeContext::SystemAdmin)
    }

    // TODO: display the Context column in the SHOW PRIVILEGE statement
    pub fn contexts(self) -> HashSet<UserPrivilegeContext> {
        use UserPrivilegeType::*;
        match self {
            Usage | Super | CreateUser | CreateRole | Grant => {
                HashSet::from([UserPrivilegeContext::SystemAdmin])
            }
            Create => HashSet::from([
                UserPrivilegeContext::Tables,
                UserPrivilegeContext::Databases,
            ]),
            _ => HashSet::from([UserPrivilegeContext::Tables]),
        }
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
