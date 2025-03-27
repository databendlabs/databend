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

use std::fmt;
use std::fmt::Display;
use std::ops;

use enumflags2::bitflags;
use enumflags2::make_bitflags;
use enumflags2::BitFlags;

// Note:
// 1. If add new privilege type, need add forward test
// 2. Do not remove existing permission types. Otherwise, forward compatibility problems may occur
#[bitflags]
#[repr(u64)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum UserPrivilegeType {
    // UsagePrivilege is a synonym for “no privileges”, if object is udf/warehouse, means can use this udf/warehouse
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
    // Privilege to alter databases or tables.
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
    // Privilege to Drop role.
    DropRole = 1 << 14,
    // Privilege to Drop user.
    DropUser = 1 << 15,
    // Privilege to Create/Drop DataMask.
    CreateDataMask = 1 << 16,
    // Privilege to Own a databend object such as database/table.
    Ownership = 1 << 17,
    // Privilege to Read stage
    Read = 1 << 18,
    // Privilege to Write stage
    Write = 1 << 19,
    // Privilege to Create database
    CreateDatabase = 1 << 20,
    // Privilege to Create warehouse
    CreateWarehouse = 1 << 21,
    // Discard Privilege Type
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
        | DropUser
        | CreateRole
        | DropRole
        | Grant
        | CreateStage
        | Set
        | CreateDataMask
        | Ownership
        | Read
        | Write
        | CreateDatabase
        | CreateWarehouse
    }
);

impl Display for UserPrivilegeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
            UserPrivilegeType::DropUser => "DROP USER",
            UserPrivilegeType::CreateRole => "CREATE ROLE",
            UserPrivilegeType::DropRole => "DROP ROLE",
            UserPrivilegeType::CreateStage => "CREATE STAGE",
            UserPrivilegeType::Grant => "GRANT",
            UserPrivilegeType::Set => "SET",
            UserPrivilegeType::CreateDataMask => "CREATE DATAMASK",
            UserPrivilegeType::Ownership => "OWNERSHIP",
            UserPrivilegeType::Read => "Read",
            UserPrivilegeType::Write => "Write",
            UserPrivilegeType::CreateDatabase => "CREATE DATABASE",
            UserPrivilegeType::CreateWarehouse => "CREATE WAREHOUSE",
        })
    }
}

impl From<databend_common_ast::ast::UserPrivilegeType> for UserPrivilegeType {
    fn from(t: databend_common_ast::ast::UserPrivilegeType) -> Self {
        match t {
            databend_common_ast::ast::UserPrivilegeType::Usage => UserPrivilegeType::Usage,
            databend_common_ast::ast::UserPrivilegeType::Select => UserPrivilegeType::Select,
            databend_common_ast::ast::UserPrivilegeType::Insert => UserPrivilegeType::Insert,
            databend_common_ast::ast::UserPrivilegeType::Update => UserPrivilegeType::Update,
            databend_common_ast::ast::UserPrivilegeType::Delete => UserPrivilegeType::Delete,
            databend_common_ast::ast::UserPrivilegeType::Create => UserPrivilegeType::Create,
            databend_common_ast::ast::UserPrivilegeType::Drop => UserPrivilegeType::Drop,
            databend_common_ast::ast::UserPrivilegeType::Alter => UserPrivilegeType::Alter,
            databend_common_ast::ast::UserPrivilegeType::Super => UserPrivilegeType::Super,
            databend_common_ast::ast::UserPrivilegeType::CreateUser => {
                UserPrivilegeType::CreateUser
            }
            databend_common_ast::ast::UserPrivilegeType::CreateRole => {
                UserPrivilegeType::CreateRole
            }
            databend_common_ast::ast::UserPrivilegeType::Grant => UserPrivilegeType::Grant,
            databend_common_ast::ast::UserPrivilegeType::CreateStage => {
                UserPrivilegeType::CreateStage
            }
            databend_common_ast::ast::UserPrivilegeType::DropRole => UserPrivilegeType::DropRole,
            databend_common_ast::ast::UserPrivilegeType::DropUser => UserPrivilegeType::DropUser,
            databend_common_ast::ast::UserPrivilegeType::CreateDataMask => {
                UserPrivilegeType::CreateDataMask
            }
            databend_common_ast::ast::UserPrivilegeType::Ownership => UserPrivilegeType::Ownership,
            databend_common_ast::ast::UserPrivilegeType::Read => UserPrivilegeType::Read,
            databend_common_ast::ast::UserPrivilegeType::Write => UserPrivilegeType::Write,
            databend_common_ast::ast::UserPrivilegeType::CreateDatabase => {
                UserPrivilegeType::CreateDatabase
            }
            databend_common_ast::ast::UserPrivilegeType::CreateWarehouse => {
                UserPrivilegeType::CreateWarehouse
            }
            databend_common_ast::ast::UserPrivilegeType::Set => UserPrivilegeType::Set,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Default, Debug, Eq, PartialEq)]
pub struct UserPrivilegeSet {
    privileges: BitFlags<UserPrivilegeType>,
}

#[allow(clippy::len_without_is_empty)]
impl UserPrivilegeSet {
    pub fn empty() -> Self {
        UserPrivilegeSet {
            privileges: BitFlags::empty(),
        }
    }

    #[inline(always)]
    pub fn len(self) -> usize {
        self.privileges.len()
    }

    pub fn iter(self) -> impl Iterator<Item = UserPrivilegeType> {
        BitFlags::from(self).iter()
    }

    /// The all privileges which available to the global grant object. It contains ALL the privileges
    /// on databases and tables, and has some Global only privileges.
    pub fn available_privileges_on_global() -> Self {
        let database_privs = Self::available_privileges_on_database(false);
        let stage_privs_without_ownership = Self::available_privileges_on_stage(false);
        let udf_privs_without_ownership = Self::available_privileges_on_udf(false);
        let wh_privs_without_ownership = Self::available_privileges_on_warehouse(false);
        let privs = make_bitflags!(UserPrivilegeType::{ Usage | Super | CreateUser | DropUser | CreateRole | DropRole | CreateDatabase | Grant | CreateDataMask | CreateWarehouse });
        (database_privs.privileges
            | privs
            | stage_privs_without_ownership.privileges
            | wh_privs_without_ownership.privileges
            | udf_privs_without_ownership.privileges)
            .into()
    }

    /// The available privileges on database object contains ALL the available privileges to a table.
    /// Currently the privileges available to a database and a table are the same, it might becomes
    /// some differences in the future.
    pub fn available_privileges_on_database(available_ownership: bool) -> Self {
        UserPrivilegeSet::available_privileges_on_table(available_ownership)
    }

    /// The all privileges global which available to the table object
    pub fn available_privileges_on_table(available_ownership: bool) -> Self {
        let tab_privs = make_bitflags!(UserPrivilegeType::{ Create | Update | Select | Insert | Delete | Drop | Alter | Grant });
        if available_ownership {
            (tab_privs | make_bitflags!(UserPrivilegeType::{  Ownership })).into()
        } else {
            tab_privs.into()
        }
    }

    pub fn available_privileges_on_stage(available_ownership: bool) -> Self {
        if available_ownership {
            make_bitflags!(UserPrivilegeType::{  Read | Write | Ownership }).into()
        } else {
            make_bitflags!(UserPrivilegeType::{  Read | Write }).into()
        }
    }

    pub fn available_privileges_on_warehouse(available_ownership: bool) -> Self {
        if available_ownership {
            make_bitflags!(UserPrivilegeType::{  Usage | Ownership }).into()
        } else {
            make_bitflags!(UserPrivilegeType::{  Usage }).into()
        }
    }

    pub fn available_privileges_on_udf(available_ownership: bool) -> Self {
        if available_ownership {
            make_bitflags!(UserPrivilegeType::{ Usage | Ownership }).into()
        } else {
            make_bitflags!(UserPrivilegeType::{ Usage }).into()
        }
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

impl Display for UserPrivilegeSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl From<UserPrivilegeType> for UserPrivilegeSet {
    fn from(value: UserPrivilegeType) -> Self {
        let mut privileges = UserPrivilegeSet::empty();
        privileges.set_privilege(value);
        privileges
    }
}
