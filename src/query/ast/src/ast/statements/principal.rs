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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::quote::QuotedString;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShareNameIdent {
    pub tenant: Identifier,
    pub share: Identifier,
}

impl Display for ShareNameIdent {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.tenant, self.share)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct UserIdentity {
    pub username: String,
    pub hostname: String,
}

impl Display for UserIdentity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}@{}",
            QuotedString(&self.username, '\''),
            QuotedString(&self.hostname, '\''),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PrincipalIdentity {
    User(UserIdentity),
    Role(String),
}

impl Display for PrincipalIdentity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PrincipalIdentity::User(u) => write!(f, " USER {u}"),
            PrincipalIdentity::Role(r) => write!(f, " ROLE '{r}'"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum CreateOption {
    Create,
    CreateIfNotExists,
    CreateOrReplace,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AuthType {
    NoPassword,
    Sha256Password,
    DoubleSha1Password,
    JWT,
}

impl Display for AuthType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            AuthType::NoPassword => "no_password",
            AuthType::Sha256Password => "sha256_password",
            AuthType::DoubleSha1Password => "double_sha1_password",
            AuthType::JWT => "jwt",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum CatalogType {
    Default,
    Hive,
    Iceberg,
}

impl Display for CatalogType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CatalogType::Default => write!(f, "DEFAULT"),
            CatalogType::Hive => write!(f, "HIVE"),
            CatalogType::Iceberg => write!(f, "ICEBERG"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum UserPrivilegeType {
    // UsagePrivilege is a synonym for “no privileges”, if object is udf, means can use this udf
    Usage,
    // Privilege to select rows from tables in a database.
    Select,
    // Privilege to insert into tables in a database.
    Insert,
    // Privilege to update rows in a table
    Update,
    // Privilege to delete rows in a table
    Delete,
    // Privilege to create databases or tables.
    Create,
    // Privilege to drop databases or tables.
    Drop,
    // Privilege to alter databases or tables.
    Alter,
    // Privilege to Kill query, Set global configs, etc.
    Super,
    // Privilege to Create User.
    CreateUser,
    // Privilege to Create Role.
    CreateRole,
    // Privilege to Grant/Revoke privileges to users or roles
    Grant,
    // Privilege to Create Stage.
    CreateStage,
    // Privilege to Drop role.
    DropRole,
    // Privilege to Drop user.
    DropUser,
    // Privilege to Create/Drop DataMask.
    CreateDataMask,
    // Privilege to Own a databend object such as database/table.
    Ownership,
    // Privilege to Read stage
    Read,
    // Privilege to Write stage
    Write,
    // Privilege to Create database
    CreateDatabase,
    // Privilege to Create warehouse
    CreateWarehouse,
    // Discard Privilege Type
    Set,
}

impl Display for UserPrivilegeType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ShareGrantObjectPrivilege {
    // For DATABASE or SCHEMA
    Usage,
    // For DATABASE
    ReferenceUsage,
    // For TABLE or VIEW
    Select,
}

impl Display for ShareGrantObjectPrivilege {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            ShareGrantObjectPrivilege::Usage => write!(f, "USAGE"),
            ShareGrantObjectPrivilege::ReferenceUsage => write!(f, "REFERENCE_USAGE"),
            ShareGrantObjectPrivilege::Select => write!(f, "SELECT"),
        }
    }
}
