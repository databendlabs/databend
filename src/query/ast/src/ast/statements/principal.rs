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

use crate::ast::escape::escape_specified;
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
    #[drive(skip)]
    pub username: String,
    #[drive(skip)]
    pub hostname: String,
}

impl UserIdentity {
    const ESCAPE_CHARS: [u8; 2] = [b'\'', b'@'];
}

impl Display for UserIdentity {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "'{}'@'{}'",
            escape_specified(&self.username, &Self::ESCAPE_CHARS),
            escape_specified(&self.hostname, &Self::ESCAPE_CHARS),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PrincipalIdentity {
    User(UserIdentity),
    Role(#[drive(skip)] String),
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
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ShareGrantObjectName {
    // database name
    Database(Identifier),
    // database name, table name
    Table(Identifier, Identifier),
}

impl Display for ShareGrantObjectName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareGrantObjectName::Database(db) => {
                write!(f, "DATABASE {db}")
            }
            ShareGrantObjectName::Table(db, table) => {
                write!(f, "TABLE {db}.{table}")
            }
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CopyOptions {
    pub on_error: OnErrorMode,
    #[drive(skip)]
    pub size_limit: usize,
    #[drive(skip)]
    pub max_files: usize,
    #[drive(skip)]
    pub split_size: usize,
    #[drive(skip)]
    pub purge: bool,
    #[drive(skip)]
    pub disable_variant_check: bool,
    #[drive(skip)]
    pub return_failed_only: bool,
    #[drive(skip)]
    pub max_file_size: usize,
    #[drive(skip)]
    pub single: bool,
    #[drive(skip)]
    pub detailed_output: bool,
}

impl Display for CopyOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OnErrorMode {}", self.on_error)?;
        write!(f, "SizeLimit {}", self.size_limit)?;
        write!(f, "MaxFiles {}", self.max_files)?;
        write!(f, "SplitSize {}", self.split_size)?;
        write!(f, "Purge {}", self.purge)?;
        write!(f, "DisableVariantCheck {}", self.disable_variant_check)?;
        write!(f, "ReturnFailedOnly {}", self.return_failed_only)?;
        write!(f, "MaxFileSize {}", self.max_file_size)?;
        write!(f, "Single {}", self.single)?;
        write!(f, "DetailedOutput {}", self.detailed_output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum OnErrorMode {
    Continue,
    SkipFileNum(#[drive(skip)] u64),
    AbortNum(#[drive(skip)] u64),
}

impl Display for OnErrorMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OnErrorMode::Continue => {
                write!(f, "continue")
            }
            OnErrorMode::SkipFileNum(n) => {
                if *n <= 1 {
                    write!(f, "skipfile")
                } else {
                    write!(f, "skipfile_{}", n)
                }
            }
            OnErrorMode::AbortNum(n) => {
                if *n <= 1 {
                    write!(f, "abort")
                } else {
                    write!(f, "abort_{}", n)
                }
            }
        }
    }
}
