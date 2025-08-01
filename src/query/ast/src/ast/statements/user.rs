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

use crate::ast::write_comma_separated_list;
use crate::ast::AuthType;
use crate::ast::CreateOption;
use crate::ast::PrincipalIdentity;
use crate::ast::ShowOptions;
use crate::ast::UserIdentity;
use crate::ast::UserPrivilegeType;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateUserStmt {
    pub create_option: CreateOption,
    pub user: UserIdentity,
    pub auth_option: AuthOption,
    pub user_options: Vec<UserOptionItem>,
}

impl Display for CreateUserStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " USER")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} IDENTIFIED", self.user)?;
        write!(f, " {}", self.auth_option)?;
        if !self.user_options.is_empty() {
            write!(f, " WITH ")?;
            write_comma_separated_list(f, &self.user_options)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct AuthOption {
    pub auth_type: Option<AuthType>,
    pub password: Option<String>,
}

impl Display for AuthOption {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(auth_type) = &self.auth_type {
            write!(f, "WITH {auth_type} ")?;
        }
        if let Some(password) = &self.password {
            write!(f, "BY '{password}'")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterUserStmt {
    // None means current user
    pub user: Option<UserIdentity>,
    // None means no change to make
    pub auth_option: Option<AuthOption>,
    pub user_options: Vec<UserOptionItem>,
}

impl Display for AlterUserStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER USER")?;
        if let Some(user) = &self.user {
            write!(f, " {}", user)?;
        } else {
            write!(f, " USER()")?;
        }
        if let Some(auth_option) = &self.auth_option {
            write!(f, " IDENTIFIED {}", auth_option)?;
        }
        if !self.user_options.is_empty() {
            write!(f, " WITH ")?;
            write_comma_separated_list(f, &self.user_options)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct GrantStmt {
    pub source: AccountMgrSource,
    pub principal: PrincipalIdentity,
}

impl Display for GrantStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "GRANT")?;
        write!(f, "{}", self.source)?;

        write!(f, " TO")?;
        write!(f, "{}", self.principal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct RevokeStmt {
    pub source: AccountMgrSource,
    pub principal: PrincipalIdentity,
}

impl Display for RevokeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REVOKE")?;
        write!(f, "{}", self.source)?;

        write!(f, " FROM")?;
        write!(f, "{}", self.principal)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowGranteesOfRoleStmt {
    pub name: String,
    pub show_option: Option<ShowOptions>,
}

impl Display for ShowGranteesOfRoleStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW GRANTS OF ROLE {}", self.name)?;

        if let Some(show_option) = &self.show_option {
            write!(f, " {show_option}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowObjectPrivilegesStmt {
    pub object: GrantObjectName,
    pub show_option: Option<ShowOptions>,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum GrantObjectName {
    Database(String),
    Table(Option<String>, String),
    UDF(String),
    Stage(String),
    Warehouse(String),
    Connection(String),
    Sequence(String),
}

impl Display for GrantObjectName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            GrantObjectName::Database(database_name) => {
                write!(f, "DATABASE {database_name}")
            }
            GrantObjectName::Table(database_name, table_name) => {
                if let Some(database_name) = database_name {
                    write!(f, "TABLE {database_name}.{table_name}")
                } else {
                    write!(f, "TABLE {table_name}")
                }
            }
            GrantObjectName::UDF(udf) => write!(f, " UDF {udf}"),
            GrantObjectName::Stage(stage) => write!(f, " STAGE {stage}"),
            GrantObjectName::Warehouse(w) => write!(f, " WAREHOUSE {w}"),
            GrantObjectName::Connection(c) => write!(f, " CONNECTION {c}"),
            GrantObjectName::Sequence(s) => write!(f, " SEQUENCE {s}"),
        }
    }
}

impl Display for ShowObjectPrivilegesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW GRANTS ON {}", self.object)?;

        if let Some(show_option) = &self.show_option {
            write!(f, " {show_option}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AccountMgrSource {
    Role {
        role: String,
    },
    Privs {
        privileges: Vec<UserPrivilegeType>,
        level: AccountMgrLevel,
    },
    ALL {
        level: AccountMgrLevel,
    },
}

impl Display for AccountMgrSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AccountMgrSource::Role { role } => write!(f, " ROLE '{role}'")?,
            AccountMgrSource::Privs { privileges, level } => {
                write!(f, " ")?;
                write_comma_separated_list(f, privileges.iter().map(|p| p.to_string()))?;
                write!(f, " ON")?;
                write!(f, " {}", level)?;
            }
            AccountMgrSource::ALL { level, .. } => {
                write!(f, " ALL PRIVILEGES")?;
                write!(f, " ON")?;
                write!(f, " {}", level)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AccountMgrLevel {
    Global,
    Database(Option<String>),
    Table(Option<String>, String),
    UDF(String),
    Stage(String),
    Warehouse(String),
    Connection(String),
    Sequence(String),
}

impl Display for AccountMgrLevel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AccountMgrLevel::Global => write!(f, " *.*"),
            AccountMgrLevel::Database(database_name) => {
                if let Some(database_name) = database_name {
                    write!(f, " {database_name}.*")
                } else {
                    write!(f, " *")
                }
            }
            AccountMgrLevel::Table(database_name, table_name) => {
                if let Some(database_name) = database_name {
                    write!(f, " {database_name}.{table_name}")
                } else {
                    write!(f, " {table_name}")
                }
            }
            AccountMgrLevel::UDF(udf) => write!(f, " UDF {udf}"),
            AccountMgrLevel::Stage(stage) => write!(f, " STAGE {stage}"),
            AccountMgrLevel::Warehouse(w) => write!(f, " WAREHOUSE {w}"),
            AccountMgrLevel::Connection(c) => write!(f, " CONNECTION {c}"),
            AccountMgrLevel::Sequence(s) => write!(f, " SEQUENCE {s}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SecondaryRolesOption {
    None,
    All,
    SpecifyRole(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum UserOptionItem {
    TenantSetting(bool),
    DefaultRole(String),
    Disabled(bool),
    SetNetworkPolicy(String),
    UnsetNetworkPolicy,
    SetPasswordPolicy(String),
    UnsetPasswordPolicy,
    MustChangePassword(bool),
    SetWorkloadGroup(String),
    UnsetWorkloadGroup,
}

impl Display for UserOptionItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            UserOptionItem::TenantSetting(true) => write!(f, "TENANTSETTING"),
            UserOptionItem::TenantSetting(false) => write!(f, "NOTENANTSETTING"),
            UserOptionItem::DefaultRole(v) => write!(f, "DEFAULT_ROLE = '{}'", v),
            UserOptionItem::SetNetworkPolicy(v) => write!(f, "SET NETWORK POLICY = '{}'", v),
            UserOptionItem::UnsetNetworkPolicy => write!(f, "UNSET NETWORK POLICY"),
            UserOptionItem::SetPasswordPolicy(v) => write!(f, "SET PASSWORD POLICY = '{}'", v),
            UserOptionItem::SetWorkloadGroup(v) => write!(f, "SET WORKLOAD GROUP = '{}'", v),
            UserOptionItem::UnsetWorkloadGroup => write!(f, "UNSET WORKLOAD GROUP"),
            UserOptionItem::UnsetPasswordPolicy => write!(f, "UNSET PASSWORD POLICY"),
            UserOptionItem::Disabled(v) => write!(f, "DISABLED = {}", v),
            UserOptionItem::MustChangePassword(v) => write!(f, "MUST_CHANGE_PASSWORD = {}", v),
        }
    }
}
