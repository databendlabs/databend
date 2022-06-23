// Copyright 2022 Datafuse Labs.
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

use common_meta_types::AuthType;
use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;
use common_meta_types::UserOption;
use common_meta_types::UserOptionFlag;
use common_meta_types::UserPrivilegeType;

use crate::ast::write_comma_separated_list;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserStmt {
    pub if_not_exists: bool,
    pub user: UserIdentity,
    pub auth_option: AuthOption,
    pub role_options: Vec<RoleOption>,
}

impl Display for CreateUserStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE USER")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} IDENTIFIED", self.user)?;
        write!(f, " {}", self.auth_option)?;
        if !self.role_options.is_empty() {
            write!(f, " WITH")?;
            for role_option in &self.role_options {
                write!(f, " {role_option}")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthOption {
    pub auth_type: Option<AuthType>,
    pub password: Option<String>,
}

impl Display for AuthOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(auth_type) = &self.auth_type {
            write!(f, "WITH {} ", auth_type.to_str())?;
        }
        if let Some(password) = &self.password {
            write!(f, "BY '{password}'")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterUserStmt {
    // None means current user
    pub user: Option<UserIdentity>,
    // None means no change to make
    pub auth_option: Option<AuthOption>,
    pub role_options: Vec<RoleOption>,
}

impl Display for AlterUserStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER USER")?;
        if let Some(user) = &self.user {
            write!(f, " {user}")?;
        } else {
            write!(f, " USER()")?;
        }
        if let Some(auth_option) = &self.auth_option {
            write!(f, " IDENTIFIED {}", auth_option)?;
        }
        if !self.role_options.is_empty() {
            write!(f, " WITH")?;
            for with_option in &self.role_options {
                write!(f, " {with_option}")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    pub source: AccountMgrSource,
    pub principal: PrincipalIdentity,
}

impl Display for GrantStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRANT")?;
        write!(f, "{}", self.source)?;

        write!(f, " TO")?;
        write!(f, "{}", self.principal)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    pub source: AccountMgrSource,
    pub principal: PrincipalIdentity,
}

impl Display for RevokeStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REVOKE")?;
        write!(f, "{}", self.source)?;

        write!(f, " FROM")?;
        write!(f, "{}", self.principal)
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum AccountMgrLevel {
    Global,
    Database(Option<String>),
    Table(Option<String>, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RoleOption {
    TenantSetting,
    NoTenantSetting,
    ConfigReload,
    NoConfigReload,
}

impl RoleOption {
    pub fn apply(&self, option: &mut UserOption) {
        match self {
            Self::TenantSetting => {
                option.set_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::NoTenantSetting => {
                option.unset_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::ConfigReload => {
                option.set_option_flag(UserOptionFlag::ConfigReload);
            }
            Self::NoConfigReload => {
                option.unset_option_flag(UserOptionFlag::ConfigReload);
            }
        }
    }
}

impl Display for AccountMgrSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountMgrSource::Role { role } => write!(f, " ROLE {role}")?,
            AccountMgrSource::Privs { privileges, level } => {
                write!(f, " ")?;
                write_comma_separated_list(f, privileges.iter().map(|p| p.to_string()))?;
                write!(f, " ON")?;
                match level {
                    AccountMgrLevel::Global => write!(f, " *.*")?,
                    AccountMgrLevel::Database(database_name) => {
                        if let Some(database_name) = database_name {
                            write!(f, " {database_name}.*")?;
                        } else {
                            write!(f, " *")?;
                        }
                    }
                    AccountMgrLevel::Table(database_name, table_name) => {
                        if let Some(database_name) = database_name {
                            write!(f, " {database_name}.{table_name}")?;
                        } else {
                            write!(f, " {table_name}")?;
                        }
                    }
                }
            }
            AccountMgrSource::ALL { level, .. } => {
                write!(f, " ALL PRIVILEGES")?;
                write!(f, " ON")?;
                match level {
                    AccountMgrLevel::Global => write!(f, " *.*")?,
                    AccountMgrLevel::Database(database_name) => {
                        if let Some(database_name) = database_name {
                            write!(f, " {database_name}.*")?;
                        } else {
                            write!(f, " *")?;
                        }
                    }
                    AccountMgrLevel::Table(database_name, table_name) => {
                        if let Some(database_name) = database_name {
                            write!(f, " {database_name}.{table_name}")?;
                        } else {
                            write!(f, " {table_name}")?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for RoleOption {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RoleOption::TenantSetting => write!(f, "TENANTSETTING"),
            RoleOption::NoTenantSetting => write!(f, "NOTENANTSETTING"),
            RoleOption::ConfigReload => write!(f, "CONFIGRELOAD"),
            RoleOption::NoConfigReload => write!(f, "NOCONFIGRELOAD"),
        }
    }
}
