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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;

use crate::ast::write_comma_separated_string_map;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::ShareGrantObjectName;
use crate::ast::ShareGrantObjectPrivilege;
use crate::ast::UriLocation;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateShareEndpointStmt {
    pub create_option: CreateOption,
    pub endpoint: Identifier,
    pub url: UriLocation,
    pub credential_options: BTreeMap<String, String>,
    pub args: BTreeMap<String, String>,
    pub comment: Option<String>,
}

impl Display for CreateShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "SHARE ENDPOINT ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.endpoint)?;
        write!(f, " URL={}", self.url)?;
        if !self.credential_options.is_empty() {
            write!(f, " CREDENTIAL=(")?;
            write_comma_separated_string_map(f, &self.credential_options)?;
            write!(f, ")")?;
        }
        if !self.args.is_empty() {
            write!(f, " ARGS=(")?;
            write_comma_separated_string_map(f, &self.args)?;
            write!(f, ")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{comment}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateShareStmt {
    pub if_not_exists: bool,
    pub share: Identifier,
    pub comment: Option<String>,
}

impl Display for CreateShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE SHARE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.share)?;
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{comment}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropShareStmt {
    pub if_exists: bool,
    pub share: Identifier,
}

impl Display for DropShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.share)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct GrantShareObjectStmt {
    pub share: Identifier,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for GrantShareObjectStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GRANT {} ON {} TO SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct RevokeShareObjectStmt {
    pub share: Identifier,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for RevokeShareObjectStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "REVOKE {} ON {} FROM SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterShareTenantsStmt {
    pub share: Identifier,
    pub if_exists: bool,
    pub tenants: Vec<Identifier>,
    pub is_add: bool,
}

impl Display for AlterShareTenantsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.share)?;
        if self.is_add {
            write!(f, " ADD TENANTS = ")?;
        } else {
            write!(f, " REMOVE TENANTS = ")?;
        }
        write!(
            f,
            "{}",
            self.tenants.iter().map(|v| v.to_string()).join(",")
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescShareStmt {
    pub share: Identifier,
}

impl Display for DescShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESC SHARE {}", self.share)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowSharesStmt {}

impl Display for ShowSharesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW SHARES")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowShareEndpointStmt {}

impl Display for ShowShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW SHARE ENDPOINT")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropShareEndpointStmt {
    pub if_exists: bool,
    pub endpoint: Identifier,
}

impl Display for DropShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SHARE ENDPOINT ")?;

        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }

        write!(f, "{}", self.endpoint)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowObjectGrantPrivilegesStmt {
    pub object: ShareGrantObjectName,
}

impl Display for ShowObjectGrantPrivilegesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW GRANTS ON {}", self.object)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowGrantsOfShareStmt {
    pub share_name: String,
}

impl Display for ShowGrantsOfShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW GRANTS OF SHARE {}", self.share_name)?;

        Ok(())
    }
}
