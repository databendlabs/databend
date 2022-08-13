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

use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use itertools::Itertools;

use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateShareStmt<'a> {
    pub if_not_exists: bool,
    pub share: Identifier<'a>,
    pub comment: Option<String>,
}

impl Display for CreateShareStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropShareStmt<'a> {
    pub if_exists: bool,
    pub share: Identifier<'a>,
}

impl Display for DropShareStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.share)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantShareObjectStmt<'a> {
    pub share: Identifier<'a>,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for GrantShareObjectStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GRANT {} ON {} TO SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeShareObjectStmt<'a> {
    pub share: Identifier<'a>,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for RevokeShareObjectStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "REVOKE {} ON {} FROM SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareTenantsStmt<'a> {
    pub share: Identifier<'a>,
    pub if_exists: bool,
    pub tenants: Vec<Identifier<'a>>,
    pub is_add: bool,
}

impl Display for AlterShareTenantsStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
