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

use databend_common_ast_visit_derive::Walk;
use databend_common_ast_visit_derive::WalkMut;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::ShareGrantObjectPrivilege;
use crate::ast::ShowOptions;
use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct ShareRef {
    pub tenant: Option<Identifier>,
    pub share: Identifier,
}

impl Display for ShareRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(tenant) = &self.tenant {
            write!(f, "{tenant}.")?;
        }
        write!(f, "{}", self.share)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct CreateShareStmt {
    pub create_option: CreateOption,
    pub name: Identifier,
    pub comment: Option<String>,
}

impl Display for CreateShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " SHARE")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct DropShareStmt {
    pub name: Identifier,
}

impl Display for DropShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SHARE {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct AlterShareStmt {
    pub if_exists: bool,
    pub name: Identifier,
    pub action: AlterShareAction,
}

impl Display for AlterShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} {}", self.name, self.action)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub enum AlterShareAction {
    AddAccounts {
        accounts: Vec<Identifier>,
    },
    RemoveAccounts {
        accounts: Vec<Identifier>,
    },
    Set {
        accounts: Option<Vec<Identifier>>,
        comment: Option<String>,
    },
}

impl Display for AlterShareAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterShareAction::AddAccounts { accounts } => {
                write!(f, "ADD ACCOUNTS = ")?;
                write_comma_separated_list(f, accounts)
            }
            AlterShareAction::RemoveAccounts { accounts } => {
                write!(f, "REMOVE ACCOUNTS = ")?;
                write_comma_separated_list(f, accounts)
            }
            AlterShareAction::Set { accounts, comment } => {
                write!(f, "SET")?;
                if let Some(accounts) = accounts {
                    write!(f, " ACCOUNTS = ")?;
                    write_comma_separated_list(f, accounts)?;
                }
                if let Some(comment) = comment {
                    write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub enum ShareGrantObjectName {
    Database(Identifier),
    Table(Option<Identifier>, Identifier),
}

impl Display for ShareGrantObjectName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareGrantObjectName::Database(database) => write!(f, "DATABASE {database}"),
            ShareGrantObjectName::Table(database, table) => {
                write!(f, "TABLE ")?;
                if let Some(database) = database {
                    write!(f, "{database}.")?;
                }
                write!(f, "{table}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct GrantShareStmt {
    pub privilege: ShareGrantObjectPrivilege,
    pub object: ShareGrantObjectName,
    pub share: Identifier,
}

impl Display for GrantShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GRANT {} ON {} TO SHARE {}",
            self.privilege, self.object, self.share
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct RevokeShareStmt {
    pub privilege: ShareGrantObjectPrivilege,
    pub object: ShareGrantObjectName,
    pub share: Identifier,
}

impl Display for RevokeShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "REVOKE {} ON {} FROM SHARE {}",
            self.privilege, self.object, self.share
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct ShowSharesStmt {
    pub show_options: Option<ShowOptions>,
}

impl Display for ShowSharesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW SHARES")?;
        if let Some(show_options) = &self.show_options {
            write!(f, " {show_options}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct DescShareStmt {
    pub name: ShareRef,
}

impl Display for DescShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESC SHARE {}", self.name)
    }
}
