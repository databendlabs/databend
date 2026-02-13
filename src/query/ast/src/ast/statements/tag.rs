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

use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Literal;
use crate::ast::TypeName;
use crate::ast::quote::QuotedString;
use crate::ast::statements::show::ShowLimit;
use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateTagStmt {
    pub create_option: CreateOption,
    pub name: Identifier,
    pub allowed_values: Option<Vec<Literal>>,
    pub comment: Option<String>,
}

impl Display for CreateTagStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        write!(f, "TAG ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if let Some(values) = &self.allowed_values {
            write!(f, " ALLOWED_VALUES = (")?;
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{v}")?;
            }
            write!(f, ")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct TagSetItem {
    pub tag_name: Identifier,
    pub tag_value: String,
}

impl Display for TagSetItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} = {}",
            self.tag_name,
            QuotedString(&self.tag_value, '\'')
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropTagStmt {
    pub if_exists: bool,
    pub name: Identifier,
}

impl Display for DropTagStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP TAG ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowTagsStmt {
    pub filter: Option<ShowLimit>,
    pub limit: Option<u64>,
}

impl Display for ShowTagsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW TAGS")?;
        if let Some(filter) = &self.filter {
            write!(f, " {filter}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterObjectTagStmt {
    pub object: AlterObjectTagTarget,
    pub action: AlterObjectTagAction,
}

impl Display for AlterObjectTagStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER {}", self.object)?;
        write!(f, " {}", self.action)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterObjectTagTarget {
    Database {
        if_exists: bool,
        catalog: Option<Identifier>,
        database: Identifier,
    },
    Table {
        if_exists: bool,
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
    },
    Stage {
        if_exists: bool,
        stage_name: String,
    },
    Connection {
        if_exists: bool,
        connection_name: Identifier,
    },
    View {
        if_exists: bool,
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        view: Identifier,
    },
    Function {
        if_exists: bool,
        udf_name: Identifier,
    },
    Procedure {
        if_exists: bool,
        name: Identifier,
        arg_types: Vec<TypeName>,
    },
}

impl Display for AlterObjectTagTarget {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterObjectTagTarget::Database {
                if_exists,
                catalog,
                database,
            } => {
                write!(f, "DATABASE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_dot_separated_list(f, catalog.iter().chain(Some(database)))?;
            }
            AlterObjectTagTarget::Table {
                if_exists,
                catalog,
                database,
                table,
            } => {
                write!(f, "TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_dot_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(table)),
                )?;
            }
            AlterObjectTagTarget::Stage {
                if_exists,
                stage_name,
            } => {
                write!(f, "STAGE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{stage_name}")?;
            }
            AlterObjectTagTarget::Connection {
                if_exists,
                connection_name,
            } => {
                write!(f, "CONNECTION ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{connection_name}")?;
            }
            AlterObjectTagTarget::View {
                if_exists,
                catalog,
                database,
                view,
            } => {
                write!(f, "VIEW ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_dot_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(view)),
                )?;
            }
            AlterObjectTagTarget::Function {
                if_exists,
                udf_name,
            } => {
                write!(f, "FUNCTION ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{udf_name}")?;
            }
            AlterObjectTagTarget::Procedure {
                if_exists,
                name,
                arg_types,
            } => {
                write!(f, "PROCEDURE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{name}(")?;
                write_comma_separated_list(f, arg_types)?;
                write!(f, ")")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterObjectTagAction {
    Set { tags: Vec<TagSetItem> },
    Unset { tags: Vec<Identifier> },
}

impl Display for AlterObjectTagAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterObjectTagAction::Set { tags } => {
                write!(f, "SET TAG ")?;
                if tags.len() == 1 {
                    write!(f, "{}", &tags[0])?;
                } else {
                    write_comma_separated_list(f, tags)?;
                }
            }
            AlterObjectTagAction::Unset { tags } => {
                write!(f, "UNSET TAG ")?;
                if tags.len() == 1 {
                    write!(f, "{}", &tags[0])?;
                } else {
                    write_comma_separated_list(f, tags)?;
                }
            }
        }
        Ok(())
    }
}
