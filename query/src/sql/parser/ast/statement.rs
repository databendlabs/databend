// Copyright 2020 Datafuse Labs.
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

use super::expression::Literal;
use super::expression::TypeName;
use super::Identifier;
use crate::sql::parser::ast::query::Query;
use crate::sql::parser::ast::write_identifier_vec;

// SQL statement
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum Statement {
    // EXPLAIN
    Explain {
        analyze: bool,
        query: Box<Statement>,
    },

    // Query statement
    Select(Box<Query>),

    // Operational statements
    ShowTables,
    ShowDatabases,
    ShowSettings,
    ShowProcessList,
    ShowCreateTable {
        name: Vec<Identifier>,
    },

    // DDL statements
    CreateTable {
        if_not_exists: bool,
        name: Vec<Identifier>,
        columns: Vec<ColumnDefinition>,
        // TODO(leiysky): use enum to represent engine instead?
        // Thus we have to check validity of engine in parser.
        engine: String,
        options: Vec<SQLProperty>,
    },
    // Describe schema of a table
    // Like `SHOW CREATE TABLE`
    Describe {
        name: Vec<Identifier>,
    },
    DropTable {
        if_exists: bool,
        name: Vec<Identifier>,
    },
    TruncateTable {
        name: Vec<Identifier>,
    },
    CreateDatabase {
        if_not_exists: bool,
        name: Vec<Identifier>,
        // TODO(leiysky): use enum to represent engine instead?
        // Thus we have to check validity of engine in parser.
        engine: String,
        options: Vec<SQLProperty>,
    },
    UseDatabase {
        name: Vec<Identifier>,
    },
    KillStmt {
        object_id: Identifier,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SQLProperty {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: TypeName,
    pub nullable: bool,
    pub default_value: Option<Literal>,
}

impl Display for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ", self.name)?;
        write!(f, "{} ", self.data_type)?;
        if self.nullable {
            write!(f, "NULL")?;
        } else {
            write!(f, "NOT NULL")?;
        }
        if let Some(default_value) = &self.default_value {
            write!(f, " DEFAULT {}", default_value)?;
        }
        Ok(())
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain { analyze, query } => {
                write!(f, "EXPLAIN ")?;
                if *analyze {
                    write!(f, "ANALYZE ")?;
                }
                write!(f, "{} ", &query)?;
            }
            Statement::Select(query) => {
                write!(f, "{}", &query)?;
            }
            Statement::ShowTables => {
                write!(f, "SHOW TABLES")?;
            }
            Statement::ShowDatabases => {
                write!(f, "SHOW DATABASES")?;
            }
            Statement::ShowSettings => {
                write!(f, "SHOW SETTINGS")?;
            }
            Statement::ShowProcessList => {
                write!(f, "SHOW PROCESSLIST")?;
            }
            Statement::ShowCreateTable { name } => {
                write!(f, "SHOW CREATE TABLE ")?;
                write_identifier_vec(name, f)?;
            }
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                ..
            } => {
                write!(f, "CREATE TABLE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_identifier_vec(name, f)?;
                write!(f, " (")?;
                for i in 0..columns.len() {
                    write!(f, "{}", columns[i])?;
                    if i != columns.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")?;
                // TODO(leiysky): display rest information
            }
            Statement::Describe { name } => {
                write!(f, "DESCRIBE ")?;
                write_identifier_vec(name, f)?;
            }
            Statement::DropTable { if_exists, name } => {
                write!(f, "DROP TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_identifier_vec(name, f)?;
            }
            Statement::TruncateTable { name } => {
                write!(f, "TRUNCATE TABLE ")?;
                write_identifier_vec(name, f)?;
            }
            Statement::CreateDatabase {
                if_not_exists,
                name,
                ..
            } => {
                write!(f, "CREATE DATABASE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_identifier_vec(name, f)?;
                // TODO(leiysky): display rest information
            }
            Statement::UseDatabase { name } => {
                write!(f, "USE ")?;
                write_identifier_vec(name, f)?;
            }
            Statement::KillStmt { object_id } => {
                write!(f, "KILL {}", object_id)?;
            }
        }
        Ok(())
    }
}
