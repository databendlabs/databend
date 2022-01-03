// Copyright 2021 Datafuse Labs.
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
use crate::parser::ast::display_identifier_vec;
use crate::parser::ast::query::Query;

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
        database: Option<Identifier>,
        table: Identifier,
    },

    // DDL statements
    CreateTable {
        if_not_exists: bool,
        database: Option<Identifier>,
        table: Identifier,
        columns: Vec<ColumnDefinition>,
        // TODO(leiysky): use enum to represent engine instead?
        // Thus we have to check validity of engine in parser.
        engine: String,
        options: Vec<SQLProperty>,
        like_db: Option<Identifier>,
        like_table: Option<Identifier>,
    },
    // Describe schema of a table
    // Like `SHOW CREATE TABLE`
    Describe {
        database: Option<Identifier>,
        table: Identifier,
    },
    DropTable {
        if_exists: bool,
        database: Option<Identifier>,
        table: Identifier,
    },
    TruncateTable {
        database: Option<Identifier>,
        table: Identifier,
    },
    CreateDatabase {
        if_not_exists: bool,
        name: Identifier,
        // TODO(leiysky): use enum to represent engine instead?
        // Thus we have to check validity of engine in parser.
        engine: String,
        options: Vec<SQLProperty>,
    },
    UseDatabase {
        name: Identifier,
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
            Statement::ShowCreateTable { database, table } => {
                write!(f, "SHOW CREATE TABLE ")?;
                let mut idents = vec![];
                if let Some(ident) = database {
                    idents.push(ident.to_owned());
                }
                idents.push(table.to_owned());
                display_identifier_vec(f, &idents)?;
            }
            Statement::CreateTable {
                if_not_exists,
                database,
                table,
                columns,
                ..
            } => {
                write!(f, "CREATE TABLE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                let mut idents = vec![];
                if let Some(ident) = database {
                    idents.push(ident.to_owned());
                }
                idents.push(table.to_owned());
                display_identifier_vec(f, &idents)?;
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
            Statement::Describe { database, table } => {
                write!(f, "DESCRIBE ")?;
                let mut idents = vec![];
                if let Some(ident) = database {
                    idents.push(ident.to_owned());
                }
                idents.push(table.to_owned());
                display_identifier_vec(f, &idents)?;
            }
            Statement::DropTable {
                if_exists,
                database,
                table,
            } => {
                write!(f, "DROP TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                let mut idents = vec![];
                if let Some(ident) = database {
                    idents.push(ident.to_owned());
                }
                idents.push(table.to_owned());
                display_identifier_vec(f, &idents)?;
            }
            Statement::TruncateTable { database, table } => {
                write!(f, "TRUNCATE TABLE ")?;
                let mut idents = vec![];
                if let Some(ident) = database {
                    idents.push(ident.to_owned());
                }
                idents.push(table.to_owned());
                display_identifier_vec(f, &idents)?;
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
                write!(f, "{}", name)?;
                // TODO(leiysky): display rest information
            }
            Statement::UseDatabase { name } => {
                write!(f, "USE ")?;
                write!(f, "{}", name)?;
            }
            Statement::KillStmt { object_id } => {
                write!(f, "KILL {}", object_id)?;
            }
        }
        Ok(())
    }
}
