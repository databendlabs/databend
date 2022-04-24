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

use crate::ast::expr::Literal;
use crate::ast::expr::TypeName;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::parser::token::Token;

// SQL statement
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum Statement<'a> {
    // EXPLAIN
    Explain {
        analyze: bool,
        query: Box<Statement<'a>>,
    },

    // Query statement
    Select(Box<Query>),

    // Operational statements
    ShowTables {
        database: Option<Identifier>,
    },
    ShowDatabases,
    ShowSettings,
    ShowProcessList,
    ShowCreateTable {
        database: Option<Identifier>,
        table: Identifier,
    },
    SetVariable {
        variable: Identifier,
        value: Literal,
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

    // DML statements
    Insert {
        table: Identifier,
        columns: Vec<Identifier>,
        source: InsertSource<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SQLProperty {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource<'a> {
    Streaming { format: String },
    Values { values_tokens: &'a [Token<'a>] },
    Select { query: Box<Query> },
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

impl<'a> Display for Statement<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain { analyze, query } => {
                write!(f, "EXPLAIN ")?;
                if *analyze {
                    write!(f, "ANALYZE ")?;
                }
                write!(f, "{}", &query)?;
            }
            Statement::Select(query) => {
                write!(f, "{}", &query)?;
            }
            Statement::ShowTables { database } => {
                write!(f, "SHOW TABLES")?;
                if let Some(database) = database {
                    write!(f, " FROM {}", database)?;
                }
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
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
            }
            Statement::SetVariable { variable, value } => {
                write!(f, "SET {} = {}", variable, value)?;
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
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                write!(f, " (")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")?;
                // TODO(leiysky): display rest information
            }
            Statement::Describe { database, table } => {
                write!(f, "DESCRIBE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
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
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
            }
            Statement::TruncateTable { database, table } => {
                write!(f, "TRUNCATE TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
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
                write!(f, "USE {}", name)?;
            }
            Statement::KillStmt { object_id } => {
                write!(f, "KILL {}", object_id)?;
            }
            Statement::Insert {
                table,
                columns,
                source,
            } => {
                write!(f, "INSERT INTO {}", table)?;
                if !columns.is_empty() {
                    write!(f, "(")?;
                    write_comma_separated_list(f, columns)?;
                    write!(f, ")")?;
                }
                match source {
                    InsertSource::Streaming { format } => write!(f, " FORMAT {}", format)?,
                    InsertSource::Values { values_tokens } => write!(
                        f,
                        " VALUES {}",
                        &values_tokens[0].source[values_tokens.first().unwrap().span.start
                            ..values_tokens.last().unwrap().span.end]
                    )?,
                    InsertSource::Select { query } => write!(f, " {}", query)?,
                }
            }
        }
        Ok(())
    }
}
