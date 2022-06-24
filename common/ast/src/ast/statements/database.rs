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

use crate::ast::statements::show::ShowLimit;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq)] // Databases
pub struct ShowDatabasesStmt<'a> {
    pub limit: Option<ShowLimit<'a>>,
}

impl Display for ShowDatabasesStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW DATABASES")?;
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateDatabaseStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
}

impl Display for ShowCreateDatabaseStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW CREATE DATABASE ")?;
        write_period_separated_list(f, self.catalog.iter().chain(Some(&self.database)))?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabaseStmt<'a> {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
    pub engine: Option<DatabaseEngine>,
    pub options: Vec<SQLProperty>,
}

impl Display for CreateDatabaseStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE DATABASE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_period_separated_list(f, self.catalog.iter().chain(Some(&self.database)))?;
        if let Some(engine) = &self.engine {
            write!(f, " ENGINE = {engine}")?;
        }
        // TODO(leiysky): display rest information
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropDatabaseStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
}

impl Display for DropDatabaseStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP DATABASE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(f, self.catalog.iter().chain(Some(&self.database)))?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
    pub action: AlterDatabaseAction<'a>,
}

impl Display for AlterDatabaseStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER DATABASE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(f, self.catalog.iter().chain(Some(&self.database)))?;
        match &self.action {
            AlterDatabaseAction::RenameDatabase { new_db } => {
                write!(f, " RENAME TO {new_db}")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterDatabaseAction<'a> {
    RenameDatabase { new_db: Identifier<'a> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseEngine {
    Default,
    Github(String),
}

impl Display for DatabaseEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let DatabaseEngine::Github(token) = self {
            write!(f, "GITHUB(token=\'{token}\')")
        } else {
            write!(f, "DEFAULT")
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SQLProperty {
    pub name: String,
    pub value: String,
}
