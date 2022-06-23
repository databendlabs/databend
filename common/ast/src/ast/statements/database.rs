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
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq)] // Databases
pub struct ShowDatabasesStmt<'a> {
    pub limit: Option<ShowLimit<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateDatabaseStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabaseStmt<'a> {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
    pub engine: Option<DatabaseEngine>,
    pub options: Vec<SQLProperty>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropDatabaseStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
    pub action: AlterDatabaseAction<'a>,
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
