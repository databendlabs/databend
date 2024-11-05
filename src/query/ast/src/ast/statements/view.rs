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

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateViewStmt {
    pub create_option: CreateOption,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
    pub columns: Vec<Identifier>,
    pub query: Box<Query>,
}

impl Display for CreateViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "VIEW ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterViewStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
    pub columns: Vec<Identifier>,
    pub query: Box<Query>,
}

impl Display for AlterViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER VIEW ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropViewStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
}

impl Display for DropViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP VIEW ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowViewsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub full: bool,
    pub limit: Option<ShowLimit>,
    pub with_history: bool,
}

impl Display for ShowViewsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW")?;
        if self.full {
            write!(f, " FULL")?;
        }
        write!(f, " VIEWS")?;
        if self.with_history {
            write!(f, " HISTORY")?;
        }
        if let Some(database) = &self.database {
            write!(f, " FROM ")?;
            if let Some(catalog) = &self.catalog {
                write!(f, "{catalog}.",)?;
            }
            write!(f, "{database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescribeViewStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
}

impl Display for DescribeViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE VIEW ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter().chain(Some(&self.view))),
        )
    }
}
