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

use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt<'a> {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub view: Identifier<'a>,
    pub query: Box<Query<'a>>,
}

impl Display for CreateViewStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE VIEW ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )?;
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub view: Identifier<'a>,
    pub query: Box<Query<'a>>,
}

impl Display for AlterViewStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER VIEW ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )?;
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub view: Identifier<'a>,
}

impl Display for DropViewStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP VIEW ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )
    }
}
