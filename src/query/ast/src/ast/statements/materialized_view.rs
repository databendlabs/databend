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

use crate::ast::ClusterOption;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::ShowLimit;
use crate::ast::write_dot_separated_list;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct CreateMaterializedViewStmt {
    pub create_option: CreateOption,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
    pub cluster_by: Option<ClusterOption>,
    pub origin_query: Box<Query>,
}

impl Display for CreateMaterializedViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "MATERIALIZED VIEW ")?;
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
        if let Some(cluster_by) = &self.cluster_by {
            write!(f, " {cluster_by}")?;
        }
        write!(f, " AS {}", self.origin_query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct DropMaterializedViewStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
}

impl Display for DropMaterializedViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP MATERIALIZED VIEW ")?;
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

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct RefreshMaterializedViewStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
}

impl Display for RefreshMaterializedViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH MATERIALIZED VIEW ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Walk, WalkMut)]
pub struct ShowCreateMaterializedViewStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub view: Identifier,
}

impl Display for ShowCreateMaterializedViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW CREATE MATERIALIZED VIEW ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.view)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct ShowMaterializedViewsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowMaterializedViewsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW MATERIALIZED VIEWS")?;
        if self.catalog.is_some() || self.database.is_some() {
            write!(f, " FROM ")?;
            write_dot_separated_list(f, self.catalog.iter().chain(&self.database))?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }
        Ok(())
    }
}
