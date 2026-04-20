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

use crate::ast::ShowLimit;
use crate::ast::TableRef;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct ShowColumnsStmt {
    pub table: TableRef,
    pub full: bool,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowColumnsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW")?;
        if self.full {
            write!(f, " FULL")?;
        }
        write!(f, " COLUMNS FROM {}", self.table.table)?;
        if let Some(branch) = &self.table.branch {
            write!(f, "/{branch}")?;
        }

        if let Some(database) = &self.table.database {
            write!(f, " FROM ")?;
            if let Some(catalog) = &self.table.catalog {
                write!(f, "{catalog}.")?;
            }
            write!(f, "{database}")?;
        }

        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}
