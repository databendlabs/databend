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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use super::InsertSource;
use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct IntoClause {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub target_columns: Vec<Identifier>,
    pub source_columns: Vec<Identifier>,
}

impl Display for IntoClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "INTO ")?;
        if let Some(catalog) = &self.catalog {
            write!(f, "{}.", catalog)?;
        }
        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}", self.table)?;
        if !self.target_columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.target_columns)?;
            write!(f, ")")?;
        }
        if !self.source_columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.source_columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WhenClause {
    pub condition: Expr,
    pub into_clauses: Vec<IntoClause>,
}

impl Display for WhenClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WHEN ")?;
        self.condition.fmt(f)?;
        write!(f, " THEN ")?;
        write_comma_separated_list(f, &self.into_clauses)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ElseClause {
    pub into_clauses: Vec<IntoClause>,
}

impl Display for ElseClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ELSE ")?;
        write_comma_separated_list(f, &self.into_clauses)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InsertMultiTableStmt {
    pub when_clauses: Vec<WhenClause>,
    pub else_clause: Option<ElseClause>,
    pub source: InsertSource,
}

impl Display for InsertMultiTableStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "INSERT INTO ")?;
        self.source.fmt(f)?;
        write!(f, " ")?;
        write_comma_separated_list(f, &self.when_clauses)?;
        if let Some(else_clause) = &self.else_clause {
            write!(f, " ELSE ")?;
            write_comma_separated_list(f, &else_clause.into_clauses)?;
        }
        Ok(())
    }
}
