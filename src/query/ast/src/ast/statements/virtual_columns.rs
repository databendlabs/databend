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

use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateVirtualColumnsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,

    pub virtual_columns: Vec<Expr>,
}

impl Display for CreateVirtualColumnsStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE VIRTUAL COLUMNS (")?;
        write_comma_separated_list(f, &self.virtual_columns)?;
        write!(f, ") FOR ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropVirtualColumnsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,

    pub virtual_columns: Vec<Expr>,
}

impl Display for DropVirtualColumnsStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP VIRTUAL COLUMNS (")?;
        write_comma_separated_list(f, &self.virtual_columns)?;
        write!(f, ") FOR ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenerateVirtualColumnsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for GenerateVirtualColumnsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "GENERATE VIRTUAL COLUMNS FOR ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        Ok(())
    }
}
