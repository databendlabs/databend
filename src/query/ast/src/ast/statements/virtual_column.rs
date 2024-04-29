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

use databend_common_meta_app::schema::CreateOption;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateVirtualColumnStmt {
    #[drive(skip)]
    pub create_option: CreateOption,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,

    pub virtual_columns: Vec<Expr>,
}

impl Display for CreateVirtualColumnStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "VIRTUAL COLUMN ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "(")?;
        write_comma_separated_list(f, &self.virtual_columns)?;
        write!(f, ") FOR ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterVirtualColumnStmt {
    #[drive(skip)]
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,

    pub virtual_columns: Vec<Expr>,
}

impl Display for AlterVirtualColumnStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER VIRTUAL COLUMN ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "(")?;
        write_comma_separated_list(f, &self.virtual_columns)?;
        write!(f, ") FOR ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropVirtualColumnStmt {
    #[drive(skip)]
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for DropVirtualColumnStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP VIRTUAL COLUMN ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "FOR ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RefreshVirtualColumnStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for RefreshVirtualColumnStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH VIRTUAL COLUMN FOR ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowVirtualColumnsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowVirtualColumnsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW VIRTUAL COLUMNS")?;
        if let Some(table) = &self.table {
            write!(f, " FROM {}", table)?;
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
