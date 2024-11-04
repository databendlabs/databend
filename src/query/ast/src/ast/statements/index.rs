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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateIndexStmt {
    pub index_type: TableIndexType,
    pub create_option: CreateOption,

    pub index_name: Identifier,

    pub query: Box<Query>,
    pub sync_creation: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum TableIndexType {
    Aggregating,
    // Join
    Inverted,
}

impl Display for TableIndexType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TableIndexType::Aggregating => {
                write!(f, "AGGREGATING")
            }
            TableIndexType::Inverted => {
                write!(f, "INVERTED")
            }
        }
    }
}

impl Display for CreateIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if !self.sync_creation {
            write!(f, "ASYNC ")?;
        }
        write!(f, "{} INDEX", self.index_type)?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }

        write!(f, " {}", self.index_name)?;
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub index: Identifier,
}

impl Display for DropIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP AGGREGATING INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }

        write!(f, " {index}", index = self.index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RefreshIndexStmt {
    pub index: Identifier,
    pub limit: Option<u64>,
}

impl Display for RefreshIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH AGGREGATING INDEX {index}", index = self.index)?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateInvertedIndexStmt {
    pub create_option: CreateOption,

    pub index_name: Identifier,

    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,

    pub columns: Vec<Identifier>,
    pub sync_creation: bool,
    pub index_options: BTreeMap<String, String>,
}

impl Display for CreateInvertedIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if !self.sync_creation {
            write!(f, "ASYNC ")?;
        }
        write!(f, "INVERTED INDEX")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }

        write!(f, " {}", self.index_name)?;
        write!(f, " ON ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " (")?;
        write_comma_separated_list(f, &self.columns)?;
        write!(f, ")")?;

        if !self.index_options.is_empty() {
            write!(f, " ")?;
            write_space_separated_string_map(f, &self.index_options)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropInvertedIndexStmt {
    pub if_exists: bool,
    pub index_name: Identifier,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for DropInvertedIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP INVERTED INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }

        write!(f, " {}", self.index_name)?;
        write!(f, " ON ")?;
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
pub struct RefreshInvertedIndexStmt {
    pub index_name: Identifier,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub limit: Option<u64>,
}

impl Display for RefreshInvertedIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH INVERTED INDEX")?;
        write!(f, " {}", self.index_name)?;
        write!(f, " ON ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}
