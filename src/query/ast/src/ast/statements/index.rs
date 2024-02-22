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

use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub index_type: TableIndexType,
    pub create_option: CreateOption,

    pub index_name: Identifier,

    pub query: Box<Query>,
    pub sync_creation: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TableIndexType {
    Aggregating,
    // Join
}

impl Display for CreateIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        let sync = if self.sync_creation { "SYNC" } else { "ASYNC" };
        write!(f, "{} {:?} INDEX", sync, self.index_type)?;
        if let CreateOption::CreateIfNotExists(if_not_exists) = self.create_option {
            if if_not_exists {
                write!(f, " IF NOT EXISTS")?;
            }
        }

        write!(f, " {:?}", self.index_name)?;
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub index: Identifier,
}

impl Display for DropIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }

        write!(f, " {index}", index = self.index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefreshIndexStmt {
    pub index: Identifier,
    pub limit: Option<u64>,
}

impl Display for RefreshIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REFRESH INDEX {index}", index = self.index)?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}
