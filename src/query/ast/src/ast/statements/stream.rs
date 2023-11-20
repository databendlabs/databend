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

use crate::ast::write_dot_separated_list;
use crate::ast::Identifier;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq)]
pub enum StreamPoint {
    AtStream {
        database: Option<Identifier>,
        name: Identifier,
    },
}

impl Display for StreamPoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamPoint::AtStream { database, name } => {
                write!(f, " AT (STREAM => ")?;
                write_dot_separated_list(f, database.iter().chain(Some(name)))?;
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStreamStmt {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
    pub table_database: Option<Identifier>,
    pub table: Identifier,
    pub stream_point: Option<StreamPoint>,
    pub comment: Option<String>,
}

impl Display for CreateStreamStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE STREAM ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter())
                .chain(Some(&self.stream)),
        )?;
        write!(f, " ON TABLE ")?;
        write_dot_separated_list(f, self.table_database.iter().chain(Some(&self.table)))?;
        if let Some(stream_point) = &self.stream_point {
            write!(f, "{}", stream_point)?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{}'", comment)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropStreamStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
}

impl Display for DropStreamStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP STREAM ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter())
                .chain(Some(&self.stream)),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowStreamsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub full: bool,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowStreamsStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW ")?;
        if self.full {
            write!(f, "FULL ")?;
        }
        write!(f, "STREAMS")?;
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

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeStreamStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
}

impl Display for DescribeStreamStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE STREAM ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter().chain(Some(&self.stream))),
        )
    }
}
