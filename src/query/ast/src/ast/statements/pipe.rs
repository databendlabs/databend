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

use crate::ast::CopyIntoTableStmt;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreatePipeStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub auto_ingest: bool,
    pub comments: String,
    pub copy_stmt: CopyIntoTableStmt,
}

impl Display for CreatePipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE PIPE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;

        if self.auto_ingest {
            write!(f, " AUTO_INGEST = TRUE")?;
        }

        if !self.comments.is_empty() {
            write!(f, " COMMENTS = '{}'", self.comments)?;
        }

        write!(f, " AS {}", self.copy_stmt)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropPipeStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropPipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP PIPE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescribePipeStmt {
    pub name: String,
}

impl Display for DescribePipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE PIPE {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterPipeStmt {
    pub if_exists: bool,
    pub name: String,
    pub options: AlterPipeOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AlterPipeOptions {
    Set {
        execution_paused: Option<bool>,
        comments: Option<String>,
    },
    Refresh {
        prefix: Option<String>,
        modified_after: Option<String>,
    },
}

impl Display for AlterPipeOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterPipeOptions::Set {
                execution_paused,
                comments,
            } => {
                if let Some(execution_paused) = execution_paused {
                    write!(f, " SET PIPE_EXECUTION_PAUSED = {}", execution_paused)?;
                }
                if let Some(comments) = comments {
                    write!(f, " SET COMMENTS = '{}'", comments)?;
                }
                Ok(())
            }
            AlterPipeOptions::Refresh {
                prefix,
                modified_after,
            } => {
                write!(f, " REFRESH")?;
                if let Some(prefix) = prefix {
                    write!(f, " PREFIX = '{}'", prefix)?;
                }
                if let Some(modified_after) = modified_after {
                    write!(f, " MODIFIED_AFTER = '{}'", modified_after)?;
                }
                Ok(())
            }
        }
    }
}

impl Display for AlterPipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER PIPE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        write!(f, "{}", self.options)?;
        Ok(())
    }
}
