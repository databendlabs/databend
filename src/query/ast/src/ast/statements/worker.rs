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

use crate::ast::Identifier;
use crate::ast::statements::tag::TagSetItem;
use crate::ast::write_comma_separated_list;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowWorkersStmt {}

impl Display for ShowWorkersStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW WORKERS")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateWorkerStmt {
    pub if_not_exists: bool,
    pub name: Identifier,
    pub options: BTreeMap<String, String>,
}

impl Display for CreateWorkerStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE WORKER ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        if !self.options.is_empty() {
            write!(f, " WITH ")?;
            for (idx, (key, value)) in self.options.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }
                write!(f, " {} = '{}'", key, value)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropWorkerStmt {
    pub if_exists: bool,
    pub name: Identifier,
}

impl Display for DropWorkerStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP WORKER ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AlterWorkerAction {
    SetTag { tags: Vec<TagSetItem> },
    UnsetTag { tags: Vec<Identifier> },
    SetOptions { options: BTreeMap<String, String> },
    UnsetOptions { options: Vec<Identifier> },
    Suspend,
    Resume,
}

impl Display for AlterWorkerAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterWorkerAction::SetTag { tags } => {
                write!(f, "SET TAG ")?;
                write_comma_separated_list(f, tags)
            }
            AlterWorkerAction::UnsetTag { tags } => {
                write!(f, "UNSET TAG ")?;
                write_comma_separated_list(f, tags)
            }
            AlterWorkerAction::SetOptions { options } => {
                write!(f, "SET ")?;
                for (idx, (key, value)) in options.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{} = '{}'", key, value)?;
                }
                Ok(())
            }
            AlterWorkerAction::UnsetOptions { options } => {
                write!(f, "UNSET ")?;
                write_comma_separated_list(f, options)
            }
            AlterWorkerAction::Suspend => write!(f, "SUSPEND"),
            AlterWorkerAction::Resume => write!(f, "RESUME"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterWorkerStmt {
    pub name: Identifier,
    pub action: AlterWorkerAction,
}

impl Display for AlterWorkerStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER WORKER {} {}", self.name, self.action)
    }
}
