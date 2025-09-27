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

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterRoleStmt {
    pub if_exists: bool,
    pub name: String,
    pub action: AlterRoleAction,
}

impl Display for AlterRoleStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER ROLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        write!(f, "{}", self.action)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterRoleAction {
    Comment(Option<String>),
}

impl Display for AlterRoleAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterRoleAction::Comment(Some(comment)) => {
                write!(f, " SET COMMENT = '{}'", comment)?;
            }
            AlterRoleAction::Comment(None) => {
                write!(f, " UNSET COMMENT")?;
            }
        }
        Ok(())
    }
}
