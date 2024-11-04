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
pub struct SystemStmt {
    pub action: SystemAction,
}

impl Display for SystemStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SYSTEM {}", self.action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SystemAction {
    Backtrace(bool),
}

impl Display for SystemAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SystemAction::Backtrace(switch) => match switch {
                true => write!(f, "ENABLE EXCEPTION_BACKTRACE"),
                false => write!(f, "DISABLE EXCEPTION_BACKTRACE"),
            },
        }
    }
}
