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

use crate::ast::Identifier;
use crate::ast::SetType;
use crate::ast::SetValues;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Settings {
    pub set_type: SetType,
    pub identifiers: Vec<Identifier>,
    pub values: SetValues,
}

impl Display for Settings {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.set_type {
            SetType::SettingsGlobal => write!(f, "GLOBAL ")?,
            SetType::SettingsSession => write!(f, "SESSION ")?,
            SetType::Variable => write!(f, "VARIABLE ")?,
            SetType::SettingsQuery => write!(f, "")?,
        }

        if self.identifiers.len() > 1 {
            write!(f, "(")?;
        }
        for (idx, variable) in self.identifiers.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{variable}")?;
        }
        if self.identifiers.len() > 1 {
            write!(f, ")")?;
        }

        match &self.values {
            SetValues::Expr(exprs) => {
                write!(f, " = ")?;
                if exprs.len() > 1 {
                    write!(f, "(")?;
                }

                for (idx, value) in exprs.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{value}")?;
                }
                if exprs.len() > 1 {
                    write!(f, ")")?;
                }
            }
            SetValues::Query(query) => {
                write!(f, " = {query}")?;
            }
            SetValues::None => {}
        }
        Ok(())
    }
}
