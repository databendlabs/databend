// Copyright 2021 Datafuse Labs.
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

mod ast_visitor;
mod expression;
mod query;
mod statement;

use std::fmt::Display;
use std::fmt::Formatter;

pub use expression::*;
pub use query::*;
pub use statement::*;

// Identifier of table name or column name.
#[derive(Debug, Clone, PartialEq)]
pub struct Identifier {
    pub name: String,
    pub quote: Option<char>,
}

fn display_identifier_vec(f: &mut Formatter<'_>, name: &[Identifier]) -> std::fmt::Result {
    for i in 0..name.len() {
        write!(f, "{}", name[i])?;
        if i != name.len() - 1 {
            write!(f, ".")?;
        }
    }
    Ok(())
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(c) = self.quote {
            write!(f, "{}", c)?;
            write!(f, "{}", self.name)?;
            write!(f, "{}", c)
        } else {
            write!(f, "{}", self.name)
        }
    }
}
