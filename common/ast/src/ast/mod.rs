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

mod expr;
mod query;
mod statement;

use std::fmt::Display;
use std::fmt::Formatter;

pub use expr::*;
pub use query::*;
pub use statement::*;

// Identifier of table name or column name.
#[derive(Debug, Clone, PartialEq)]
pub struct Identifier {
    pub name: String,
    pub quote: Option<char>,
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

fn write_period_separated_list(
    f: &mut Formatter<'_>,
    items: impl IntoIterator<Item = impl Display>,
) -> std::fmt::Result {
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ".")?;
        }
        write!(f, "{}", item)?;
    }
    Ok(())
}

fn write_comma_separated_list(
    f: &mut Formatter<'_>,
    items: impl IntoIterator<Item = impl Display>,
) -> std::fmt::Result {
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", item)?;
    }
    Ok(())
}
