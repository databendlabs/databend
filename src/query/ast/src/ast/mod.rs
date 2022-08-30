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
mod format;
mod query;
mod statements;

use std::fmt::Display;
use std::fmt::Formatter;

pub use expr::*;
pub use format::*;
pub use query::*;
pub use statements::*;

use crate::parser::token::Token;

// Identifier of table name or column name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier<'a> {
    pub name: String,
    pub quote: Option<char>,
    pub span: Token<'a>,
}

impl<'a> Identifier<'a> {
    pub fn is_quoted(&self) -> bool {
        self.quote.is_some()
    }
}

impl<'a> Display for Identifier<'a> {
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

/// Write input items into `a, b, c`
fn write_comma_separated_list(
    f: &mut Formatter<'_>,
    items: impl IntoIterator<Item = impl Display>,
) -> std::fmt::Result {
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

/// Write input items into `'a', 'b', 'c'`
fn write_quoted_comma_separated_list(
    f: &mut Formatter<'_>,
    items: impl IntoIterator<Item = impl Display>,
) -> std::fmt::Result {
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "'{item}'")?;
    }
    Ok(())
}

/// Write input map items into `field_a=x field_b=y`
fn write_space_seperated_map(
    f: &mut Formatter<'_>,
    items: impl IntoIterator<Item = (impl Display, impl Display)>,
) -> std::fmt::Result {
    for (i, (k, v)) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, " ")?;
        }
        write!(f, "{k}='{v}'")?;
    }
    Ok(())
}
