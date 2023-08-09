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

use common_exception::Span;

use crate::parser::quote::quote_ident;

// Identifier of table name or column name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub name: String,
    pub quote: Option<char>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPosition {
    pub pos: usize,
    pub name: String,
    pub span: Span,
}

impl ColumnPosition {
    pub fn create(pos: usize, span: Span) -> ColumnPosition {
        ColumnPosition {
            pos,
            name: format!("${}", pos),
            span,
        }
    }
    pub fn name(&self) -> String {
        format!("${}", self.pos)
    }
}

impl Display for ColumnPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.pos)
    }
}

impl Identifier {
    pub fn is_quoted(&self) -> bool {
        self.quote.is_some()
    }

    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quote: None,
            span: Span::default(),
        }
    }

    pub fn from_name_with_quoted(name: impl Into<String>, quote: Option<char>) -> Self {
        Self {
            name: name.into(),
            quote,
            span: Span::default(),
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(c) = self.quote {
            let quoted = quote_ident(&self.name, c, true);
            write!(f, "{}", quoted)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

pub(crate) fn write_period_separated_list(
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
pub(crate) fn write_comma_separated_list(
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
pub(crate) fn write_quoted_comma_separated_list(
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
pub(crate) fn write_space_separated_map(
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
