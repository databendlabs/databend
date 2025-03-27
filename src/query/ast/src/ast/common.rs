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
use std::fmt::Write as _;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use ethnum::i256;

use super::quote::QuotedString;
use crate::ast::quote::QuotedIdent;
use crate::ast::WithOptions;
use crate::Span;

// Identifier of table name or column name.
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct Identifier {
    pub span: Span,
    pub name: String,
    pub quote: Option<char>,
    #[drive(skip)]
    pub ident_type: IdentifierType,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum IdentifierType {
    #[default]
    None,
    Hole,
    Variable,
}

impl Identifier {
    pub fn is_quoted(&self) -> bool {
        self.quote.is_some()
    }

    pub fn is_hole(&self) -> bool {
        self.ident_type == IdentifierType::Hole
    }

    pub fn is_variable(&self) -> bool {
        self.ident_type == IdentifierType::Variable
    }

    pub fn from_name(span: Span, name: impl Into<String>) -> Self {
        Self {
            span,
            name: name.into(),
            quote: None,
            ident_type: IdentifierType::None,
        }
    }

    pub fn from_name_with_quoted(span: Span, name: impl Into<String>, quote: Option<char>) -> Self {
        Self {
            span,
            name: name.into(),
            quote,
            ident_type: IdentifierType::None,
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.is_hole() {
            write!(f, "IDENTIFIER(:{})", self.name)
        } else if self.is_variable() {
            write!(f, "IDENTIFIER(${})", self.name)
        } else if let Some(quote) = self.quote {
            write!(f, "{}", QuotedIdent(&self.name, quote))
        } else {
            write!(f, "{}", self.name)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ColumnPosition {
    pub span: Span,
    pub pos: usize,
    pub name: String,
}

impl ColumnPosition {
    pub fn create(span: Span, pos: usize) -> ColumnPosition {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "${}", self.pos)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ColumnID {
    Name(Identifier),
    Position(ColumnPosition),
}

impl ColumnID {
    pub fn name(&self) -> &str {
        match self {
            ColumnID::Name(id) => &id.name,
            ColumnID::Position(id) => &id.name,
        }
    }
}

impl Display for ColumnID {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ColumnID::Name(id) => write!(f, "{}", id),
            ColumnID::Position(id) => write!(f, "{}", id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DatabaseRef {
    pub catalog: Option<Identifier>,
    pub database: Identifier,
}

impl Display for DatabaseRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(catalog) = &self.catalog {
            write!(f, "{}.", catalog)?;
        }
        write!(f, "{}", self.database)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct TableRef {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub with_options: Option<WithOptions>,
}

impl Display for TableRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        assert!(self.catalog.is_none() || (self.catalog.is_some() && self.database.is_some()));
        if let Some(catalog) = &self.catalog {
            write!(f, "{}.", catalog)?;
        }
        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}", self.table)?;

        if let Some(with_options) = &self.with_options {
            write!(f, " {with_options}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ColumnRef {
    pub database: Option<Identifier>,
    pub table: Option<Identifier>,
    pub column: ColumnID,
}

impl Display for ColumnRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        assert!(self.database.is_none() || (self.database.is_some() && self.table.is_some()));

        if f.alternate() {
            write!(f, "{}", self.column)?;
            return Ok(());
        }

        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
        write!(f, "{}", self.column)?;
        Ok(())
    }
}

pub(crate) fn write_dot_separated_list(
    f: &mut Formatter,
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
    f: &mut Formatter,
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
pub(crate) fn write_comma_separated_string_list(
    f: &mut Formatter,
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

/// Write input map items into `field_a=x, field_b=y`
pub(crate) fn write_comma_separated_map(
    f: &mut Formatter,
    items: impl IntoIterator<Item = (impl Display, impl Display)>,
) -> std::fmt::Result {
    for (i, (k, v)) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{k} = {v}")?;
    }
    Ok(())
}

/// Write input map items into `field_a='x', field_b='y'`
pub(crate) fn write_comma_separated_string_map(
    f: &mut Formatter,
    items: impl IntoIterator<Item = (impl Display, impl Display)>,
) -> std::fmt::Result {
    for (i, (k, v)) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{k} = {}", QuotedString(v.to_string(), '\''))?;
    }
    Ok(())
}

/// Write input map items into `field_a='x' field_b='y'`
pub(crate) fn write_space_separated_string_map(
    f: &mut Formatter,
    items: impl IntoIterator<Item = (impl Display, impl Display)>,
) -> std::fmt::Result {
    for (i, (k, v)) in items.into_iter().enumerate() {
        if i > 0 {
            write!(f, " ")?;
        }
        write!(f, "{k} = {}", QuotedString(v.to_string(), '\''))?;
    }
    Ok(())
}

pub fn display_decimal_256(num: i256, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = i256::from(10).pow(scale as u32);
        // -1/10 = 0
        if num >= 0 {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        }
    }
    buf
}
