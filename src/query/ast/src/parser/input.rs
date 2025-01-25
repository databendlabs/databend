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

use std::ops::Range;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeTo;

use enum_as_inner::EnumAsInner;

use crate::parser::token::Token;
use crate::parser::Backtrace;

/// Input tokens slice with a backtrace that records all errors including
/// the optional branch.
#[derive(Debug, Clone, Copy)]
pub struct Input<'a> {
    pub tokens: &'a [Token<'a>],
    pub dialect: Dialect,
    pub mode: ParseMode,
    pub backtrace: &'a Backtrace,
}

impl<'a> std::ops::Deref for Input<'a> {
    type Target = [Token<'a>];

    fn deref(&self) -> &Self::Target {
        self.tokens
    }
}

impl nom::InputLength for Input<'_> {
    fn input_len(&self) -> usize {
        self.tokens.input_len()
    }
}

impl nom::Offset for Input<'_> {
    fn offset(&self, second: &Self) -> usize {
        let fst = self.tokens.as_ptr();
        let snd = second.tokens.as_ptr();

        (snd as usize - fst as usize) / std::mem::size_of::<Token>()
    }
}

impl nom::Slice<Range<usize>> for Input<'_> {
    fn slice(&self, range: Range<usize>) -> Self {
        Input {
            tokens: &self.tokens[range],
            ..*self
        }
    }
}

impl nom::Slice<RangeTo<usize>> for Input<'_> {
    fn slice(&self, range: RangeTo<usize>) -> Self {
        Input {
            tokens: &self.tokens[range],
            ..*self
        }
    }
}

impl nom::Slice<RangeFrom<usize>> for Input<'_> {
    fn slice(&self, range: RangeFrom<usize>) -> Self {
        Input {
            tokens: &self.tokens[range],
            ..*self
        }
    }
}

impl nom::Slice<RangeFull> for Input<'_> {
    fn slice(&self, _: RangeFull) -> Self {
        *self
    }
}

#[derive(Clone, Debug)]
pub struct WithSpan<'a, T> {
    pub(crate) span: Input<'a>,
    pub(crate) elem: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, EnumAsInner)]
pub enum ParseMode {
    #[default]
    Default,
    Template,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, EnumAsInner)]
pub enum Dialect {
    #[default]
    PostgreSQL,
    MySQL,
    Hive,
    PRQL,
    Experimental,
}

impl Dialect {
    pub fn is_ident_quote(&self, c: char) -> bool {
        match self {
            Dialect::MySQL => c == '`',
            Dialect::Hive => c == '`',
            // TODO: remove '`' quote support once mysql handler correctly set mysql dialect.
            Dialect::Experimental | Dialect::PostgreSQL | Dialect::PRQL => c == '"' || c == '`',
        }
    }

    pub fn is_string_quote(&self, c: char) -> bool {
        match self {
            Dialect::MySQL => c == '\'' || c == '"',
            Dialect::Hive => c == '\'' || c == '"',
            Dialect::Experimental | Dialect::PostgreSQL | Dialect::PRQL => c == '\'',
        }
    }

    pub fn substr_index_zero_literal_as_one(&self) -> bool {
        match self {
            Dialect::MySQL => false,
            Dialect::Hive => true,
            Dialect::Experimental | Dialect::PostgreSQL | Dialect::PRQL => false,
        }
    }

    pub fn default_ident_quote(&self) -> char {
        match self {
            Dialect::MySQL | Dialect::Hive => '`',
            Dialect::Experimental | Dialect::PostgreSQL | Dialect::PRQL => '"',
        }
    }
}
