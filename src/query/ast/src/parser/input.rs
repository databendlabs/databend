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
use std::sync::atomic::AtomicBool;

use enum_as_inner::EnumAsInner;

use crate::parser::token::Token;
use crate::parser::Backtrace;

/// Input tokens slice with a backtrace that records all errors including
/// the optional branch.
#[derive(Debug, Clone, Copy)]
pub struct Input<'a>(pub &'a [Token<'a>], pub ParserConfig, pub &'a Backtrace);

#[derive(Debug, Clone, Copy)]
pub struct ParserConfig {
    pub dialect: Dialect,
    pub allow_streaming_insert_source: bool,
}

impl ParserConfig {
    pub fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            allow_streaming_insert_source: true,
        }
    }
}

impl std::ops::Deref for ParserConfig {
    type Target = Dialect;

    fn deref(&self) -> &Self::Target {
        &self.dialect
    }
}

impl<'a> std::ops::Deref for Input<'a> {
    type Target = [Token<'a>];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> nom::InputLength for Input<'a> {
    fn input_len(&self) -> usize {
        self.0.input_len()
    }
}

impl<'a> nom::Offset for Input<'a> {
    fn offset(&self, second: &Self) -> usize {
        let fst = self.0.as_ptr();
        let snd = second.0.as_ptr();

        (snd as usize - fst as usize) / std::mem::size_of::<Token>()
    }
}

impl<'a> nom::Slice<Range<usize>> for Input<'a> {
    fn slice(&self, range: Range<usize>) -> Self {
        Input(&self.0[range], self.1, self.2)
    }
}

impl<'a> nom::Slice<RangeTo<usize>> for Input<'a> {
    fn slice(&self, range: RangeTo<usize>) -> Self {
        Input(&self.0[range], self.1, self.2)
    }
}

impl<'a> nom::Slice<RangeFrom<usize>> for Input<'a> {
    fn slice(&self, range: RangeFrom<usize>) -> Self {
        Input(&self.0[range], self.1, self.2)
    }
}

impl<'a> nom::Slice<RangeFull> for Input<'a> {
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
pub enum Dialect {
    #[default]
    PostgreSQL,
    MySQL,
    Hive,
    Experimental,
}

impl Dialect {
    pub fn is_ident_quote(&self, c: char) -> bool {
        match self {
            Dialect::MySQL => c == '`',
            Dialect::Hive => c == '`',
            // TODO: remove '`' quote support once mysql handler correctly set mysql dialect.
            Dialect::Experimental | Dialect::PostgreSQL => c == '"' || c == '`',
        }
    }

    pub fn is_string_quote(&self, c: char) -> bool {
        match self {
            Dialect::MySQL => c == '\'' || c == '"',
            Dialect::Hive => c == '\'' || c == '"',
            Dialect::Experimental | Dialect::PostgreSQL => c == '\'',
        }
    }

    pub fn is_null_biggest(&self) -> bool {
        match self {
            Dialect::MySQL => false,
            Dialect::Hive => false,
            Dialect::Experimental | Dialect::PostgreSQL => true,
        }
    }

    pub fn substr_index_zero_literal_as_one(&self) -> bool {
        match self {
            Dialect::MySQL => false,
            Dialect::Hive => true,
            Dialect::Experimental | Dialect::PostgreSQL => false,
        }
    }
}
