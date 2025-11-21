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

use std::iter::Enumerate;
use std::ops::Bound;
use std::ops::RangeBounds;

use enum_as_inner::EnumAsInner;
use nom::Needed;

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

impl<'a> Input<'a> {
    pub fn slice<R>(&self, range: R) -> Self
    where R: RangeBounds<usize> {
        let len = self.tokens.len();
        let start = match range.start_bound() {
            Bound::Included(&idx) => idx,
            Bound::Excluded(&idx) => idx + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&idx) => idx + 1,
            Bound::Excluded(&idx) => idx,
            Bound::Unbounded => len,
        };

        Input {
            tokens: &self.tokens[start.min(len)..end.min(len)],
            ..*self
        }
    }
}

impl nom::Offset for Input<'_> {
    fn offset(&self, second: &Self) -> usize {
        let fst = self.tokens.as_ptr();
        let snd = second.tokens.as_ptr();

        (snd as usize - fst as usize) / std::mem::size_of::<Token>()
    }
}

impl<'a> nom::Input for Input<'a> {
    type Item = &'a Token<'a>;
    type Iter = std::slice::Iter<'a, Token<'a>>;
    type IterIndices = Enumerate<Self::Iter>;

    fn input_len(&self) -> usize {
        self.tokens.len()
    }

    fn take(&self, index: usize) -> Self {
        self.slice(0..index)
    }

    fn take_from(&self, index: usize) -> Self {
        self.slice(index..)
    }

    fn take_split(&self, index: usize) -> (Self, Self) {
        let (prefix, suffix) = self.tokens.split_at(index);

        (
            Input {
                tokens: prefix,
                ..*self
            },
            Input {
                tokens: suffix,
                ..*self
            },
        )
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where P: Fn(Self::Item) -> bool {
        self.tokens.iter().position(predicate)
    }

    fn iter_elements(&self) -> Self::Iter {
        self.tokens.iter()
    }

    fn iter_indices(&self) -> Self::IterIndices {
        self.iter_elements().enumerate()
    }

    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
        if self.tokens.len() >= count {
            Ok(count)
        } else {
            Err(Needed::new(count - self.tokens.len()))
        }
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
