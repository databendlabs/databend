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

use std::iter::Cloned;
use std::iter::Enumerate;
use std::ops::Bound;
use std::ops::RangeBounds;

use enum_as_inner::EnumAsInner;
use nom::Needed;
use nom::Offset;

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

impl<'a> Offset for Input<'a> {
    fn offset(&self, second: &Self) -> usize {
        self.tokens.len().saturating_sub(second.tokens.len())
    }
}

impl<'a> nom::Input for Input<'a> {
    type Item = Token<'a>;
    type Iter = Cloned<std::slice::Iter<'a, Token<'a>>>;
    type IterIndices = Enumerate<Self::Iter>;

    fn input_len(&self) -> usize {
        self.tokens.len()
    }

    fn take(&self, index: usize) -> Self {
        self.slice(..index)
    }

    fn take_from(&self, index: usize) -> Self {
        self.slice(index..)
    }

    fn take_split(&self, index: usize) -> (Self, Self) {
        (self.slice(index..), self.slice(..index))
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where P: Fn(Self::Item) -> bool {
        self.tokens
            .iter()
            .position(|token| predicate(token.clone()))
    }

    fn iter_elements(&self) -> Self::Iter {
        self.tokens.iter().cloned()
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

impl<'a, T: PartialEq> PartialEq for WithSpan<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.elem == other.elem
            && std::ptr::eq(self.span.tokens.as_ptr(), other.span.tokens.as_ptr())
            && self.span.tokens.len() == other.span.tokens.len()
    }
}

impl<'a, T: Eq> Eq for WithSpan<'a, T> {}

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
