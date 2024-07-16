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

use std::cmp::Ordering;

use databend_common_expression::Column;

use super::rows::Rows;

/// A cursor point to a certain row in a data block.
#[derive(Clone)]
pub struct Cursor<R: Rows> {
    pub input_index: usize,
    pub row_index: usize,

    num_rows: usize,

    /// rows within [`Cursor`] should be monotonic.
    rows: R,
}

impl<R: Rows> Cursor<R> {
    pub fn new(input_index: usize, rows: R) -> Self {
        Self {
            input_index,
            row_index: 0,
            num_rows: rows.len(),
            rows,
        }
    }

    #[inline]
    pub fn advance(&mut self) -> usize {
        let res = self.row_index;
        self.row_index += 1;
        res
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.num_rows == self.row_index
    }

    #[inline]
    pub fn current(&self) -> R::Item<'_> {
        self.rows.row(self.row_index)
    }

    #[inline]
    pub fn last(&self) -> R::Item<'_> {
        self.rows.row(self.num_rows - 1)
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    #[inline]
    pub fn to_column(&self) -> Column {
        self.rows.to_column()
    }

    pub fn cursor_mut(&self) -> CursorMut<'_, R> {
        CursorMut {
            row_index: self.row_index,
            cursor: self,
        }
    }

    fn same_index(&self, other: &Self) -> bool {
        self.input_index == other.input_index && self.row_index == other.row_index
    }
}

impl<R: Rows> Ord for Cursor<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.same_index(other) {
            Ordering::Equal
        } else {
            self.current().cmp(&other.current())
        }
    }
}

impl<R: Rows> PartialEq for Cursor<R> {
    fn eq(&self, other: &Self) -> bool {
        self.same_index(other) || self.current() == other.current()
    }
}

impl<R: Rows> Eq for Cursor<R> {}

impl<R: Rows> PartialOrd for Cursor<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, R: Rows> PartialEq<Cursor<R>> for CursorMut<'a, R> {
    fn eq(&self, other: &Cursor<R>) -> bool {
        self.same_index(other) || self.current() == other.current()
    }
}

impl<'a, R: Rows> PartialOrd<Cursor<R>> for CursorMut<'a, R> {
    fn partial_cmp(&self, other: &Cursor<R>) -> Option<Ordering> {
        Some(if self.same_index(other) {
            Ordering::Equal
        } else {
            self.current().cmp(&other.current())
        })
    }
}

pub struct CursorMut<'a, R: Rows> {
    pub row_index: usize,

    cursor: &'a Cursor<R>,
}

impl<'a, R: Rows> CursorMut<'a, R> {
    pub fn advance(&mut self) -> usize {
        let res = self.row_index;
        self.row_index += 1;
        res
    }

    pub fn is_finished(&self) -> bool {
        self.row_index == self.cursor.num_rows
    }

    pub fn current<'b>(&'b self) -> R::Item<'a> {
        self.cursor.rows.row(self.row_index)
    }

    fn same_index(&self, other: &Cursor<R>) -> bool {
        self.cursor.input_index == other.input_index && self.row_index == other.row_index
    }
}
