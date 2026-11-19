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
use std::marker::PhantomData;

use super::rows::Rows;

/// A cursor point to a certain row in a data block.
#[derive(Clone)]
pub struct Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub input_index: usize,
    pub row_index: usize,

    _o: PhantomData<O>,

    /// rows within [`Cursor`] should be monotonic.
    rows: R,
}

impl<R, O> Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub fn new(input_index: usize, rows: R) -> Self {
        O::new_cursor(input_index, rows)
    }

    #[inline]
    pub fn advance(&mut self) -> usize {
        let res = self.row_index;
        self.row_index += 1;
        res
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.rows.len() == self.row_index
    }

    #[inline]
    pub fn current(&self) -> R::Item<'_> {
        self.rows.row(self.row_index)
    }

    #[inline]
    pub fn last(&self) -> R::Item<'_> {
        self.rows.last()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn cursor_mut(&self) -> CursorMut<'_, R, O> {
        CursorMut {
            row_index: self.row_index,
            cursor: self,
        }
    }
}

pub trait CursorOrder<R: Rows>: Sized + Copy {
    fn eq(a: &Cursor<R, Self>, b: &Cursor<R, Self>) -> bool;

    fn cmp(a: &Cursor<R, Self>, b: &Cursor<R, Self>) -> Ordering;

    fn new_cursor(input_index: usize, rows: R) -> Cursor<R, Self> {
        Cursor::<R, Self> {
            input_index,
            row_index: 0,
            rows,
            _o: PhantomData,
        }
    }
}

impl<R, O> Ord for Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        O::cmp(self, other)
    }
}

impl<R, O> PartialEq for Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn eq(&self, other: &Self) -> bool {
        O::eq(self, other)
    }
}

impl<R, O> Eq for Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
}

impl<R, O> PartialOrd for Cursor<R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct CursorMut<'a, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub row_index: usize,

    cursor: &'a Cursor<R, O>,
}

impl<'a, R, O> CursorMut<'a, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub fn advance(&mut self) -> usize {
        let res = self.row_index;
        self.row_index += 1;
        res
    }

    pub fn is_finished(&self) -> bool {
        self.row_index == self.cursor.rows.len()
    }

    pub fn current<'b>(&'b self) -> R::Item<'a> {
        self.cursor.rows.row(self.row_index)
    }
}
