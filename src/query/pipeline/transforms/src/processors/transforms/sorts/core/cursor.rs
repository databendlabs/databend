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

/// A cursor pointing to a row in a sorted input stream.
#[derive(Debug, Clone)]
pub struct Cursor<'a, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub input_index: usize,
    pub row_index: usize,
    num_rows: usize,
    current: R::Item<'a>,
    last: R::Item<'a>,
    _o: PhantomData<O>,
}

impl<'a, R, O> Copy for Cursor<'a, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
}

impl<'a, R, O> Cursor<'a, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    pub fn new(
        input_index: usize,
        num_rows: usize,
        current: R::Item<'a>,
        last: R::Item<'a>,
    ) -> Self {
        debug_assert!(num_rows > 0);
        Self {
            input_index,
            row_index: 0,
            num_rows,
            current,
            last,
            _o: PhantomData,
        }
    }

    pub fn advance(&mut self, count: usize, current: Option<R::Item<'a>>) {
        self.row_index += count;
        debug_assert!(self.row_index <= self.num_rows);
        debug_assert_eq!(current.is_some(), !self.is_finished());
        if let Some(current) = current {
            self.current = current;
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.num_rows == self.row_index
    }

    #[inline]
    pub fn current(&self) -> R::Item<'a> {
        self.current
    }

    #[inline]
    pub fn last(&self) -> R::Item<'a> {
        self.last
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

pub trait CursorOrder<R: Rows>: Sized + Copy {
    fn eq<'a>(a: &Cursor<'a, R, Self>, b: &Cursor<'a, R, Self>) -> bool;

    fn cmp<'a>(a: &Cursor<'a, R, Self>, b: &Cursor<'a, R, Self>) -> Ordering;
}

impl<R, O> Ord for Cursor<'_, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        O::cmp(self, other)
    }
}

impl<R, O> PartialEq for Cursor<'_, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn eq(&self, other: &Self) -> bool {
        O::eq(self, other)
    }
}

impl<R, O> Eq for Cursor<'_, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
}

impl<R, O> PartialOrd for Cursor<'_, R, O>
where
    R: Rows,
    O: CursorOrder<R>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
