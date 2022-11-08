// Copyright 2022 Datafuse Labs.
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

use common_arrow::ArrayRef;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

mod arrow;
mod simple;

pub use arrow::*;
pub use simple::*;

/// Convert columns to rows.
pub trait RowConverter<T: Rows>
where Self: Sized
{
    fn create(
        sort_columns_descriptions: Vec<SortColumnDescription>,
        output_schema: DataSchemaRef,
    ) -> Result<Self>;
    fn convert(&mut self, columns: &[ArrayRef]) -> Result<T>;
}

/// Rows to compare.
pub trait Rows {
    type Item<'a>: Ord
    where Self: 'a;

    fn len(&self) -> usize;
    fn row(&self, index: usize) -> Self::Item<'_>;
    fn row_unchecked(&self, _index: usize) -> Self::Item<'_> {
        unimplemented!()
    }
}

/// A cursor point to a certain row in a data block.
pub struct Cursor<R: Rows> {
    pub input_index: usize,
    pub row_index: usize,

    num_rows: usize,

    rows: R,
}

impl<R: Rows> Cursor<R> {
    pub fn try_create(input_index: usize, rows: R) -> Self {
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
    fn current(&self) -> R::Item<'_> {
        self.rows.row_unchecked(self.row_index)
    }
}

impl<R: Rows> Ord for Cursor<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current()
            .cmp(&other.current())
            .then_with(|| self.input_index.cmp(&other.input_index))
    }
}

impl<R: Rows> PartialEq for Cursor<R> {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl<R: Rows> Eq for Cursor<R> {}

impl<R: Rows> PartialOrd for Cursor<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
