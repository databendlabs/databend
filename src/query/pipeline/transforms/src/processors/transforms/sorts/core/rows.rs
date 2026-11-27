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
use std::fmt::Debug;
use std::ops::Range;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;

use super::RowConverter;

/// Rows can be compared.
pub trait Rows
where Self: Sized + Clone + Debug + Send
{
    const IS_ASC_COLUMN: bool;
    type Item<'a>: Ord + Debug
    where Self: 'a;
    type Type: ArgType;
    type Converter: RowConverter<Self>;

    fn len(&self) -> usize;
    fn row(&self, index: usize) -> Self::Item<'_>;

    /// Compares row `i` of `self` with row `j` of `other`, the same as comparing their items.
    ///
    /// Merging compares rows far more often than it reads them, so a type can compare in place
    /// instead of building both items.
    #[inline(always)]
    fn cmp_rows(&self, i: usize, other: &Self, j: usize) -> Ordering {
        self.row(i).cmp(&other.row(j))
    }

    /// Whether row `i` of `self` equals row `j` of `other`, the same as comparing their items.
    #[inline(always)]
    fn eq_rows(&self, i: usize, other: &Self, j: usize) -> bool {
        self.row(i) == other.row(j)
    }

    /// The number of rows at the start of `rows` that are less than or equal to row `j` of
    /// `other`.
    #[inline]
    fn leading_rows_le(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        let item = other.row(j);
        rows.take_while(|&i| self.row(i) <= item).count()
    }

    /// The number of rows at the start of `rows` that equal row `j` of `other`.
    #[inline]
    fn leading_rows_eq(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        let item = other.row(j);
        rows.take_while(|&i| self.row(i) == item).count()
    }

    /// Which rows are less than or equal to `bound`, the boundary of a TopN.
    fn rows_le_bound(&self, bound: &Scalar) -> Bitmap {
        let bound = Self::scalar_as_item(bound);
        let mut bitmap = MutableBitmap::with_capacity(self.len());
        for i in 0..self.len() {
            bitmap.push(self.row(i) <= bound);
        }
        bitmap.into()
    }

    fn to_column(&self) -> Column;

    fn from_column(col: &Column) -> Result<Self>;

    fn data_type() -> DataType {
        Self::Type::data_type()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first(&self) -> Self::Item<'_> {
        self.row(0)
    }

    fn last(&self) -> Self::Item<'_> {
        self.row(self.len() - 1)
    }

    fn slice(&self, range: Range<usize>) -> Self;

    fn scalar_as_item<'a>(s: &'a Scalar) -> Self::Item<'a>;

    fn owned_item(item: Self::Item<'_>) -> Scalar;
}
