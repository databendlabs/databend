// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::prelude::*;

/// A wrapper for a column.
/// It can help to better access the data without cast into nullable/const column.
pub struct ColumnViewer<'a, T: Scalar> {
    pub column: &'a T::ColumnType,
    pub validity: Bitmap,

    // for not nullable column, it's 0. we only need keep one sign bit to tell `null_at` that it's not null.
    // for nullable column, it's usize::max, validity will be cloned from nullable column.
    null_mask: usize,
    // for const column, it's 0, `value` function will fetch the first value of the column.
    // for not const column, it's usize::max, `value` function will fetch the value of the row in the column.
    non_const_mask: usize,
    size: usize,
}

impl<'a, T: Scalar> ColumnViewer<'a, T> {
    pub fn create(column: &'a ColumnRef) -> Result<Self> {
        let null_mask = get_null_mask(column);
        let non_const_mask = non_const_mask(column);
        let size = column.len();

        let (column, validity) = if column.is_nullable() {
            let c: &NullableColumn = unsafe { Series::static_cast(column) };
            (c.inner(), c.ensure_validity().clone())
        } else {
            let mut bitmap = MutableBitmap::with_capacity(1);
            bitmap.push(true);

            if column.is_const() {
                let c: &ConstColumn = unsafe { Series::static_cast(column) };
                (c.inner(), bitmap.into())
            } else {
                (column, bitmap.into())
            }
        };

        let column: &T::ColumnType = Series::check_get(column)?;

        Ok(Self {
            column,
            validity,
            null_mask,
            non_const_mask,
            size,
        })
    }

    #[inline]
    pub fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    pub fn null_at(&self, i: usize) -> bool {
        !self.valid_at(i)
    }

    #[inline]
    pub fn value(&self, i: usize) -> <T as Scalar>::RefType<'a> {
        self.column.get_data(i & self.non_const_mask)
    }

    #[inline]
    pub fn column(&self) -> &T::ColumnType {
        self.column
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[inline]
fn get_null_mask(column: &ColumnRef) -> usize {
    if !column.is_const() && !column.only_null() && column.is_nullable() {
        usize::MAX
    } else {
        0
    }
}

#[inline]
fn non_const_mask(column: &ColumnRef) -> usize {
    if !column.is_const() && !column.only_null() {
        usize::MAX
    } else {
        0
    }
}

pub struct ColumnViewerIter<'a, T: Scalar> {
    pub viewer: ColumnViewer<'a, T>,
    pub size: usize,
    pub pos: usize,
}

impl<'a, T: Scalar> ColumnViewerIter<'a, T> {
    pub fn create(col: &'a ColumnRef) -> Result<Self> {
        let viewer = ColumnViewer::create(col)?;
        let size = viewer.len();
        Ok(Self {
            viewer,
            size,
            pos: 0,
        })
    }
}

impl<'a, T: Scalar> Iterator for ColumnViewerIter<'a, T> {
    type Item = T::RefType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.size {
            None
        } else {
            let item = self.viewer.value(self.pos);
            self.pos += 1;
            Some(item)
        }
    }
}
