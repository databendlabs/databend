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

use std::sync::Arc;

/// A builder for a column.
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::prelude::MutableColumn;
use crate::prelude::Scalar;
use crate::ColumnRef;
use crate::ConstColumn;
use crate::NullableColumn;
use crate::ScalarColumn;
use crate::ScalarColumnBuilder;

pub type NullableColumnBuilder<T> = ColumnBuilderBase<true, T>;
pub type ColumnBuilder<T> = ColumnBuilderBase<false, T>;

pub struct ColumnBuilderBase<const NULLABLE: bool, T: Scalar> {
    builder: <T::ColumnType as ScalarColumn>::Builder,
    validity: MutableBitmap,
}

impl<const NULLABLE: bool, T> ColumnBuilderBase<NULLABLE, T>
where T: Scalar
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: <<T::ColumnType as ScalarColumn>::Builder>::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    fn build_nonull(&mut self, length: usize) -> ColumnRef {
        let size = self.len();
        let col = self.builder.to_column();
        if length != size && size == 1 {
            return Arc::new(ConstColumn::new(col, length));
        }
        col
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.builder.len() == 0
    }
}

impl<T> ColumnBuilderBase<true, T>
where T: Scalar
{
    #[inline]
    pub fn build(&mut self, length: usize) -> ColumnRef {
        let validity = std::mem::take(&mut self.validity).into();
        let column = self.build_nonull(length);
        Arc::new(NullableColumn::new(column, validity))
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_default();
        self.validity.push(false);
    }

    #[inline]
    pub fn append(&mut self, value: <T as Scalar>::RefType<'_>, valid: bool) {
        self.builder.push(value);
        self.validity.push(valid);
    }
}

impl<T> ColumnBuilderBase<false, T>
where T: Scalar
{
    #[inline]
    pub fn build(&mut self, length: usize) -> ColumnRef {
        self.build_nonull(length)
    }

    #[inline]
    pub fn build_column(&mut self) -> <T as Scalar>::ColumnType {
        self.builder.finish()
    }

    pub fn from_iterator<'a>(
        it: impl Iterator<Item = <T as Scalar>::RefType<'a>>,
    ) -> <T as Scalar>::ColumnType {
        <<T as Scalar>::ColumnType as ScalarColumn>::from_iterator(it)
    }

    #[inline]
    pub fn append(&mut self, value: <T as Scalar>::RefType<'_>) {
        self.builder.push(value);
    }
}
