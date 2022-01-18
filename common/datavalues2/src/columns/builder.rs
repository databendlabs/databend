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
use crate::ColumnRef;
use crate::ConstColumn;
use crate::NullableColumn;
use crate::ScalarType;

pub struct ColumnBuilder<T: ScalarType> {
    builder: T::MutableColumnType,
    validity: MutableBitmap,
}

impl<T> ColumnBuilder<T>
where T: ScalarType + Default
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: <T::MutableColumnType>::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_default();
        self.validity.push(false);
    }

    #[inline]
    pub fn append_value(&mut self, value: T) {
        self.builder.append(value);
        self.validity.push(true);
    }

    #[inline]
    pub fn append(&mut self, value: T, valid: bool) {
        self.builder.append(value);
        self.validity.push(valid);
    }

    #[inline]
    pub fn build(&mut self, length: usize, nullable: bool) -> ColumnRef {
        let column = self.build_nonull(length);
        if nullable {
            return Arc::new(NullableColumn::new(
                column,
                std::mem::take(&mut self.validity).into(),
            ));
        }
        column
    }

    fn build_nonull(&mut self, length: usize) -> ColumnRef {
        let col = self.builder.as_column();
        if length != self.len() {
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
