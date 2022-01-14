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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::{Bitmap, MutableBitmap};

mod mutable;
use std::sync::Arc;

pub use mutable::*;

use crate::prelude::*;

#[derive(Clone)]
pub struct NullableColumn {
    validity: Bitmap,
    column: ColumnRef,
}

impl NullableColumn {
    pub fn new(column: ColumnRef, validity: Bitmap) -> Self {
        Self { column, validity }
    }
}

impl Column for NullableColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        let nest = self.column.data_type();
        Arc::new(DataTypeNullable::create(nest))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn null_at(&self, row: usize) -> bool {
        !self.validity.get_bit(row)
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (false, Some(&self.validity))
    }

    fn memory_size(&self) -> usize {
        self.column.memory_size()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        self.column.as_arrow_array()
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        Arc::new(Self {
            column: self.column.slice(offset, length),
            validity: self.validity.clone().slice(offset, length),
        })
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        let column = self.column.replicate(offsets);

        let capacity = *offsets.last().unwrap();
        let mut bitmap = MutableBitmap::with_capacity(capacity);
        let mut previous_offset: usize = 0;
        for i in 0..self.len() {
            let offset: usize = offsets[i];
            let bit = self.validity.get_bit(i);
            bitmap.extend_constant(offset - previous_offset, bit);
            previous_offset = offset;
        }

        Arc::new(Self {
            validity: bitmap.into(),
            column,
        })
    }

    unsafe fn get_unchecked(&self, _index: usize) -> DataValue {
        self.column.get_unchecked(0)
    }
}

