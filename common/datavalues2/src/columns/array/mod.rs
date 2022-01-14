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
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;

mod mutable;
use std::sync::Arc;

pub use mutable::*;

use crate::prelude::*;

type LargeListArray = ListArray<i64>;

#[derive(Clone)]
pub struct ArrayColumn {
    offsets: Buffer<i64>,
    values: ColumnRef,
}

impl ArrayColumn {
    pub fn new(array: LargeListArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone().into_column(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .clone(),
        )
    }
}

impl Column for ArrayColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        todo!()
    }

    fn is_nullable(&self) -> bool {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn null_at(&self, row: usize) -> bool {
        todo!()
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        todo!()
    }

    fn memory_size(&self) -> usize {
        todo!()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        let arrow_type = self.data_type().arrow_type();
        let array = self.values.as_arrow_array();
        Arc::new(LargeListArray::from_data(
            arrow_type,
            self.offsets.clone(),
            array,
            None,
        ))
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        unsafe {
            let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
            Arc::new(Self {
                offsets,
                values: self.values.clone(),
            })
        }
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        // match datatypes
        // TODO: see https://github.com/ClickHouse/ClickHouse/blob/340b53ef853348758c9042b16a8599120ebc8d22/src/Columns/ColumnArray.cpp
        todo!()
    }

    unsafe fn get_unchecked(&self, index: usize) -> DataValue {
        todo!()
    }
}
