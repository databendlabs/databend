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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;

use crate::prelude::*;

mod iterator;
pub use iterator::*;

#[derive(Debug, Clone)]
pub struct NullColumn {
    length: usize,
}

impl From<NullArray> for NullColumn {
    fn from(array: NullArray) -> Self {
        Self::new(array.len())
    }
}

impl NullColumn {
    pub fn new(length: usize) -> Self {
        Self { length }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self {
            length: array.len(),
        }
    }
    pub fn data_type(&self) -> DataTypePtr {
        Arc::new(DataTypeNullable::create(Arc::new(DataTypeNull {})))
    }
}

impl Column for NullColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
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
        (true, None)
    }

    fn memory_size(&self) -> usize {
        todo!()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        todo!()
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        Arc::new(Self {
            length: *offsets.last().unwrap() as usize,
        })
    }

    unsafe fn get_unchecked(&self, index: usize) -> DataValue {
        todo!()
    }
}
