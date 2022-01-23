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
use common_arrow::arrow::datatypes::DataType as ArrowType;

use crate::prelude::*;

mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

#[derive(Debug, Clone)]
pub struct BooleanColumn {
    values: Bitmap,
}

impl From<BooleanArray> for BooleanColumn {
    fn from(array: BooleanArray) -> Self {
        Self {
            values: array.values().clone(),
        }
    }
}

impl BooleanColumn {
    pub fn new(array: BooleanArray) -> Self {
        Self {
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn from_arrow_data(values: Bitmap, validity: Option<Bitmap>) -> Self {
        Self::from_arrow_array(&BooleanArray::from_data(
            ArrowType::Boolean,
            values,
            validity,
        ))
    }

    pub fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl Column for BooleanColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        BooleanType::arc()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn memory_size(&self) -> usize {
        self.values.as_slice().0.len()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        let array = BooleanArray::from_data(ArrowType::Boolean, self.values.clone(), None);
        Arc::new(array)
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe {
            Arc::new(Self {
                values: self.values.clone().slice_unchecked(offset, length),
            })
        }
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        if offsets.is_empty() {
            return self.slice(0, 0);
        }

        let mut builder = MutableBooleanColumn::with_capacity(*offsets.last().unwrap());

        let mut previous_offset: usize = 0;

        (0..self.len()).for_each(|i| {
            let offset: usize = offsets[i];
            let data = self.values.get_bit(i);
            for _ in previous_offset..offset {
                builder.append_value(data);
            }
            previous_offset = offset;
        });

        builder.as_column()
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn get(&self, index: usize) -> DataValue {
        DataValue::Boolean(self.values.get_bit(index))
    }
}
