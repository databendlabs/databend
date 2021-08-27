// Copyright 2020 Datafuse Labs.
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

use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::aggregate;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DFNullArray {
    pub array: NullArray,
}

impl Deref for DFNullArray {
    type Target = NullArray;
    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl DFNullArray {
    pub fn new(array: NullArray) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<NullArray>().unwrap();
        Self {
            array: array.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFBooleanArray {
    pub array: BooleanArray,
}

impl Deref for DFBooleanArray {
    type Target = BooleanArray;
    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl DFBooleanArray {
    pub fn new(array: BooleanArray) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        Self {
            array: array.clone(),
        }
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    pub fn get_array_ref(&self) -> ArrayRef {
        Arc::new(self.array.clone()) as ArrayRef
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    pub fn get_array_memory_size(&self) -> usize {
        aggregate::estimated_bytes_size(&self.array)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }
}

#[derive(Debug, Clone)]
pub struct DFUtf8Array {
    pub array: LargeUtf8Array,
}

impl DFUtf8Array {
    pub fn new(array: LargeUtf8Array) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<LargeUtf8Array>().unwrap();
        Self {
            array: array.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFBinaryArray {
    pub array: LargeBinaryArray,
}

impl DFBinaryArray {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        Self {
            array: array.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct DFListArray {
    pub array: LargeListArray,
}

impl Deref for DFListArray {
    type Target = LargeListArray;
    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl DFListArray {
    pub fn new(array: LargeListArray) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
        Self {
            array: array.clone(),
        }
    }

    pub fn sub_data_type(&self) -> DataType {
        match self.data_type() {
            DataType::List(sub_types) => sub_types.data_type().clone(),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFStructArray {
    pub array: StructArray,
}

impl Deref for DFStructArray {
    type Target = StructArray;
    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl DFStructArray {
    pub fn new(array: StructArray) -> Self {
        Self { array }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        Self {
            array: array.clone(),
        }
    }
}
