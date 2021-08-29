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

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::aggregate;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DFBooleanArray {
    pub(crate) array: BooleanArray,
}

impl From<BooleanArray> for DFBooleanArray {
    fn from(array: BooleanArray) -> Self {
        Self { array }
    }
}

impl DFBooleanArray {
    pub fn new(array: BooleanArray) -> Self {
        Self { array }
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

    pub fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    pub fn get_inner(&self) -> &BooleanArray {
        &self.array
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => Some(self.array.value_unchecked(index)),
        };
        Ok(v.into())
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }
    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
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

    pub fn collect_values(&self) -> Vec<Option<bool>> {
        self.array.iter().collect()
    }
}
