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

mod builder;
mod iterator;

pub use builder::*;
use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::cast::binary_to_large_binary;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_exception::ErrorCode;
use common_exception::Result;
pub use iterator::*;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DFStringArray {
    pub(crate) array: LargeBinaryArray,
}

impl From<LargeBinaryArray> for DFStringArray {
    fn from(array: LargeBinaryArray) -> Self {
        Self { array }
    }
}

impl DFStringArray {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let arrow_type = get_physical_arrow_type(array.data_type());

        if arrow_type == &ArrowDataType::Binary {
            let arr = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let arr = binary_to_large_binary(arr, ArrowDataType::LargeBinary);
            return Self::new(arr);
        }

        if arrow_type == &ArrowDataType::Utf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            let iter = arr.iter().map(|x| x.map(|x| x.as_bytes()));
            return Self::from_iter_trusted_length(iter);
        }

        if arrow_type == &ArrowDataType::LargeUtf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();

            let iter = arr.iter().map(|x| x.map(|x| x.as_bytes()));
            return Self::from_iter_trusted_length(iter);
        }

        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn inner(&self) -> &LargeBinaryArray {
        &self.array
    }

    pub fn data_type(&self) -> &DataType {
        &DataType::String
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => Some(self.array.value_unchecked(index).to_owned()),
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
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, Option<&Bitmap>) {
        (self.array.null_count(), self.array.validity())
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

    pub fn collect_values(&self) -> Vec<Option<Vec<u8>>> {
        let e = self.inner().iter().map(|c| c.map(|d| d.to_owned()));
        e.collect()
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_string_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &LargeBinaryArray,
    indices: I,
) -> LargeBinaryArray {
    match arr.null_count() {
        0 => {
            let iter = indices
                .into_iter()
                .map(|idx| Some(arr.value_unchecked(idx)));
            LargeBinaryArray::from_trusted_len_iter_unchecked(iter)
        }
        _ => {
            let iter = indices.into_iter().map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value_unchecked(idx))
                }
            });
            LargeBinaryArray::from_trusted_len_iter_unchecked(iter)
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_string_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &LargeBinaryArray,
    indices: I,
) -> LargeBinaryArray {
    match arr.null_count() {
        0 => {
            let iter = indices
                .into_iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value_unchecked(idx)));

            LargeBinaryArray::from_trusted_len_iter_unchecked(iter)
        }
        _ => {
            let iter = indices.into_iter().map(|opt_idx| {
                opt_idx.and_then(|idx| {
                    if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value_unchecked(idx))
                    }
                })
            });

            LargeBinaryArray::from_trusted_len_iter_unchecked(iter)
        }
    }
}
