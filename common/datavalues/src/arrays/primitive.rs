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

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::aggregate;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::prelude::*;

/// DFPrimitiveArray is generic struct which wrapped arrow's PrimitiveArray
#[derive(Debug, Clone)]
pub struct DFPrimitiveArray<T: DFPrimitiveType> {
    pub array: PrimitiveArray<T>,
    pub data_type: DataType,
}

impl<T: DFPrimitiveType> DFPrimitiveArray<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        let data_type: DataType = array.data_type().into();
        Self { array, data_type }
    }

    pub fn from_array(array: &dyn Array) -> Self {
        let data_type: DataType = array.data_type().into();
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        Self {
            array: array.clone(),
            data_type,
        }
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
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
    pub fn get_array_ref(&self) -> ArrayRef {
        Arc::new(self.array.clone()) as ArrayRef
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn get_array_memory_size(&self) -> usize {
        aggregate::estimated_bytes_size(&self.array)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Create a new DataArray by taking ownership of the AlignedVec. This operation is zero copy.
    pub fn new_from_aligned_vec(values: AlignedVec<T>) -> Self {
        let array = to_primitive::<T>(values, None);
        Self::new(array)
    }

    /// Nullify values in slice with an existing null bitmap
    pub fn new_from_owned_with_null_bitmap(
        values: AlignedVec<T>,
        validity: Option<Bitmap>,
        data_type: DataType,
    ) -> Self {
        let array = to_primitive::<T>(values, validity);
        Self::new(array)
    }

    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = T> + '_ + Send + Sync + ExactSizeIterator + DoubleEndedIterator + TrustedLen
    {
        // .copied was significantly slower in benchmark, next call did not inline?
        self.array
            .values()
            .iter()
            .copied()
            .trust_my_length(self.len())
    }
}

#[inline]
pub fn to_primitive<T: DFPrimitiveType>(
    values: AlignedVec<T>,
    validity: Option<Bitmap>,
) -> PrimitiveArray<T> {
    PrimitiveArray::from_data(T::DATA_TYPE, values.into(), validity)
}

pub type DFUInt8Array = DFPrimitiveArray<u8>;
pub type DFUInt16Array = DFPrimitiveArray<u16>;
pub type DFUInt32Array = DFPrimitiveArray<u32>;
pub type DFUInt64Array = DFPrimitiveArray<u64>;

pub type DFInt8Array = DFPrimitiveArray<i8>;
pub type DFInt16Array = DFPrimitiveArray<i16>;
pub type DFInt32Array = DFPrimitiveArray<i32>;
pub type DFInt64Array = DFPrimitiveArray<i64>;

pub type DFFloat32Array = DFPrimitiveArray<f32>;
pub type DFFloat64Array = DFPrimitiveArray<f64>;
