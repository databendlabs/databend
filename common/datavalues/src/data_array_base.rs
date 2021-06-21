// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::ArrayData;
use common_arrow::arrow::array::{self as arrow_array};
use common_arrow::arrow::buffer::Buffer;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_df_type::*;
use crate::DataArrayRef;
use crate::DataType;
use crate::DataValue;
use crate::*;

/// DataArrayBase is generic struct which implements DataArray
pub struct DataArrayBase<T> {
    array: arrow_array::ArrayRef,
    t: PhantomData<T>,
}

impl<T> DataArrayBase<T> {
    pub fn data_type(&self) -> DataType {
        DataType::try_from(self.array.data_type()).unwrap()
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, Option<Buffer>) {
        let data = self.array.data();

        (
            data.null_count(),
            data.null_bitmap().as_ref().map(|bitmap| {
                let buff = bitmap.buffer_ref();
                buff.clone()
            }),
        )
    }

    pub fn limit(&self, num_elements: usize) -> DataArrayRef {
        self.slice(0, num_elements)
    }

    pub fn get_array_memory_size(&self) -> usize {
        todo!()
    }

    pub fn slice(&self, offset: usize, length: usize) -> DataArrayRef {
        todo!()
    }

    pub fn cast_with_type(&self, data_type: &DataType) -> Result<DataArrayRef> {
        todo!()
    }

    pub fn try_get(&self, index: usize) -> Result<DataValue> {
        todo!()
    }
}

impl<T> From<arrow_array::ArrayRef> for DataArrayBase<T> {
    fn from(array: arrow_array::ArrayRef) -> Self {
        Self {
            array,
            t: PhantomData::<T>,
        }
    }
}

impl<T> From<&arrow_array::ArrayRef> for DataArrayBase<T> {
    fn from(array: &arrow_array::ArrayRef) -> Self {
        Self {
            array: array.clone(),
            t: PhantomData::<T>,
        }
    }
}
