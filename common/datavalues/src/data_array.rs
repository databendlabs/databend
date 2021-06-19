// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::buffer::Buffer;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_df_type::*;
use crate::DataType;
use crate::IGetDataType;

pub struct DataArray<T> {
    array: ArrayRef,
    t: PhantomData<T>,
}

impl<T> DataArray<T> {
    pub fn data_type(&self) -> DataType {
        self.array.data_type().into().unwrap()
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

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    fn is_null_mask(&self) -> BooleanArray {
        if self.null_count() == 0 {
            (0..self.len()).map(|_| Some(false)).collect()
        } else {
            let data = self.data();
            let valid = data.null_buffer().unwrap();
            let invert = !valid;

            let array_data = ArrayData::builder(DataType::Boolean)
                .len(self.len())
                .offset(self.offset())
                .add_buffer(invert)
                .build();
            BooleanArray::from(array_data)
        }
    }
    fn is_not_null_mask(&self) -> BooleanArray {
        if self.null_count() == 0 {
            (0..self.len()).map(|_| Some(true)).collect()
        } else {
            let data = self.data();
            let valid = data.null_buffer().unwrap().clone();

            let array_data = ArrayData::builder(DataType::Boolean)
                .len(self.len())
                .offset(self.offset())
                .add_buffer(valid)
                .build();
            BooleanArray::from(array_data)
        }
    }
}

pub trait ArrayTrait {}

impl<T> From<arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: ArrayRef) -> Self {
        Self {
            array,
            t: PhantomData::<T>,
        }
    }
}

impl<T> From<&arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: &arrow_array::ArrayRef) -> Self {
        Self {
            array: array.clone(),
            t: PhantomData::<T>,
        }
    }
}
