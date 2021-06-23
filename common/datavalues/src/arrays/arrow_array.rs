// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayData;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_arrow::arrow::datatypes::DataType;
use num::Num;

/// Cheaply get the null mask as BooleanArray.
pub trait IsNull {
    fn is_null_mask(&self) -> BooleanArray;
    fn is_not_null_mask(&self) -> BooleanArray;
}

impl IsNull for &dyn Array {
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

pub trait GetValues {
    fn get_values<T>(&self) -> &[T::Native]
    where
        T: ArrowPrimitiveType,
        T::Native: Num;
}

impl GetValues for ArrayData {
    fn get_values<T>(&self) -> &[T::Native]
    where
        T: ArrowPrimitiveType,
        T::Native: Num,
    {
        debug_assert_eq!(&T::DATA_TYPE, self.data_type());
        // the first buffer is the value array
        let value_buf = &self.buffers()[0];
        let offset = self.offset();
        let vals = unsafe { value_buf.typed_data::<T::Native>() };
        &vals[offset..offset + self.len()]
    }
}

impl GetValues for &dyn Array {
    fn get_values<T>(&self) -> &[T::Native]
    where
        T: ArrowPrimitiveType,
        T::Native: Num,
    {
        self.data_ref().get_values::<T>()
    }
}

impl GetValues for ArrayRef {
    fn get_values<T>(&self) -> &[T::Native]
    where
        T: ArrowPrimitiveType,
        T::Native: Num,
    {
        self.data_ref().get_values::<T>()
    }
}

pub trait ToPrimitive {
    fn into_primitive_array<T>(self) -> PrimitiveArray<T>
    where T: ArrowPrimitiveType;
}

impl ToPrimitive for ArrayData {
    fn into_primitive_array<T>(self) -> PrimitiveArray<T>
    where T: ArrowPrimitiveType {
        PrimitiveArray::from(self)
    }
}

impl ToPrimitive for &dyn Array {
    fn into_primitive_array<T>(self) -> PrimitiveArray<T>
    where T: ArrowPrimitiveType {
        self.data().clone().into_primitive_array()
    }
}
