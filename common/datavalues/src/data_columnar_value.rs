// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_exception::Result;

use crate::BooleanArray;
use crate::DataArrayRef;
use crate::DataType;
use crate::DataValue;
use crate::PrimitiveArrayRef;
use crate::StringArray;

#[derive(Clone, Debug)]
pub enum DataColumnarValue {
    // Array of values.
    Array(DataArrayRef),
    // A Single value.
    Constant(DataValue, usize)
}

impl DataColumnarValue {
    pub fn data_type(&self) -> DataType {
        let x = match self {
            DataColumnarValue::Array(v) => v.data_type().clone(),
            DataColumnarValue::Constant(v, _) => v.data_type()
        };
        x
    }

    #[inline]
    pub fn to_array(&self) -> Result<DataArrayRef> {
        match self {
            DataColumnarValue::Array(array) => Ok(array.clone()),
            DataColumnarValue::Constant(scalar, size) => scalar.to_array_with_size(*size)
        }
    }

    pub fn len(&self) -> usize {
        match self {
            DataColumnarValue::Array(array) => array.len(),
            DataColumnarValue::Constant(_, size) => *size
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DataColumnarValue::Array(array) => array.len() == 0,
            DataColumnarValue::Constant(_, size) => *size == 0
        }
    }
}

impl From<DataArrayRef> for DataColumnarValue {
    fn from(array: DataArrayRef) -> Self {
        DataColumnarValue::Array(array)
    }
}

impl<T: ArrowPrimitiveType> From<PrimitiveArrayRef<T>> for DataColumnarValue {
    fn from(array: PrimitiveArrayRef<T>) -> Self {
        DataColumnarValue::Array(array as DataArrayRef)
    }
}

impl From<Arc<BooleanArray>> for DataColumnarValue {
    fn from(array: Arc<BooleanArray>) -> Self {
        DataColumnarValue::Array(array as DataArrayRef)
    }
}

impl From<Arc<StringArray>> for DataColumnarValue {
    fn from(array: Arc<StringArray>) -> Self {
        DataColumnarValue::Array(array as DataArrayRef)
    }
}
