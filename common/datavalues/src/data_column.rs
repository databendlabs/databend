// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_exception::Result;

use crate::data_array_trait::DataArrayTrait;
use crate::BooleanArray;
use crate::DataType;
use crate::DataValue;
use crate::IGetDataType;
use crate::StringArray;

pub type DataArrayRef = Arc<DataArrayTrait>;
#[derive(Clone, Debug)]
pub enum DataColumn {
    // Array of values.
    Array(DataArrayRef),
    // A Single value.
    Constant(DataValue, usize),
}

impl DataColumn {
    #[inline]
    pub fn data_type(&self) -> DataType {
        let x = match self {
            DataColumn::Array(v) => v.data_type(),
            DataColumn::Constant(v, _) => v.data_type(),
        };
        x
    }

    #[inline]
    pub fn to_array(&self) -> Result<DataArrayRef> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, size) => scalar.to_array_with_size(*size),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.len(),
            DataColumn::Constant(_, size) => *size,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            DataColumn::Array(array) => array.len() == 0,
            DataColumn::Constant(_, size) => *size == 0,
        }
    }

    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.get_array_memory_size(),
            DataColumn::Constant(scalar, size) => scalar
                .to_array_with_size(*size)
                .map(|arr| arr.get_array_memory_size())
                .unwrap_or(0),
        }
    }

    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> DataColumn {
        match self {
            DataColumn::Array(array) => DataColumn::Array(array.slice(offset, length)),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), length),
        }
    }

    pub fn clone_empty(&self) -> DataColumn {
        match self {
            DataColumn::Array(array) => DataColumn::Array(array.slice(0, 0)),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), 0),
        }
    }
}

impl From<DataArrayRef> for DataColumn {
    fn from(array: DataArrayRef) -> Self {
        DataColumn::Array(array)
    }
}

impl<T: ArrowPrimitiveType> From<PrimitiveArrayRef<T>> for DataColumn {
    fn from(array: PrimitiveArrayRef<T>) -> Self {
        DataColumn::Array(array as DataArrayRef)
    }
}

impl From<Arc<BooleanArray>> for DataColumn {
    fn from(array: Arc<BooleanArray>) -> Self {
        DataColumn::Array(array as DataArrayRef)
    }
}

impl From<Arc<StringArray>> for DataColumn {
    fn from(array: Arc<StringArray>) -> Self {
        DataColumn::Array(array as DataArrayRef)
    }
}
