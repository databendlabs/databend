// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::DataArrayRef;
use crate::DataType;
use crate::DataValue;

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

    #[inline]
    pub fn clone_empty(&self) -> DataColumn {
        match self {
            DataColumn::Array(array) => DataColumn::Array(array.slice(0, 0)),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), 0),
        }
    }

    #[inline]
    pub fn cast_with_type(&self, data_type: &DataType) -> Result<DataColumn> {
        match self {
            DataColumn::Array(array) => Ok(DataColumn::Array(array.cast_with_type(data_type)?)),
            DataColumn::Constant(scalar, size) => {
                let array = scalar.to_array_with_size(1)?;
                let array = array.cast_with_type(data_type)?;

                let value = array.try_get(0)?;
                Ok(DataColumn::Constant(value, *size))
            }
        }
    }

    #[inline]
    pub fn resize_constant(&self, size: usize) -> Self {
        match self {
            DataColumn::Array(array) if array.len() == 1 => {
                let value = array.try_get(0).unwrap();
                DataColumn::Constant(value, size)
            }
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), size),
            _ => self.clone(),
        }
    }

    #[inline]
    pub fn try_get(&self, index: usize) -> Result<DataValue> {
        match self {
            DataColumn::Array(array) => Ok(array.try_get(index)?),
            DataColumn::Constant(scalar, _) => Ok(scalar.clone()),
        }
    }
}

// static methods
impl DataColumn {
    fn to_array_with_size(value: DataValue, size: usize) -> Result<DataArrayRef> {
        todo!()
    }
}

impl From<DataArrayRef> for DataColumn {
    fn from(array: DataArrayRef) -> Self {
        DataColumn::Array(array)
    }
}
