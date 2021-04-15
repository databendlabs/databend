// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;

use crate::DataArrayRef;
use crate::DataType;
use crate::DataValue;

#[derive(Clone, Debug)]
pub enum DataColumnarValue {
    // Array of values.
    Array(DataArrayRef),
    // A Single value.
    Scalar(DataValue),
}

impl DataColumnarValue {
    pub fn data_type(&self) -> DataType {
        let x = match self {
            DataColumnarValue::Array(v) => v.data_type().clone(),
            DataColumnarValue::Scalar(v) => v.data_type(),
        };
        x
    }

    #[inline]
    pub fn to_array(&self, size: usize) -> Result<DataArrayRef> {
        match self {
            DataColumnarValue::Array(array) => Ok(array.clone()),
            DataColumnarValue::Scalar(scalar) => scalar.to_array_with_size(size),
        }
    }
}
