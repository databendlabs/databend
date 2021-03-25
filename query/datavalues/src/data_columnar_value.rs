// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::DataValueResult;
use crate::{DataArrayRef, DataType, DataValue};

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

    pub fn to_array(&self, size: usize) -> DataValueResult<DataArrayRef> {
        match self {
            DataColumnarValue::Array(array) => Ok(array.clone()),
            DataColumnarValue::Scalar(scalar) => scalar.to_array(size),
        }
    }
}
