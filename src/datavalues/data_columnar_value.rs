// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datavalues::{DataArrayRef, DataType, DataValue};
use crate::error::FuseQueryResult;

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

    pub fn to_array(&self, size: usize) -> FuseQueryResult<DataArrayRef> {
        match self {
            DataColumnarValue::Array(array) => Ok(array.clone()),
            DataColumnarValue::Scalar(scalar) => scalar.to_array(size),
        }
    }
}
