// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::{DataArrayRef, DataColumnarValue, DataType};
use crate::datavalues::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::error::{FuseQueryError, FuseQueryResult};

pub fn data_array_eq(
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(&l, &r, eq)
        }

        (DataColumnarValue::Array(l), DataColumnarValue::Scalar(r)) => {
            arrow_array_compute!(&l, r.to_array(l.len())?, eq)
        }

        (DataColumnarValue::Scalar(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(l.to_array(r.len())?, &r, eq)
        }

        _ => Err(FuseQueryError::Unsupported(format!(
            "left:{:?}, right:{:?} can not do data_array_eq",
            left.data_type(),
            right.data_type()
        ))),
    }
}
