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

pub fn data_array_add(
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(&l, &r, add)
        }
        (DataColumnarValue::Array(l), DataColumnarValue::Scalar(r)) => {
            let ra = r.to_array(l.len())?;
            arrow_array_compute!(&l, &ra, add)
        }
        (DataColumnarValue::Scalar(l), DataColumnarValue::Array(r)) => {
            let la = l.to_array(r.len())?;
            arrow_array_compute!(&la, &r, add)
        }
        _ => Err(FuseQueryError::Unsupported(format!(
            "left:{:?}, right:{:?} can not do data_array_add",
            left.data_type(),
            right.data_type()
        ))),
    }
}

pub fn data_array_sub(
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(&l, &r, subtract)
        }
        (DataColumnarValue::Array(l), DataColumnarValue::Scalar(r)) => {
            let ra = r.to_array(l.len())?;
            arrow_array_compute!(&l, &ra, subtract)
        }
        (DataColumnarValue::Scalar(l), DataColumnarValue::Array(r)) => {
            let la = l.to_array(r.len())?;
            arrow_array_compute!(&la, &r, subtract)
        }
        _ => Err(FuseQueryError::Unsupported(format!(
            "left:{:?}, right:{:?} can not do data_array_sub",
            left.data_type(),
            right.data_type()
        ))),
    }
}

pub fn data_array_mul(
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(&l, &r, multiply)
        }
        (DataColumnarValue::Array(l), DataColumnarValue::Scalar(r)) => {
            let ra = r.to_array(l.len())?;
            arrow_array_compute!(&l, &ra, multiply)
        }
        (DataColumnarValue::Scalar(l), DataColumnarValue::Array(r)) => {
            let la = l.to_array(r.len())?;
            arrow_array_compute!(&la, &r, multiply)
        }
        _ => Err(FuseQueryError::Unsupported(format!(
            "left:{:?}, right:{:?} can not do data_array_mul",
            left.data_type(),
            right.data_type()
        ))),
    }
}

pub fn data_array_div(
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(l), DataColumnarValue::Array(r)) => {
            arrow_array_compute!(&l, &r, divide)
        }
        (DataColumnarValue::Array(l), DataColumnarValue::Scalar(r)) => {
            let ra = r.to_array(l.len())?;
            arrow_array_compute!(&l, &ra, divide)
        }
        (DataColumnarValue::Scalar(l), DataColumnarValue::Array(r)) => {
            let la = l.to_array(r.len())?;
            arrow_array_compute!(&la, &r, divide)
        }
        _ => Err(FuseQueryError::Unsupported(format!(
            "left:{:?}, right:{:?} can not do data_array_div",
            left.data_type(),
            right.data_type()
        ))),
    }
}
