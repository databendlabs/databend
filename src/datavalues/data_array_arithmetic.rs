// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::{DataArrayRef, DataType};
use crate::datavalues::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::error::{Error, Result};

pub fn data_array_add(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, add)
}

pub fn data_array_sub(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, subtract)
}

pub fn data_array_mul(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, multiply)
}

pub fn data_array_div(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, divide)
}
