// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, UInt64Array};

use crate::datavalues::{DataArrayRef, DataType};
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
