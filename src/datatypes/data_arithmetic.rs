// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, UInt64Array};

use crate::datatypes::{DataArrayRef, DataType};
use crate::error::{Error, Result};

macro_rules! arithmetic_compute {
    ($LEFT:expr, $RIGHT:expr, $FUNC:ident) => {
        match ($LEFT).data_type() {
            DataType::Int64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Int64Array),
                    downcast_array!($RIGHT, Int64Array),
                )?));
            }
            DataType::UInt64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, UInt64Array),
                    downcast_array!($RIGHT, UInt64Array),
                )?));
            }
            DataType::Float64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Float64Array),
                    downcast_array!($RIGHT, Float64Array),
                )?));
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported Datatype: {:?} for Function: {} in compute_array",
                ($LEFT).data_type(),
                stringify!($FUNC),
            ))),
        }
    };
}

pub fn array_add(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, add)
}

pub fn array_sub(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, subtract)
}

pub fn array_mul(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, multiply)
}

pub fn array_div(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    arithmetic_compute!(&left, &right, divide)
}
