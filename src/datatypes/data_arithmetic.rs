// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use super::*;

macro_rules! downcast_array {
    ($ARRAY:expr, $TYPE:ident) => {
        $ARRAY.as_any().downcast_ref::<$TYPE>().expect(
            format!(
                "Failed to downcast datatype:{:?} item to {}",
                ($ARRAY).data_type(),
                stringify!($TYPE)
            )
            .as_str(),
        );
    };
}

macro_rules! compute_array {
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
                "Datatype: {:?} for Function: {} in compute_array",
                ($LEFT).data_type(),
                stringify!($FUNC),
            ))),
        }
    };
}

pub fn array_add(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    compute_array!(&left, &right, add)
}

pub fn array_sub(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    compute_array!(&left, &right, subtract)
}

pub fn array_mul(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    compute_array!(&left, &right, multiply)
}

pub fn array_div(left: DataArrayRef, right: DataArrayRef) -> Result<DataArrayRef> {
    compute_array!(&left, &right, divide)
}
