// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use arrow::array::{Float64Array, Int64Array, UInt64Array};
use arrow::compute;

use crate::datavalues::{DataArrayRef, DataType, DataValue};
use crate::error::{Error, Result};

pub fn data_array_min(value: DataArrayRef) -> Result<DataValue> {
    Ok(match value.data_type() {
        DataType::Int64 => typed_array_min_max_to_data_value!(value, Int64Array, Int64, min),
        DataType::UInt64 => typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, min),
        DataType::Float64 => typed_array_min_max_to_data_value!(value, Float64Array, Float64, min),
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported data_array_min() for data type: {:?}",
                value.data_type()
            )))
        }
    })
}

pub fn data_array_max(value: DataArrayRef) -> Result<DataValue> {
    Ok(match value.data_type() {
        DataType::Int64 => typed_array_min_max_to_data_value!(value, Int64Array, Int64, max),
        DataType::UInt64 => typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, max),
        DataType::Float64 => typed_array_min_max_to_data_value!(value, Float64Array, Float64, max),
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported data_array_max() for data type: {:?}",
                value.data_type()
            )))
        }
    })
}

pub fn data_array_sum(value: DataArrayRef) -> Result<DataValue> {
    Ok(match value.data_type() {
        DataType::Int64 => typed_array_sum_to_data_value!(value, Int64Array, Int64),
        DataType::UInt64 => typed_array_sum_to_data_value!(value, UInt64Array, UInt64),
        DataType::Float64 => typed_array_sum_to_data_value!(value, Float64Array, Float64),
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported data_array_sum() for data type: {:?}",
                value.data_type()
            )))
        }
    })
}
