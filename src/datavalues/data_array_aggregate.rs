// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use arrow::compute;

use crate::datavalues::{DataArrayRef, DataType, DataValue};
use crate::datavalues::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::error::{Error, Result};

pub fn data_array_min(value: DataArrayRef) -> Result<DataValue> {
    Ok(match value.data_type() {
        DataType::Int8 => typed_array_min_max_to_data_value!(value, Int8Array, Int8, min),
        DataType::Int16 => typed_array_min_max_to_data_value!(value, Int16Array, Int16, min),
        DataType::Int32 => typed_array_min_max_to_data_value!(value, Int32Array, Int32, min),
        DataType::Int64 => typed_array_min_max_to_data_value!(value, Int64Array, Int64, min),
        DataType::UInt8 => typed_array_min_max_to_data_value!(value, UInt8Array, UInt8, min),
        DataType::UInt16 => typed_array_min_max_to_data_value!(value, UInt16Array, UInt16, min),
        DataType::UInt32 => typed_array_min_max_to_data_value!(value, UInt32Array, UInt32, min),
        DataType::UInt64 => typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, min),
        DataType::Float32 => typed_array_min_max_to_data_value!(value, Float32Array, Float32, min),
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
        DataType::Int8 => typed_array_min_max_to_data_value!(value, Int8Array, Int8, max),
        DataType::Int16 => typed_array_min_max_to_data_value!(value, Int16Array, Int16, max),
        DataType::Int32 => typed_array_min_max_to_data_value!(value, Int32Array, Int32, max),
        DataType::Int64 => typed_array_min_max_to_data_value!(value, Int64Array, Int64, max),
        DataType::UInt8 => typed_array_min_max_to_data_value!(value, UInt8Array, UInt8, max),
        DataType::UInt16 => typed_array_min_max_to_data_value!(value, UInt16Array, UInt16, max),
        DataType::UInt32 => typed_array_min_max_to_data_value!(value, UInt32Array, UInt32, max),
        DataType::UInt64 => typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, max),
        DataType::Float32 => typed_array_min_max_to_data_value!(value, Float32Array, Float32, max),
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
        DataType::Int8 => typed_array_sum_to_data_value!(value, Int8Array, Int8),
        DataType::Int16 => typed_array_sum_to_data_value!(value, Int16Array, Int16),
        DataType::Int32 => typed_array_sum_to_data_value!(value, Int32Array, Int32),
        DataType::Int64 => typed_array_sum_to_data_value!(value, Int64Array, Int64),
        DataType::UInt8 => typed_array_sum_to_data_value!(value, UInt8Array, UInt8),
        DataType::UInt16 => typed_array_sum_to_data_value!(value, UInt16Array, UInt16),
        DataType::UInt32 => typed_array_sum_to_data_value!(value, UInt32Array, UInt32),
        DataType::UInt64 => typed_array_sum_to_data_value!(value, UInt64Array, UInt64),
        DataType::Float32 => typed_array_sum_to_data_value!(value, Float32Array, Float32),
        DataType::Float64 => typed_array_sum_to_data_value!(value, Float64Array, Float64),
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported data_array_sum() for data type: {:?}",
                value.data_type()
            )))
        }
    })
}
