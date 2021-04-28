// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow;
use common_exception::{Result, ErrorCodes};
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::datatypes::DataType;

pub type DataArrayRef = arrow::array::ArrayRef;

pub type NullArray = arrow::array::NullArray;

pub type BooleanArray = arrow::array::BooleanArray;

pub type Int8Array = arrow::array::Int8Array;
pub type Int16Array = arrow::array::Int16Array;
pub type Int32Array = arrow::array::Int32Array;
pub type Int64Array = arrow::array::Int64Array;
pub type UInt8Array = arrow::array::UInt8Array;
pub type UInt16Array = arrow::array::UInt16Array;
pub type UInt32Array = arrow::array::UInt32Array;
pub type UInt64Array = arrow::array::UInt64Array;
pub type Float32Array = arrow::array::Float32Array;
pub type Float64Array = arrow::array::Float64Array;

pub type StringArray = arrow::array::StringArray;
pub type BinaryArray = arrow::array::BinaryArray;

pub type Date32Array = arrow::array::Date32Array;
pub type Date64Array = arrow::array::Date64Array;

pub type StructArray = arrow::array::StructArray;

pub fn data_array_cast(array: &ArrayRef, to_type: &DataType) -> Result<ArrayRef> {
    arrow::compute::cast(&array, &to_type).map_err(ErrorCodes::from_arrow)
}

// pub fn data_array_cast(array: &Result<ArrayRef>, to_type: &DataType) -> Result<ArrayRef> {
//     arrow::compute::cast(&left_array, &coercion_type).map_err(ErrorCodes::from_arrow)
// }

