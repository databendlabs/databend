// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::TimestampMicrosecondArray;
use common_arrow::arrow::array::TimestampMillisecondArray;
use common_arrow::arrow::array::TimestampNanosecondArray;
use common_arrow::arrow::array::TimestampSecondArray;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;

pub type DataArrayRef = arrow::array::ArrayRef;

pub type NullArray = arrow::array::NullArray;

pub type BooleanArray = arrow::array::BooleanArray;

pub type PrimitiveArrayRef<T> = Arc<arrow::array::PrimitiveArray<T>>;
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
    Ok(arrow::compute::cast(&array, &to_type)?)
}

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "NULL".to_string()
        } else {
            array.value($row).to_string()
        };

        Ok(s)
    }};
}

macro_rules! make_string_hex {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            let mut tmp = "".to_string();

            for character in array.value($row) {
                tmp += &format!("{:02x}", character);
            }

            tmp
        };

        Ok(s)
    }};
}

macro_rules! make_string_datetime {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_datetime($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

macro_rules! make_string_date {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_date($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

pub fn data_array_to_string(column: &DataArrayRef, row: usize) -> Result<String> {
    if column.is_null(row) {
        return Ok("NULL".to_string());
    }
    match column.data_type() {
        DataType::Utf8 => make_string!(StringArray, column, row),
        DataType::Binary => make_string_hex!(BinaryArray, column, row),
        DataType::Boolean => make_string!(BooleanArray, column, row),
        DataType::Int8 => make_string!(Int8Array, column, row),
        DataType::Int16 => make_string!(Int16Array, column, row),
        DataType::Int32 => make_string!(Int32Array, column, row),
        DataType::Int64 => make_string!(Int64Array, column, row),
        DataType::UInt8 => make_string!(UInt8Array, column, row),
        DataType::UInt16 => make_string!(UInt16Array, column, row),
        DataType::UInt32 => make_string!(UInt32Array, column, row),
        DataType::UInt64 => make_string!(UInt64Array, column, row),
        DataType::Float16 => make_string!(Float32Array, column, row),
        DataType::Float32 => make_string!(Float32Array, column, row),
        DataType::Float64 => make_string!(Float64Array, column, row),
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
            make_string_datetime!(TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
            make_string_datetime!(TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
            make_string_datetime!(TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
            make_string_datetime!(TimestampNanosecondArray, column, row)
        }
        DataType::Date32 => make_string_date!(Date32Array, column, row),
        DataType::Date64 => make_string_date!(Date64Array, column, row),
        _ => Result::Err(ErrorCode::BadDataValueType(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}
