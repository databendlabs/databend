// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::array::GenericStringArray;
use common_arrow::arrow::array::LargeBinaryArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::StringOffsetSizeTrait;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Date32Type;
use common_arrow::arrow::datatypes::Date64Type;
use common_arrow::arrow::datatypes::DurationMicrosecondType;
use common_arrow::arrow::datatypes::DurationMillisecondType;
use common_arrow::arrow::datatypes::DurationNanosecondType;
use common_arrow::arrow::datatypes::DurationSecondType;
use common_arrow::arrow::datatypes::Float32Type;
use common_arrow::arrow::datatypes::Float64Type;
use common_arrow::arrow::datatypes::Int16Type;
use common_arrow::arrow::datatypes::Int32Type;
use common_arrow::arrow::datatypes::Int64Type;
use common_arrow::arrow::datatypes::Int8Type;
use common_arrow::arrow::datatypes::IntervalDayTimeType;
use common_arrow::arrow::datatypes::IntervalUnit;
use common_arrow::arrow::datatypes::IntervalYearMonthType;
use common_arrow::arrow::datatypes::Time32MillisecondType;
use common_arrow::arrow::datatypes::Time32SecondType;
use common_arrow::arrow::datatypes::Time64MicrosecondType;
use common_arrow::arrow::datatypes::Time64NanosecondType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_arrow::arrow::datatypes::TimestampMicrosecondType;
use common_arrow::arrow::datatypes::TimestampMillisecondType;
use common_arrow::arrow::datatypes::TimestampNanosecondType;
use common_arrow::arrow::datatypes::TimestampSecondType;
use common_arrow::arrow::datatypes::UInt16Type;
use common_arrow::arrow::datatypes::UInt32Type;
use common_arrow::arrow::datatypes::UInt64Type;
use common_arrow::arrow::datatypes::UInt8Type;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataColumn;
use crate::DataValue;
use crate::Series;

pub trait FuseDataHasher {
    fn hash_bool(v: &bool) -> u64;

    fn hash_i8(v: &i8) -> u64;
    fn hash_i16(v: &i16) -> u64;
    fn hash_i32(v: &i32) -> u64;
    fn hash_i64(v: &i64) -> u64;

    fn hash_u8(v: &u8) -> u64;
    fn hash_u16(v: &u16) -> u64;
    fn hash_u32(v: &u32) -> u64;
    fn hash_u64(v: &u64) -> u64;

    fn hash_f32(v: &f32) -> u64;
    fn hash_f64(v: &f64) -> u64;

    fn hash_bytes(bytes: &[u8]) -> u64;
}

pub struct DataArrayHashDispatcher<Hasher: FuseDataHasher>(PhantomData<Hasher>);

impl<Hasher: FuseDataHasher> DataArrayHashDispatcher<Hasher> {
    pub fn dispatch(input: &DataColumn) -> Result<DataColumn> {
        match input {
            DataColumn::Array(input) => Ok(DataColumn::Array(Self::dispatch_array(input)?)),
            DataColumn::Constant(input, rows) => {
                Ok(DataColumn::Constant(Self::dispatch_constant(input)?, *rows))
            }
        }
    }

    fn dispatch_constant(input: &DataValue) -> Result<DataValue> {
        match input {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Boolean(None) => Ok(DataValue::Null),
            DataValue::Int8(None) => Ok(DataValue::Null),
            DataValue::Int16(None) => Ok(DataValue::Null),
            DataValue::Int32(None) => Ok(DataValue::Null),
            DataValue::Int64(None) => Ok(DataValue::Null),
            DataValue::UInt8(None) => Ok(DataValue::Null),
            DataValue::UInt16(None) => Ok(DataValue::Null),
            DataValue::UInt32(None) => Ok(DataValue::Null),
            DataValue::UInt64(None) => Ok(DataValue::Null),
            DataValue::Float32(None) => Ok(DataValue::Null),
            DataValue::Float64(None) => Ok(DataValue::Null),
            DataValue::Date32(None) => Ok(DataValue::Null),
            DataValue::Date64(None) => Ok(DataValue::Null),
            DataValue::Utf8(None) => Ok(DataValue::Null),
            DataValue::Binary(None) => Ok(DataValue::Null),
            DataValue::TimestampSecond(None) => Ok(DataValue::Null),
            DataValue::TimestampMicrosecond(None) => Ok(DataValue::Null),
            DataValue::TimestampMillisecond(None) => Ok(DataValue::Null),
            DataValue::TimestampNanosecond(None) => Ok(DataValue::Null),
            DataValue::IntervalDayTime(None) => Ok(DataValue::Null),
            DataValue::IntervalYearMonth(None) => Ok(DataValue::Null),
            DataValue::Boolean(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_bool(v)))),
            DataValue::Int8(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i8(v)))),
            DataValue::Int16(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i16(v)))),
            DataValue::Int32(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i32(v)))),
            DataValue::Int64(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i64(v)))),
            DataValue::UInt8(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_u8(v)))),
            DataValue::UInt16(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_u16(v)))),
            DataValue::UInt32(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_u32(v)))),
            DataValue::UInt64(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_u64(v)))),
            DataValue::Float32(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_f32(v)))),
            DataValue::Float64(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_f64(v)))),
            DataValue::Date32(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i32(v)))),
            DataValue::Date64(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i64(v)))),
            DataValue::Utf8(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_bytes(v.as_bytes()))))
            }
            DataValue::Binary(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_bytes(v.as_slice()))))
            }
            DataValue::TimestampSecond(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i64(v)))),
            DataValue::TimestampMicrosecond(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_i64(v))))
            }
            DataValue::TimestampMillisecond(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_i64(v))))
            }
            DataValue::TimestampNanosecond(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_i64(v))))
            }
            DataValue::IntervalDayTime(Some(v)) => Ok(DataValue::UInt64(Some(Hasher::hash_i64(v)))),
            DataValue::IntervalYearMonth(Some(v)) => {
                Ok(DataValue::UInt64(Some(Hasher::hash_i32(v))))
            }
            _ => Result::Err(ErrorCode::BadDataValueType(
                "DataArray Error: data_array_hash_with_array must be string or binary type.",
            )),
        }
    }

    fn dispatch_array(input: &Series) -> Result<Series> {
        match input.data_type() {
            DataType::Int8 => Self::dispatch_primitive_array(input, Int8Type {}, Hasher::hash_i8),
            DataType::Int16 => {
                Self::dispatch_primitive_array(input, Int16Type {}, Hasher::hash_i16)
            }
            DataType::Int32 => {
                Self::dispatch_primitive_array(input, Int32Type {}, Hasher::hash_i32)
            }
            DataType::Int64 => {
                Self::dispatch_primitive_array(input, Int64Type {}, Hasher::hash_i64)
            }
            DataType::UInt8 => Self::dispatch_primitive_array(input, UInt8Type {}, Hasher::hash_u8),
            DataType::UInt16 => {
                Self::dispatch_primitive_array(input, UInt16Type {}, Hasher::hash_u16)
            }
            DataType::UInt32 => {
                Self::dispatch_primitive_array(input, UInt32Type {}, Hasher::hash_u32)
            }
            DataType::UInt64 => {
                Self::dispatch_primitive_array(input, UInt64Type {}, Hasher::hash_u64)
            }
            DataType::Float32 => {
                Self::dispatch_primitive_array(input, Float32Type {}, Hasher::hash_f32)
            }
            DataType::Float64 => {
                Self::dispatch_primitive_array(input, Float64Type {}, Hasher::hash_f64)
            }
            DataType::Date32 => {
                Self::dispatch_primitive_array(input, Date32Type {}, Hasher::hash_i32)
            }
            DataType::Date64 => {
                Self::dispatch_primitive_array(input, Date64Type {}, Hasher::hash_i64)
            }
            DataType::Time32(TimeUnit::Second) => {
                Self::dispatch_primitive_array(input, Time32SecondType {}, Hasher::hash_i32)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Self::dispatch_primitive_array(input, Time32MillisecondType {}, Hasher::hash_i32)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Self::dispatch_primitive_array(input, Time64MicrosecondType {}, Hasher::hash_i64)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::dispatch_primitive_array(input, Time64NanosecondType {}, Hasher::hash_i64)
            }
            DataType::Duration(TimeUnit::Second) => {
                Self::dispatch_primitive_array(input, DurationSecondType {}, Hasher::hash_i64)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Self::dispatch_primitive_array(input, DurationMillisecondType {}, Hasher::hash_i64)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::dispatch_primitive_array(input, DurationMicrosecondType {}, Hasher::hash_i64)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Self::dispatch_primitive_array(input, DurationNanosecondType {}, Hasher::hash_i64)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::dispatch_primitive_array(input, IntervalYearMonthType {}, Hasher::hash_i32)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Self::dispatch_primitive_array(input, IntervalDayTimeType {}, Hasher::hash_i64)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Self::dispatch_primitive_array(input, TimestampSecondType {}, Hasher::hash_i64)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Self::dispatch_primitive_array(input, TimestampMillisecondType {}, Hasher::hash_i64)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::dispatch_primitive_array(input, TimestampMicrosecondType {}, Hasher::hash_i64)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::dispatch_primitive_array(input, TimestampNanosecondType {}, Hasher::hash_i64)
            }
            DataType::Utf8 => Self::dispatch_string_array::<i32>(input),
            DataType::Utf8 => Self::dispatch_string_array::<i64>(input),
            DataType::Binary => Self::dispatch_binary_array(input),
            DataType::LargeBinary => Self::dispatch_large_binary_array(input),
            _ => Result::Err(ErrorCode::BadDataValueType(
                " DataArray Error: data_array_hash_with_array must be string or binary type.",
            )),
        }
    }

    fn dispatch_primitive_array<T: ArrowPrimitiveType, F: Fn(&T::Native) -> u64>(
        input: &Series,
        _: T,
        fun: F,
    ) -> Result<Series> {
        let primitive_data = input
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    input.data_type(),
                    stringify!(PrimitiveArray<T>)
                ))
            })?;

        let value_size = primitive_data.len();
        let mut hash_builder = UInt64Array::builder(value_size);
        for index in 0..value_size {
            match primitive_data.is_null(index) {
                true => {
                    let _ = hash_builder.append_null()?;
                }
                false => {
                    let _ = hash_builder.append_value(fun(&primitive_data.value(index)))?;
                }
            };
        }

        Ok(Arc::new(hash_builder.finish()))
    }

    fn dispatch_string_array<T: StringOffsetSizeTrait>(data: &Series) -> Result<Series> {
        let binary_data = data
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                ErrorCode::BadDataValueType(format!(
                    "DataArray Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(GenericStringArray<T>)
                ))
            })?;

        let value_size = binary_data.len();
        let mut hash_builder = UInt64Array::builder(value_size);

        for index in 0..binary_data.len() {
            match binary_data.is_null(index) {
                true => {
                    let _ = hash_builder.append_null()?;
                }
                false => {
                    let _ = hash_builder
                        .append_value(Hasher::hash_bytes(binary_data.value(index).as_bytes()))?;
                }
            };
        }

        Ok(Arc::new(hash_builder.finish()))
    }

    fn dispatch_binary_array(data: &Series) -> Result<Series> {
        let binary_data = data.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                data.data_type(),
                stringify!(BinaryArray)
            ))
        })?;

        let value_size = binary_data.len();
        let mut hash_builder = UInt64Array::builder(value_size);

        for index in 0..binary_data.len() {
            match binary_data.is_null(index) {
                true => {
                    let _ = hash_builder.append_null()?;
                }
                false => {
                    let _ =
                        hash_builder.append_value(Hasher::hash_bytes(binary_data.value(index)))?;
                }
            };
        }

        Ok(Arc::new(hash_builder.finish()))
    }

    fn dispatch_large_binary_array(data: &Series) -> Result<Series> {
        let binary_data = data
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(LargeBinaryArray)
                ))
            })?;

        let value_size = binary_data.len();
        let mut hash_builder = UInt64Array::builder(value_size);

        for index in 0..binary_data.len() {
            match binary_data.is_null(index) {
                true => {
                    let _ = hash_builder.append_null()?;
                }
                false => {
                    let _ =
                        hash_builder.append_value(Hasher::hash_bytes(binary_data.value(index)))?;
                }
            };
        }

        Ok(Arc::new(hash_builder.finish()))
    }
}
