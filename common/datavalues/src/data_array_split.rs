// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.


use crate::{DataColumnarValue, DataArrayRef};
use common_arrow::arrow::datatypes::{IntervalUnit, DataType, ArrowPrimitiveType, UInt8Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type, Float32Type, Float64Type, TimeUnit, Time32SecondType, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType, DurationSecondType, DurationMillisecondType, DurationMicrosecondType, DurationNanosecondType, IntervalYearMonthType, IntervalDayTimeType, TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType};
use common_arrow::arrow::array::{BooleanArray, PrimitiveArray, UInt64Array, BufferBuilder, ArrayData};
use common_arrow::arrow::buffer::MutableBuffer;
use common_exception::{Result, ErrorCodes};
use std::sync::Arc;

pub struct DataColumnarSplit;

impl DataColumnarSplit {
    #[inline]
    pub fn data_columnar_split(data: &DataColumnarValue, indices: &DataColumnarValue, nums: usize) -> Result<Vec<DataArrayRef>> {
        // if data.len() != indices.len() {
        //     return Result::Err(ErrorCodes::BadDataArrayLength(format!(
        //         "Selector requires data and indices to have the same number of arrays. data has {}, indices has {}.",
        //         data.len(),
        //         indices.len()
        //     )));
        // }

        match (data, indices) {
            (DataColumnarValue::Array(data), DataColumnarValue::Array(indices)) => {
                let indices_array_values = Self::indices_values(indices)?;
                Self::data_array_split(data, indices_array_values, nums)
            }
            _ => Err(ErrorCodes::BadColumnarType("".to_string()))
        }
    }

    fn data_array_split(data: &DataArrayRef, indices: &[u64], nums: usize) -> Result<Vec<DataArrayRef>> {
        match data.data_type() {
            // DataType::Boolean => {},
            DataType::Int8 => Self::data_array_split_for_primitive::<Int8Type>(data, indices, nums),
            DataType::Int16 => Self::data_array_split_for_primitive::<Int16Type>(data, indices, nums),
            DataType::Int32 => Self::data_array_split_for_primitive::<Int32Type>(data, indices, nums),
            DataType::Int64 => Self::data_array_split_for_primitive::<Int64Type>(data, indices, nums),
            DataType::UInt8 => Self::data_array_split_for_primitive::<UInt8Type>(data, indices, nums),
            DataType::UInt16 => Self::data_array_split_for_primitive::<UInt16Type>(data, indices, nums),
            DataType::UInt32 => Self::data_array_split_for_primitive::<UInt32Type>(data, indices, nums),
            DataType::UInt64 => Self::data_array_split_for_primitive::<UInt64Type>(data, indices, nums),
            DataType::Float32 => Self::data_array_split_for_primitive::<Float32Type>(data, indices, nums),
            DataType::Float64 => Self::data_array_split_for_primitive::<Float64Type>(data, indices, nums),
            DataType::Date32 => Self::data_array_split_for_primitive::<UInt8Type>(data, indices, nums),
            DataType::Date64 => Self::data_array_split_for_primitive::<UInt8Type>(data, indices, nums),
            DataType::Time32(TimeUnit::Second) => Self::data_array_split_for_primitive::<Time32SecondType>(data, indices, nums),
            DataType::Time32(TimeUnit::Millisecond) => Self::data_array_split_for_primitive::<Time32MillisecondType>(data, indices, nums),
            DataType::Time64(TimeUnit::Microsecond) => Self::data_array_split_for_primitive::<Time64MicrosecondType>(data, indices, nums),
            DataType::Time64(TimeUnit::Nanosecond) => Self::data_array_split_for_primitive::<Time64NanosecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Second) => Self::data_array_split_for_primitive::<DurationSecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Millisecond) => Self::data_array_split_for_primitive::<DurationMillisecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Microsecond) => Self::data_array_split_for_primitive::<DurationMicrosecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Nanosecond) => Self::data_array_split_for_primitive::<DurationNanosecondType>(data, indices, nums),
            DataType::Interval(IntervalUnit::YearMonth) => Self::data_array_split_for_primitive::<IntervalYearMonthType>(data, indices, nums),
            DataType::Interval(IntervalUnit::DayTime) => Self::data_array_split_for_primitive::<IntervalDayTimeType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Second, _) => Self::data_array_split_for_primitive::<TimestampSecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::data_array_split_for_primitive::<TimestampMillisecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::data_array_split_for_primitive::<TimestampMicrosecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::data_array_split_for_primitive::<TimestampNanosecondType>(data, indices, nums),
            // DataType::Binary => {},
            // DataType::FixedSizeBinary(i32) => {},
            // DataType::LargeBinary => {},
            // DataType::Utf8 => {},
            // DataType::LargeUtf8 => {},
            // DataType::List(Box < Field>) => {},
            // DataType::FixedSizeList(Box < Field>, i32) => {},
            // DataType::LargeList(Box < Field>) => {},
            // DataType::Struct(Vec < Field>) => {},
            // DataType::Union(Vec < Field>) => {},
            // DataType::Dictionary(Box < DataType>, Box < DataType>) => {},
            // DataType::Decimal(usize, usize) => {},
            _ => Result::Err(ErrorCodes::BadDataValueType("".to_string()))
        }
    }

    #[inline]
    fn data_array_split_for_primitive<T: ArrowPrimitiveType>(data: &DataArrayRef, indices: &[u64], nums: usize) -> Result<Vec<DataArrayRef>> {
        let mut res: Vec<DataArrayRef> = vec![];

        if data.null_count() == 0 {
            let data_array = data.as_any().downcast_ref::<PrimitiveArray<T>>().ok_or_else(|| {
                ErrorCodes::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(PrimitiveArray<T>)
                ))
            })?;

            let data_array_values = data_array.values();
            let mut selected_array = vec![];

            let reserve_size = ((data_array_values.len() as f64) * 1.1 / (nums as f64)) as usize;
            for index in 0..nums {
                selected_array.push(BufferBuilder::<T::Native>::new(reserve_size))
            }

            for index in 0..data_array_values.len() {
                selected_array[indices[index] as usize].append(data_array_values[index]);
            }

            for index in 0..nums {
                let mut builder = ArrayData::builder(T::DATA_TYPE)
                    .len(selected_array[index].len())
                    .add_buffer(selected_array[index].finish());

                res.push(Arc::new(PrimitiveArray::<T>::from(builder.build())));
            }
        }

        Ok(res)
    }

    #[inline]
    fn indices_values(indices: &DataArrayRef) -> Result<&[u64]> {
        Ok(downcast_array!(indices, UInt64Array)?.values())
    }
}
