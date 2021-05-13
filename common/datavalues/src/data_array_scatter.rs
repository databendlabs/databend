// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.


use crate::{DataColumnarValue, DataArrayRef};
use common_arrow::arrow::datatypes::{IntervalUnit, DataType, ArrowPrimitiveType, UInt8Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type, Float32Type, Float64Type, TimeUnit, Time32SecondType, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType, DurationSecondType, DurationMillisecondType, DurationMicrosecondType, DurationNanosecondType, IntervalYearMonthType, IntervalDayTimeType, TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType, Date32Type, Date64Type};
use common_arrow::arrow::array::{BooleanArray, PrimitiveArray, UInt64Array, BufferBuilder, ArrayData, BooleanBufferBuilder, ArrayRef};
use common_arrow::arrow::buffer::MutableBuffer;
use common_exception::{Result, ErrorCodes};
use std::sync::Arc;
use common_arrow::parquet::record::reader::Reader::PrimitiveReader;

pub struct DataColumnarScatter;

impl DataColumnarScatter {
    #[inline]
    pub fn scatter(data: &DataArrayRef, indices: &DataArrayRef, nums: usize) -> Result<Vec<DataArrayRef>> {
        if data.len() != indices.len() {
            return Result::Err(ErrorCodes::BadDataArrayLength(format!(
                "Selector requires data and indices to have the same number of arrays. data has {}, indices has {}.",
                data.len(),
                indices.len()
            )));
        }

        let indices_array_values = Self::indices_values(indices)?;
        Self::scatter_data(data, indices_array_values, nums)
    }

    fn scatter_data(data: &DataArrayRef, indices: &[u64], nums: usize) -> Result<Vec<DataArrayRef>> {
        match data.data_type() {
            // DataType::Boolean => {},
            DataType::Int8 => Self::scatter_primitive_data::<Int8Type>(data, indices, nums),
            DataType::Int16 => Self::scatter_primitive_data::<Int16Type>(data, indices, nums),
            DataType::Int32 => Self::scatter_primitive_data::<Int32Type>(data, indices, nums),
            DataType::Int64 => Self::scatter_primitive_data::<Int64Type>(data, indices, nums),
            DataType::UInt8 => Self::scatter_primitive_data::<UInt8Type>(data, indices, nums),
            DataType::UInt16 => Self::scatter_primitive_data::<UInt16Type>(data, indices, nums),
            DataType::UInt32 => Self::scatter_primitive_data::<UInt32Type>(data, indices, nums),
            DataType::UInt64 => Self::scatter_primitive_data::<UInt64Type>(data, indices, nums),
            DataType::Float32 => Self::scatter_primitive_data::<Float32Type>(data, indices, nums),
            DataType::Float64 => Self::scatter_primitive_data::<Float64Type>(data, indices, nums),
            DataType::Date32 => Self::scatter_primitive_data::<Date32Type>(data, indices, nums),
            DataType::Date64 => Self::scatter_primitive_data::<Date64Type>(data, indices, nums),
            DataType::Time32(TimeUnit::Second) => Self::scatter_primitive_data::<Time32SecondType>(data, indices, nums),
            DataType::Time32(TimeUnit::Millisecond) => Self::scatter_primitive_data::<Time32MillisecondType>(data, indices, nums),
            DataType::Time64(TimeUnit::Microsecond) => Self::scatter_primitive_data::<Time64MicrosecondType>(data, indices, nums),
            DataType::Time64(TimeUnit::Nanosecond) => Self::scatter_primitive_data::<Time64NanosecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Second) => Self::scatter_primitive_data::<DurationSecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Millisecond) => Self::scatter_primitive_data::<DurationMillisecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Microsecond) => Self::scatter_primitive_data::<DurationMicrosecondType>(data, indices, nums),
            DataType::Duration(TimeUnit::Nanosecond) => Self::scatter_primitive_data::<DurationNanosecondType>(data, indices, nums),
            DataType::Interval(IntervalUnit::YearMonth) => Self::scatter_primitive_data::<IntervalYearMonthType>(data, indices, nums),
            DataType::Interval(IntervalUnit::DayTime) => Self::scatter_primitive_data::<IntervalDayTimeType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Second, _) => Self::scatter_primitive_data::<TimestampSecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::scatter_primitive_data::<TimestampMillisecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::scatter_primitive_data::<TimestampMicrosecondType>(data, indices, nums),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::scatter_primitive_data::<TimestampNanosecondType>(data, indices, nums),
            // DataType::Binary => {},
            // DataType::FixedSizeBinary(i32) => {},
            // DataType::LargeBinary => {},
            // DataType::Utf8 => {},
            // DataType::LargeUtf8 => {},
            // DataType::List(Box < Field>) => {},
            // DataType::FixedSizeList(Box<Field>, i32) => {},
            // DataType::LargeList(Box<Field>) => {},
            // DataType::Struct(Vec<Field>) => {},
            // DataType::Union(Vec<Field>) => {},
            // DataType::Dictionary(Box<DataType>, Box<DataType>) => {},
            // DataType::Decimal(usize, usize) => {},
            _ => Result::Err(ErrorCodes::BadDataValueType("".to_string()))
        }
    }

    #[inline]
    fn scatter_primitive_data<T: ArrowPrimitiveType>(data: &DataArrayRef, indices: &[u64], scattered_size: usize) -> Result<Vec<DataArrayRef>> {
        let primitive_data = data
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ErrorCodes::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(PrimitiveArray<T>)
                ))
            })?;

        let primitive_data_slice = primitive_data.values();
        let mut scattered_data_builder = Self::create_scatter_builders::<T>(scattered_size, indices.len());

        for index in 0..primitive_data_slice.len() {
            scattered_data_builder[indices[index] as usize].append(primitive_data_slice[index]);
        }

        let mut scattered_null_bit = vec![];
        if data.null_count() > 0 {
            let mut scattered_null_bit_builders = vec![];
            for index in 0..scattered_size {
                scattered_null_bit_builders.push(BooleanBufferBuilder::new(scattered_data_builder[index].len()))
            }

            for index in 0..primitive_data_slice.len() {
                scattered_null_bit_builders[indices[index] as usize].append(!data.is_null(index));
            }

            for index in 0..scattered_size {
                scattered_null_bit.push(scattered_null_bit_builders[index].finish());
            }
        }

        let mut scattered_data_res: Vec<ArrayRef> = vec![];
        for index in 0..scattered_size {
            let mut builder = ArrayData::builder(T::DATA_TYPE)
                .len(scattered_data_builder[index].len())
                .add_buffer(scattered_data_builder[index].finish());

            match data.null_count() {
                0 => scattered_data_res.push(Arc::new(PrimitiveArray::<T>::from(builder.build()))),
                _ => {
                    // We always remove the first element, which is similar to pop_first
                    builder = builder.null_bit_buffer(scattered_null_bit.remove(0));
                    scattered_data_res.push(Arc::new(PrimitiveArray::<T>::from(builder.build())));
                }
            }
        }

        Ok(scattered_data_res)
    }

    #[inline]
    fn create_scatter_builders<T: ArrowPrimitiveType>(
        scattered_size: usize,
        scatter_data_len: usize) -> Vec<BufferBuilder<T::Native>> {
        let guess_scattered_len = ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size).map(|_| {
            BufferBuilder::<T::Native>::new(guess_scattered_len)
        }).collect::<Vec<_>>()
    }

    #[inline]
    fn indices_values(indices: &DataArrayRef) -> Result<&[u64]> {
        Ok(downcast_array!(indices, UInt64Array)?.values())
    }
}
