// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayData;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::array::BinaryBuilder;
use common_arrow::arrow::array::BooleanBufferBuilder;
use common_arrow::arrow::array::BufferBuilder;
use common_arrow::arrow::array::GenericStringArray;
use common_arrow::arrow::array::GenericStringBuilder;
use common_arrow::arrow::array::LargeBinaryArray;
use common_arrow::arrow::array::LargeBinaryBuilder;
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
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::DataArrayRef;

pub struct DataArrayScatter;

impl DataArrayScatter {
    #[inline]
    pub fn scatter(
        data: &DataArrayRef,
        indices: &DataArrayRef,
        nums: usize
    ) -> Result<Vec<DataArrayRef>> {
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

    fn scatter_data(
        data: &DataArrayRef,
        indices: &[u64],
        nums: usize
    ) -> Result<Vec<DataArrayRef>> {
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
            DataType::Time32(TimeUnit::Second) => {
                Self::scatter_primitive_data::<Time32SecondType>(data, indices, nums)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Self::scatter_primitive_data::<Time32MillisecondType>(data, indices, nums)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Self::scatter_primitive_data::<Time64MicrosecondType>(data, indices, nums)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::scatter_primitive_data::<Time64NanosecondType>(data, indices, nums)
            }
            DataType::Duration(TimeUnit::Second) => {
                Self::scatter_primitive_data::<DurationSecondType>(data, indices, nums)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Self::scatter_primitive_data::<DurationMillisecondType>(data, indices, nums)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::scatter_primitive_data::<DurationMicrosecondType>(data, indices, nums)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Self::scatter_primitive_data::<DurationNanosecondType>(data, indices, nums)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::scatter_primitive_data::<IntervalYearMonthType>(data, indices, nums)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Self::scatter_primitive_data::<IntervalDayTimeType>(data, indices, nums)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Self::scatter_primitive_data::<TimestampSecondType>(data, indices, nums)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Self::scatter_primitive_data::<TimestampMillisecondType>(data, indices, nums)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::scatter_primitive_data::<TimestampMicrosecondType>(data, indices, nums)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::scatter_primitive_data::<TimestampNanosecondType>(data, indices, nums)
            }
            DataType::Binary => Self::scatter_binary_data(data, indices, nums),
            DataType::LargeBinary => Self::scatter_large_binary_data(data, indices, nums),
            DataType::Utf8 => Self::scatter_string_data::<i32>(data, indices, nums),
            DataType::LargeUtf8 => Self::scatter_string_data::<i64>(data, indices, nums),
            // DataType::Decimal(_, _) => {},
            // DataType::FixedSizeBinary(i32) => {},
            // DataType::List(Box < Field>) => {},
            // DataType::FixedSizeList(Box<Field>, i32) => {},
            // DataType::LargeList(Box<Field>) => {},
            // DataType::Struct(Vec<Field>) => {},
            // DataType::Union(Vec<Field>) => {},
            // DataType::Dictionary(Box<DataType>, Box<DataType>) => {},
            _ => Result::Err(ErrorCodes::BadDataValueType(format!(
                "DataType:{:?} does not implement scatter",
                stringify!(PrimitiveArray<T>)
            )))
        }
    }

    #[inline]
    fn scatter_primitive_data<T: ArrowPrimitiveType>(
        data: &DataArrayRef,
        indices: &[u64],
        scattered_size: usize
    ) -> Result<Vec<DataArrayRef>> {
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
        let mut scattered_data_builder =
            Self::create_primitive_builders::<T>(scattered_size, indices.len());

        for index in 0..primitive_data_slice.len() {
            scattered_data_builder[indices[index] as usize].append(primitive_data_slice[index]);
        }

        let mut scattered_null_bit = vec![];
        if data.null_count() > 0 {
            let mut scattered_null_bit_builders = vec![];

            for builder in &scattered_data_builder {
                scattered_null_bit_builders.push(BooleanBufferBuilder::new(builder.len()))
            }

            for index in 0..primitive_data_slice.len() {
                scattered_null_bit_builders[indices[index] as usize].append(!data.is_null(index));
            }

            for mut builder in scattered_null_bit_builders {
                scattered_null_bit.push(builder.finish());
            }
        }

        let mut scattered_data_res: Vec<ArrayRef> = vec![];
        for index in 0..scattered_size {
            // We don't care about time zones, which are always bound to the schema
            let mut builder = ArrayData::builder(T::DATA_TYPE)
                .len(scattered_data_builder[index].len())
                .add_buffer(scattered_data_builder[index].finish());

            match data.null_count() {
                0 => scattered_data_res.push(Arc::new(PrimitiveArray::<T>::from(builder.build()))),
                _ => {
                    builder = builder.null_bit_buffer(scattered_null_bit[index].clone());
                    scattered_data_res.push(Arc::new(PrimitiveArray::<T>::from(builder.build())));
                }
            }
        }

        Ok(scattered_data_res)
    }

    fn scatter_binary_data(
        data: &DataArrayRef,
        indices: &[u64],
        scattered_size: usize
    ) -> Result<Vec<ArrayRef>> {
        let binary_data = data.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            ErrorCodes::BadDataValueType(format!(
                "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                data.data_type(),
                stringify!(BinaryArray)
            ))
        })?;

        let value_size = binary_data.value_data().len();
        let mut scattered_data_builder = Self::create_binary_builders(scattered_size, value_size);

        for index in 0..binary_data.len() {
            if !binary_data.is_null(index) {
                scattered_data_builder[indices[index] as usize]
                    .append_value(binary_data.value(index))?;
            } else {
                scattered_data_builder[indices[index] as usize].append_null()?;
            }
        }

        let mut scattered_data_res: Vec<ArrayRef> = vec![];
        for mut builder in scattered_data_builder {
            scattered_data_res.push(Arc::new(builder.finish()));
        }

        Ok(scattered_data_res)
    }

    fn scatter_large_binary_data(
        data: &DataArrayRef,
        indices: &[u64],
        scattered_size: usize
    ) -> Result<Vec<ArrayRef>> {
        let binary_data = data
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                ErrorCodes::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(LargeBinaryArray)
                ))
            })?;

        let value_size = binary_data.value_data().len();
        let mut scattered_data_builder =
            Self::create_large_binary_builders(scattered_size, value_size);

        for index in 0..binary_data.len() {
            if !binary_data.is_null(index) {
                scattered_data_builder[indices[index] as usize]
                    .append_value(binary_data.value(index))?;
            } else {
                scattered_data_builder[indices[index] as usize].append_null()?;
            }
        }

        let mut scattered_data_res: Vec<ArrayRef> = vec![];
        for mut builder in scattered_data_builder {
            scattered_data_res.push(Arc::new(builder.finish()));
        }

        Ok(scattered_data_res)
    }

    fn scatter_string_data<T: StringOffsetSizeTrait>(
        data: &DataArrayRef,
        indices: &[u64],
        scattered_size: usize
    ) -> Result<Vec<ArrayRef>> {
        let binary_data = data
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                ErrorCodes::BadDataValueType(format!(
                    "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                    data.data_type(),
                    stringify!(GenericStringArray<T>)
                ))
            })?;

        let value_size = binary_data.value_data().len();
        let mut scattered_data_builder =
            Self::create_string_builders::<T>(scattered_size, value_size);

        for index in 0..binary_data.len() {
            if !binary_data.is_null(index) {
                scattered_data_builder[indices[index] as usize]
                    .append_value(binary_data.value(index))?;
            } else {
                scattered_data_builder[indices[index] as usize].append_null()?;
            }
        }

        let mut scattered_data_res: Vec<ArrayRef> = vec![];
        for mut builder in scattered_data_builder {
            scattered_data_res.push(Arc::new(builder.finish()));
        }

        Ok(scattered_data_res)
    }

    #[inline]
    fn create_primitive_builders<T: ArrowPrimitiveType>(
        scattered_size: usize,
        scatter_data_len: usize
    ) -> Vec<BufferBuilder<T::Native>> {
        let guess_scattered_len =
            ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size)
            .map(|_| BufferBuilder::<T::Native>::new(guess_scattered_len))
            .collect::<Vec<_>>()
    }

    #[inline]
    fn create_binary_builders(
        scattered_size: usize,
        scatter_data_len: usize
    ) -> Vec<BinaryBuilder> {
        let guess_scattered_len =
            ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size)
            .map(|_| BinaryBuilder::new(guess_scattered_len))
            .collect::<Vec<_>>()
    }

    #[inline]
    fn create_large_binary_builders(
        scattered_size: usize,
        scatter_data_len: usize
    ) -> Vec<LargeBinaryBuilder> {
        let guess_scattered_len =
            ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size)
            .map(|_| LargeBinaryBuilder::new(guess_scattered_len))
            .collect::<Vec<_>>()
    }

    #[inline]
    fn create_string_builders<T: StringOffsetSizeTrait>(
        scattered_size: usize,
        scatter_data_len: usize
    ) -> Vec<GenericStringBuilder<T>> {
        let guess_scattered_len =
            ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size)
            .map(|_| GenericStringBuilder::<T>::new(guess_scattered_len))
            .collect::<Vec<_>>()
    }

    #[inline]
    fn indices_values(indices: &DataArrayRef) -> Result<&[u64]> {
        Ok(downcast_array!(indices, UInt64Array)?.values())
    }
}
