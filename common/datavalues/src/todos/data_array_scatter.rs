// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayData;
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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataColumnarValue;
use crate::DataValue;
use crate::Series;

pub struct DataArrayScatter;

impl DataArrayScatter {
    #[inline]
    pub fn scatter(
        data: &DataColumnarValue,
        indices: &DataColumnarValue,
        nums: usize,
    ) -> Result<Vec<DataColumnarValue>> {
        if data.len() != indices.len() {
            return Result::Err(ErrorCode::BadDataArrayLength(format!(
                "Selector requires data and indices to have the same number of arrays. data has {}, indices has {}.",
                data.len(),
                indices.len()
            )));
        }

        match (data, indices) {
            (DataColumnarValue::Array(data), DataColumnarValue::Array(indices)) => {
                let indices_array_values = Self::indices_values(indices)?;
                Self::scatter_data(data, indices_array_values, nums)
            }
            (DataColumnarValue::Constant(_, _), DataColumnarValue::Array(indices)) => {
                let indices_array_values = Self::indices_values(indices)?;
                Self::scatter_data(&data.to_array()?, indices_array_values, nums)
            }
            (DataColumnarValue::Array(_), DataColumnarValue::Constant(indices, _)) => {
                Self::scatter_data_with_constant_indices(data, indices, nums)
            }
            (DataColumnarValue::Constant(_, _), DataColumnarValue::Constant(indices, _)) => {
                Self::scatter_data_with_constant_indices(data, indices, nums)
            }
        }
    }

    fn scatter_data_with_constant_indices(
        data: &DataColumnarValue,
        indices: &DataValue,
        nums: usize,
    ) -> Result<Vec<DataColumnarValue>> {
        let scatter_data = |index: usize| -> Result<Vec<DataColumnarValue>> {
            if index >= nums {
                return Err(ErrorCode::LogicalError(format!(
                    "Logical error: the indices [{}] value greater than scatters size",
                    index
                )));
            }

            let mut scattered_data_res = Vec::with_capacity(nums);

            for res_index in 0..nums {
                if res_index == index {
                    scattered_data_res.push(data.clone());
                } else {
                    scattered_data_res.push(data.clone_empty());
                }
            }

            Ok(scattered_data_res)
        };

        let v = match indices {
            DataValue::Int8(Some(v)) => *v as usize,
            DataValue::Int16(Some(v)) => *v as usize,
            DataValue::Int32(Some(v)) => *v as usize,
            DataValue::Int64(Some(v)) => *v as usize,
            DataValue::UInt8(Some(v)) => *v as usize,
            DataValue::UInt16(Some(v)) => *v as usize,
            DataValue::UInt32(Some(v)) => *v as usize,
            DataValue::UInt64(Some(v)) => *v as usize,
            _ => return Err(ErrorCode::BadDataValueType("")),
        };

        scatter_data(v)
    }

    fn scatter_data(data: &Series, indices: &[u64], nums: usize) -> Result<Vec<DataColumnarValue>> {
        match data.data_type() {
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
            _ => Result::Err(ErrorCode::BadDataValueType(format!(
                "DataType:{:?} does not implement scatter",
                stringify!(PrimitiveArray<T>)
            ))),
        }
    }

    #[inline]
    fn scatter_primitive_data<T: ArrowPrimitiveType>(
        data: &Series,
        indices: &[u64],
        scattered_size: usize,
    ) -> Result<Vec<DataColumnarValue>> {
        let primitive_data = data
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ErrorCode::BadDataValueType(format!(
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

        let mut scattered_data_res: Vec<DataColumnarValue> = Vec::with_capacity(scattered_size);
        for index in 0..scattered_size {
            // We don't care about time zones, which are always bound to the schema
            let mut builder = ArrayData::builder(T::DATA_TYPE)
                .len(scattered_data_builder[index].len())
                .add_buffer(scattered_data_builder[index].finish());

            match data.null_count() {
                0 => scattered_data_res.push(DataColumnarValue::Array(Arc::new(
                    PrimitiveArray::<T>::from(builder.build()),
                ))),
                _ => {
                    builder = builder.null_bit_buffer(scattered_null_bit[index].clone());
                    scattered_data_res.push(DataColumnarValue::Array(Arc::new(
                        PrimitiveArray::<T>::from(builder.build()),
                    )));
                }
            }
        }

        Ok(scattered_data_res)
    }

    fn scatter_binary_data(
        data: &Series,
        indices: &[u64],
        scattered_size: usize,
    ) -> Result<Vec<DataColumnarValue>> {
        let binary_data = data.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
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

        let mut scattered_data_res = Vec::with_capacity(scattered_data_builder.len());
        for mut builder in scattered_data_builder {
            scattered_data_res.push(DataColumnarValue::Array(Arc::new(builder.finish())));
        }

        Ok(scattered_data_res)
    }

    fn scatter_large_binary_data(
        data: &Series,
        indices: &[u64],
        scattered_size: usize,
    ) -> Result<Vec<DataColumnarValue>> {
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

        let mut scattered_data_res = Vec::with_capacity(scattered_data_builder.len());
        for mut builder in scattered_data_builder {
            scattered_data_res.push(DataColumnarValue::Array(Arc::new(builder.finish())));
        }

        Ok(scattered_data_res)
    }

    fn scatter_string_data<T: StringOffsetSizeTrait>(
        data: &Series,
        indices: &[u64],
        scattered_size: usize,
    ) -> Result<Vec<DataColumnarValue>> {
        let binary_data = data
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                ErrorCode::BadDataValueType(format!(
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

        let mut scattered_data_res = Vec::with_capacity(scattered_data_builder.len());
        for mut builder in scattered_data_builder {
            scattered_data_res.push(DataColumnarValue::Array(Arc::new(builder.finish())));
        }

        Ok(scattered_data_res)
    }

    #[inline]
    fn create_primitive_builders<T: ArrowPrimitiveType>(
        scattered_size: usize,
        scatter_data_len: usize,
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
        scatter_data_len: usize,
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
        scatter_data_len: usize,
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
        scatter_data_len: usize,
    ) -> Vec<GenericStringBuilder<T>> {
        let guess_scattered_len =
            ((scatter_data_len as f64) * 1.1 / (scattered_size as f64)) as usize;
        (0..scattered_size)
            .map(|_| GenericStringBuilder::<T>::new(guess_scattered_len))
            .collect::<Vec<_>>()
    }

    #[inline]
    fn indices_values(indices: &Series) -> Result<&[u64]> {
        Ok(downcast_array!(indices, UInt64Array)?.values())
    }
}
