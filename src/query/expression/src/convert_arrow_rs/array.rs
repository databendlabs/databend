// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::make_array;
use arrow_array::Array;
use arrow_array::ArrowPrimitiveType;
use arrow_array::BooleanArray;
use arrow_array::Int16Array;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::Int8Array;
use arrow_array::LargeStringArray;
use arrow_array::NullArray;
use arrow_array::PrimitiveArray;
use arrow_array::StringArray;
use arrow_array::UInt16Array;
use arrow_array::UInt32Array;
use arrow_array::UInt64Array;
use arrow_array::UInt8Array;
use arrow_buffer::i256;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer as Buffer2;
use common_arrow::arrow::Either;
use ordered_float::OrderedFloat;

use crate::types::decimal::DecimalColumn;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::Column;

fn try_take_buffer<T: Clone>(buffer: Buffer2<T>) -> Vec<T> {
    // currently need a copy if buffer has more then 1 reference
    match buffer.into_mut() {
        Either::Left(b) => b.as_slice().to_vec(),
        Either::Right(v) => v,
    }
}

fn numbers_into<TN: ArrowNativeType, TA: ArrowPrimitiveType>(
    buf: Buffer2<TN>,
    data_type: DataType,
) -> Result<Arc<dyn Array>, ArrowError> {
    let v: Vec<TN> = try_take_buffer(buf);
    let len = v.len();
    let buf = Buffer::from_vec(v);
    let data = ArrayData::builder(data_type)
        .len(len)
        .offset(0)
        .add_buffer(buf)
        .build()?;
    Ok(Arc::new(PrimitiveArray::<TA>::from(data)))
}

impl Column {
    pub fn into_arrow_rs(self) -> Result<Arc<dyn Array>, ArrowError> {
        let array: Arc<dyn Array> = match self {
            Column::Null { len } => Arc::new(NullArray::new(len)),
            Column::EmptyArray { len } => Arc::new(NullArray::new(len)),
            Column::EmptyMap { len } => Arc::new(NullArray::new(len)),
            Column::Boolean(bitmap) => {
                let buf = bitmap.as_slice().0;
                let array_data = ArrayData::builder(DataType::Boolean)
                    .len(bitmap.len())
                    .add_buffer(buf.into())
                    .build()?;
                Arc::new(BooleanArray::from(array_data))
            }
            Column::String(col) => {
                let len = col.len();
                let values: Vec<u8> = try_take_buffer(col.data);
                let offsets: Vec<u64> = try_take_buffer(col.offsets);
                let offsets = Buffer::from_vec(offsets);
                let array_data = ArrayData::builder(DataType::LargeUtf8)
                    .len(len)
                    .add_buffer(offsets)
                    .add_buffer(values.into());
                let array_data = unsafe { array_data.build_unchecked() };
                Arc::new(LargeStringArray::from(array_data))
            }
            Column::Number(NumberColumn::UInt8(buf)) => {
                numbers_into::<u8, arrow_array::types::UInt8Type>(buf, DataType::UInt8)?
            }
            Column::Number(NumberColumn::UInt16(buf)) => {
                numbers_into::<u16, arrow_array::types::UInt16Type>(buf, DataType::UInt16)?
            }
            Column::Number(NumberColumn::UInt32(buf)) => {
                numbers_into::<u32, arrow_array::types::UInt32Type>(buf, DataType::UInt32)?
            }
            Column::Number(NumberColumn::UInt64(buf)) => {
                numbers_into::<u64, arrow_array::types::UInt64Type>(buf, DataType::UInt64)?
            }
            Column::Number(NumberColumn::Int8(buf)) => {
                numbers_into::<i8, arrow_array::types::Int8Type>(buf, DataType::Int8)?
            }
            Column::Number(NumberColumn::Int16(buf)) => {
                numbers_into::<i16, arrow_array::types::Int16Type>(buf, DataType::Int16)?
            }
            Column::Number(NumberColumn::Int32(buf)) => {
                numbers_into::<i32, arrow_array::types::Int32Type>(buf, DataType::Int32)?
            }
            Column::Number(NumberColumn::Int64(buf)) => {
                numbers_into::<i64, arrow_array::types::Int64Type>(buf, DataType::Int64)?
            }
            Column::Number(NumberColumn::Float32(buf)) => {
                let buf =
                    unsafe { std::mem::transmute::<Buffer2<OrderedFloat<f32>>, Buffer2<f32>>(buf) };
                numbers_into::<f32, arrow_array::types::Float32Type>(buf, DataType::Float32)?
            }
            Column::Number(NumberColumn::Float64(buf)) => {
                let buf =
                    unsafe { std::mem::transmute::<Buffer2<OrderedFloat<f64>>, Buffer2<f64>>(buf) };
                numbers_into::<f64, arrow_array::types::Float64Type>(buf, DataType::Float64)?
            }
            Column::Decimal(DecimalColumn::Decimal128(buf, size)) => {
                numbers_into::<i128, arrow_array::types::Decimal128Type>(
                    buf,
                    DataType::Decimal128(size.precision, size.scale as i8),
                )?
            }
            Column::Decimal(DecimalColumn::Decimal256(buf, size)) => {
                let v: Vec<ethnum::i256> = try_take_buffer(buf);
                // todo(youngsofun): arrow_rs use u128 for lo while arrow2 use i128, recheck it later.
                let v: Vec<i256> = v
                    .into_iter()
                    .map(|i| {
                        let (hi, lo) = i.into_words();
                        i256::from_parts(lo as u128, hi)
                    })
                    .collect();
                let len = v.len();
                let buf = Buffer::from_vec(v);
                let data =
                    ArrayData::builder(DataType::Decimal256(size.precision, size.scale as i8))
                        .len(len)
                        .add_buffer(buf)
                        .build()?;
                Arc::new(PrimitiveArray::<arrow_array::types::Decimal256Type>::from(
                    data,
                ))
            }

            Column::Timestamp(buf) => {
                numbers_into::<i64, arrow_array::types::Time64NanosecondType>(
                    buf,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                )?
            }
            Column::Date(buf) => {
                numbers_into::<i32, arrow_array::types::Date32Type>(buf, DataType::Date32)?
            }
            Column::Nullable(col) => {
                let arrow_array = col.column.into_arrow_rs()?;
                let data = arrow_array.into_data();
                let buf = col.validity.as_slice().0;
                let builder = ArrayDataBuilder::from(data);
                // bitmap copied here
                let data = builder.null_bit_buffer(Some(buf.into())).build()?;
                make_array(data)
            }
            _ => {
                let data_type = self.data_type();
                Err(ArrowError::NotYetImplemented(format!(
                    "Column::into_arrow_rs() for {data_type} not implemented yet"
                )))?
            }
        };
        Ok(array)
    }

    pub fn from_arrow_rs(array: Arc<dyn Array>) -> Result<Self, ArrowError> {
        let data_type = array.data_type();
        let column = match data_type {
            DataType::Null => Column::Null { len: array.len() },
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                let offsets = array.value_offsets().to_vec();
                let offsets = unsafe { std::mem::transmute::<Vec<i64>, Vec<u64>>(offsets) };
                Column::String(StringColumn {
                    offsets: offsets.into(),
                    data: array.value_data().to_vec().into(),
                })
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let offsets = array
                    .value_offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String(StringColumn {
                    offsets: offsets.into(),
                    data: array.value_data().to_vec().into(),
                })
            }
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let bytes = array.values().clone().into_vec().map_err(|_| {
                    ArrowError::CastError(
                        "can not covert Buffer of BooleanArray to Vec".to_string(),
                    )
                })?;
                let bitmap = Bitmap::try_new(bytes, array.len()).map_err(|e| {
                    ArrowError::CastError(format!(
                        "can not covert  BooleanArray to Column::Boolean: {e:?}"
                    ))
                })?;
                Column::Boolean(bitmap)
            }
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                let buffer2: Buffer2<i8> = array.values().to_vec().into();
                Column::Number(NumberColumn::Int8(buffer2))
            }
            DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                let buffer2: Buffer2<u8> = array.values().to_vec().into();
                Column::Number(NumberColumn::UInt8(buffer2))
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                let buffer2: Buffer2<i16> = array.values().to_vec().into();
                Column::Number(NumberColumn::Int16(buffer2))
            }
            DataType::UInt16 => {
                let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                let buffer2: Buffer2<u16> = array.values().to_vec().into();
                Column::Number(NumberColumn::UInt16(buffer2))
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let buffer2: Buffer2<i32> = array.values().to_vec().into();
                Column::Number(NumberColumn::Int32(buffer2))
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                let buffer2: Buffer2<u32> = array.values().to_vec().into();
                Column::Number(NumberColumn::UInt32(buffer2))
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let buffer2: Buffer2<i64> = array.values().to_vec().into();
                Column::Number(NumberColumn::Int64(buffer2))
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                let buffer2: Buffer2<u64> = array.values().to_vec().into();
                Column::Number(NumberColumn::UInt64(buffer2))
            }

            _ => Err(ArrowError::NotYetImplemented(format!(
                "Column::from_arrow_rs() for {data_type} not implemented yet"
            )))?,
        };
        if let Some(nulls) = array.into_data().nulls() {
            let validity =
                Bitmap::try_new(nulls.buffer().to_vec(), nulls.offset()).map_err(|e| {
                    ArrowError::CastError(format!(
                        "fail to cast arrow_rs::NullBuffer to arrow2::Bitmap: {e}"
                    ))
                })?;
            let column = NullableColumn { column, validity };
            Ok(Column::Nullable(Box::new(column)))
        } else {
            Ok(column)
        }
    }
}
