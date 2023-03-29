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
use arrow_array::LargeBinaryArray;
use arrow_array::NullArray;
use arrow_array::PrimitiveArray;
use arrow_buffer::buffer::BooleanBuffer;
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::TimeUnit;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer as Buffer2;
use common_arrow::arrow::types::NativeType;
use ordered_float::OrderedFloat;

use crate::types::decimal::DecimalColumn;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::F32;
use crate::types::F64;
use crate::Column;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

fn numbers_into<TN: ArrowNativeType + NativeType, TA: ArrowPrimitiveType>(
    buf: Buffer2<TN>,
    data_type: DataType,
) -> Result<Arc<dyn Array>, ArrowError> {
    let len = buf.len();
    let buf = Buffer::from(buf);
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
                let len = bitmap.len();
                let null_buffer = NullBuffer::from(bitmap);
                let array_data = ArrayData::builder(DataType::Boolean)
                    .len(len)
                    .add_buffer(null_buffer.buffer().clone())
                    .build()?;
                Arc::new(BooleanArray::from(array_data))
            }
            Column::String(col) => {
                let len = col.len();
                let values = Buffer::from(col.data);
                let offsets = Buffer::from(col.offsets);
                let array_data = ArrayData::builder(DataType::LargeBinary)
                    .len(len)
                    .add_buffer(offsets)
                    .add_buffer(values);
                let array_data = unsafe { array_data.build_unchecked() };
                Arc::new(LargeBinaryArray::from(array_data))
            }
            Column::Variant(col) => {
                let len = col.len();
                let values = Buffer::from(col.data);
                let offsets = Buffer::from(col.offsets);
                let array_data = ArrayData::builder(DataType::LargeBinary)
                    .len(len)
                    .add_buffer(offsets)
                    .add_buffer(values);
                let array_data = unsafe { array_data.build_unchecked() };
                Arc::new(LargeBinaryArray::from(array_data))
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
                // todo(youngsofun): arrow_rs use u128 for lo while arrow2 use i128, recheck it later.
                let buf = unsafe {
                    std::mem::transmute::<_, Buffer2<common_arrow::arrow::types::i256>>(buf)
                };
                let len = buf.len();
                let buf = Buffer::from(buf);
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
                let null_buffer = NullBuffer::from(col.validity);
                let inner = col.column.into_arrow_rs()?;
                let builder = ArrayDataBuilder::from(inner.data().clone());

                let data = builder
                    .null_bit_buffer(Some(null_buffer.buffer().clone()))
                    .build()?;
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

    pub fn from_arrow_rs(array: Arc<dyn Array>, field: &Field) -> Result<Self, ArrowError> {
        if let Some(extent) = field.metadata().get(EXTENSION_KEY).map(|v| v.as_str()) {
            match extent {
                ARROW_EXT_TYPE_EMPTY_ARRAY => return Ok(Column::EmptyArray { len: array.len() }),
                ARROW_EXT_TYPE_EMPTY_MAP => return Ok(Column::EmptyMap { len: array.len() }),
                _ => {}
            }
        }

        let data_type = array.data_type();
        let data = array.data_ref();
        let column = match data_type {
            DataType::Null => Column::Null { len: array.len() },
            DataType::LargeUtf8 => {
                let offsets =
                    unsafe { std::mem::transmute::<_, Buffer2<u64>>(data.buffers()[0].clone()) };
                let values =
                    unsafe { std::mem::transmute::<_, Buffer2<u8>>(data.buffers()[1].clone()) };

                Column::Variant(StringColumn {
                    offsets,
                    data: values,
                })
            }

            DataType::LargeBinary => {
                let offsets =
                    unsafe { std::mem::transmute::<_, Buffer2<u64>>(data.buffers()[0].clone()) };
                let values =
                    unsafe { std::mem::transmute::<_, Buffer2<u8>>(data.buffers()[1].clone()) };

                match field.metadata().get(EXTENSION_KEY).map(|v| v.as_str()) {
                    Some(ARROW_EXT_TYPE_VARIANT) => Column::Variant(StringColumn {
                        offsets,
                        data: values,
                    }),
                    _ => Column::String(StringColumn {
                        offsets,
                        data: values,
                    }),
                }
            }

            DataType::Utf8 => {
                let offsets =
                    unsafe { std::mem::transmute::<_, Buffer2<i32>>(data.buffers()[0].clone()) };
                let offsets: Vec<u64> = offsets.iter().map(|x| *x as u64).collect::<Vec<_>>();
                let values =
                    unsafe { std::mem::transmute::<_, Buffer2<u8>>(data.buffers()[1].clone()) };

                Column::String(StringColumn {
                    offsets: offsets.into(),
                    data: values,
                })
            }

            DataType::Binary => {
                let offsets =
                    unsafe { std::mem::transmute::<_, Buffer2<i32>>(data.buffers()[0].clone()) };
                let offsets: Vec<u64> = offsets.iter().map(|x| *x as u64).collect::<Vec<_>>();
                let values =
                    unsafe { std::mem::transmute::<_, Buffer2<u8>>(data.buffers()[1].clone()) };

                Column::String(StringColumn {
                    offsets: offsets.into(),
                    data: values,
                })
            }
            DataType::Boolean => {
                let boolean_buffer =
                    BooleanBuffer::new(data.buffers()[0].clone(), data.offset(), data.len());

                let null_buffer = NullBuffer::new(boolean_buffer);

                let bitmap = Bitmap::from_null_buffer(null_buffer);
                Column::Boolean(bitmap)
            }
            DataType::Int8 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::Int8(buffer2))
            }
            DataType::UInt8 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::UInt8(buffer2))
            }
            DataType::Int16 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::Int16(buffer2))
            }
            DataType::UInt16 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::UInt16(buffer2))
            }
            DataType::Int32 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::Int32(buffer2))
            }
            DataType::UInt32 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::UInt32(buffer2))
            }
            DataType::Int64 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::Int64(buffer2))
            }
            DataType::UInt64 => {
                let buffer2 = Buffer2::from(data.buffers()[0].clone());
                Column::Number(NumberColumn::UInt64(buffer2))
            }

            DataType::Float32 => {
                let buffer2: Buffer2<f32> = Buffer2::from(data.buffers()[0].clone());
                let buffer = unsafe { std::mem::transmute::<_, Buffer2<F32>>(buffer2) };
                Column::Number(NumberColumn::Float32(buffer))
            }

            DataType::Float64 => {
                let buffer2: Buffer2<f32> = Buffer2::from(data.buffers()[0].clone());
                let buffer = unsafe { std::mem::transmute::<_, Buffer2<F64>>(buffer2) };

                Column::Number(NumberColumn::Float64(buffer))
            }

            _ => Err(ArrowError::NotYetImplemented(format!(
                "Column::from_arrow_rs() for {data_type} not implemented yet"
            )))?,
        };
        if let Some(nulls) = array.into_data().nulls() {
            let validity = Bitmap::from_null_buffer(nulls.clone());
            let column = NullableColumn { column, validity };
            Ok(Column::Nullable(Box::new(column)))
        } else {
            Ok(column)
        }
    }
}
