// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_io::prelude::*;

mod array;
mod boolean;
mod date;
mod decimal;
mod map;
mod null;
mod nullable;
mod number;
mod string;
mod timestamp;
mod tuple;
mod variant;

pub use array::*;
pub use boolean::*;
use common_exception::Result;
pub use date::*;
pub use decimal::*;
use enum_dispatch::enum_dispatch;
use ethnum::i256;
pub use map::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
use serde_json::Value;
pub use string::*;
pub use timestamp::*;
pub use tuple::*;
pub use variant::*;

use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::string::StringColumnBuilder;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::Column;
use crate::Scalar;

#[enum_dispatch]
pub trait TypeDeserializer: Send + Sync {
    fn memory_size(&self) -> usize;
    fn len(&self) -> usize;

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()>;

    fn de_default(&mut self);

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        format: &FormatSettings,
    ) -> Result<()>;

    fn de_json(&mut self, reader: &Value, format: &FormatSettings) -> Result<()>;

    fn de_null(&mut self, _format: &FormatSettings) -> bool {
        false
    }

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<()>;

    /// Note this method will return err only when inner builder is empty.
    fn pop_data_value(&mut self) -> Result<Scalar>;

    fn finish_to_column(&mut self) -> Column;
}

#[enum_dispatch(TypeDeserializer)]
pub enum TypeDeserializerImpl {
    Null(NullDeserializer),
    Nullable(NullableDeserializer),
    Array(ArrayDeserializer),
    Map(MapDeserializer),
    Boolean(BooleanDeserializer),
    Int8(NumberDeserializer<i8, i8>),
    Int16(NumberDeserializer<i16, i16>),
    Int32(NumberDeserializer<i32, i32>),
    Int64(NumberDeserializer<i64, i64>),
    UInt8(NumberDeserializer<u8, u8>),
    UInt16(NumberDeserializer<u16, u16>),
    UInt32(NumberDeserializer<u32, u32>),
    UInt64(NumberDeserializer<u64, u64>),
    Float32(NumberDeserializer<F32, f32>),
    Float64(NumberDeserializer<F64, f64>),
    Decimal128(DecimalDeserializer<i128>),
    Decimal256(DecimalDeserializer<i256>),

    Date(DateDeserializer),
    Timestamp(TimestampDeserializer),
    String(StringDeserializer),
    Struct(StructDeserializer),
    Variant(VariantDeserializer),
}

impl TypeDeserializerImpl {
    pub fn with_capacity(ty: &DataType, capacity: usize) -> TypeDeserializerImpl {
        match ty {
            DataType::Null => 0.into(),
            DataType::Boolean => MutableBitmap::with_capacity(capacity).into(),
            DataType::String => StringColumnBuilder::with_capacity(capacity, capacity * 4).into(),
            DataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8 => {
                    NumberDeserializer::<u8, u8>::with_capacity(capacity).into()
                }
                NumberDataType::UInt16 => {
                    NumberDeserializer::<u16, u16>::with_capacity(capacity).into()
                }
                NumberDataType::UInt32 => {
                    NumberDeserializer::<u32, u32>::with_capacity(capacity).into()
                }
                NumberDataType::UInt64 => {
                    NumberDeserializer::<u64, u64>::with_capacity(capacity).into()
                }
                NumberDataType::Int8 => {
                    NumberDeserializer::<i8, i8>::with_capacity(capacity).into()
                }
                NumberDataType::Int16 => {
                    NumberDeserializer::<i16, i16>::with_capacity(capacity).into()
                }
                NumberDataType::Int32 => {
                    NumberDeserializer::<i32, i32>::with_capacity(capacity).into()
                }
                NumberDataType::Int64 => {
                    NumberDeserializer::<i64, i64>::with_capacity(capacity).into()
                }
                NumberDataType::Float32 => {
                    NumberDeserializer::<F32, f32>::with_capacity(capacity).into()
                }
                NumberDataType::Float64 => {
                    NumberDeserializer::<F64, f64>::with_capacity(capacity).into()
                }
            },
            DataType::Date => DateDeserializer::with_capacity(capacity).into(),
            DataType::Timestamp => TimestampDeserializer::with_capacity(capacity).into(),
            DataType::Nullable(inner_ty) => {
                NullableDeserializer::with_capacity(capacity, inner_ty.as_ref()).into()
            }
            DataType::Variant => VariantDeserializer::with_capacity(capacity).into(),
            DataType::Array(ty) => ArrayDeserializer::with_capacity(capacity, ty).into(),
            DataType::Map(ty) => MapDeserializer::with_capacity(capacity, ty).into(),
            DataType::Tuple(types) => TupleDeserializer::with_capacity(capacity, types).into(),
            DataType::Decimal(types) => match types {
                DecimalDataType::Decimal128(_) => {
                    DecimalDeserializer::<i128>::with_capacity(types, capacity).into()
                }
                DecimalDataType::Decimal256(_) => {
                    DecimalDeserializer::<i256>::with_capacity(types, capacity).into()
                }
            },
            _ => unimplemented!(),
        }
    }
}
