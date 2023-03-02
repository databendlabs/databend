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
