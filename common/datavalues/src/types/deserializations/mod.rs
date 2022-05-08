// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_io::prelude::*;
use enum_dispatch::enum_dispatch;
use serde_json::Value;

use crate::prelude::*;

mod array;
mod boolean;
mod date;
mod null;
mod nullable;
mod number;
mod string;
mod timestamp;
mod variant;

pub use array::*;
pub use boolean::*;
pub use date::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use timestamp::*;
pub use variant::*;

#[enum_dispatch]
pub trait TypeDeserializer: Send + Sync {
    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()>;

    fn de_default(&mut self, format: &FormatSettings);

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

    fn de_whole_text(&mut self, reader: &[u8], format: &FormatSettings) -> Result<()>;

    fn de_text<R: BufferRead>(
        &mut self,
        reader: &mut CheckpointReader<R>,
        format: &FormatSettings,
    ) -> Result<()>;

    fn de_text_csv<R: BufferRead>(
        &mut self,
        reader: &mut CheckpointReader<R>,
        format: &FormatSettings,
    ) -> Result<()> {
        self.de_text(reader, format)
    }

    fn de_text_json<R: BufferRead>(
        &mut self,
        reader: &mut CheckpointReader<R>,
        format: &FormatSettings,
    ) -> Result<()> {
        self.de_text(reader, format)
    }

    fn de_text_quoted<R: BufferRead>(
        &mut self,
        reader: &mut CheckpointReader<R>,
        format: &FormatSettings,
    ) -> Result<()> {
        self.de_text(reader, format)
    }

    fn append_data_value(&mut self, value: DataValue, format: &FormatSettings) -> Result<()>;

    /// Note this method will return err only when inner builder is empty.
    fn pop_data_value(&mut self) -> Result<DataValue>;

    fn finish_to_column(&mut self) -> ColumnRef;
}

#[enum_dispatch(TypeDeserializer)]
pub enum TypeDeserializerImpl {
    Null(NullDeserializer),
    Nullable(NullableDeserializer),
    Array(ArrayDeserializer),
    Boolean(BooleanDeserializer),
    Int8(NumberDeserializer<i8>),
    Int16(NumberDeserializer<i16>),
    Int32(NumberDeserializer<i32>),
    Int64(NumberDeserializer<i64>),
    UInt8(NumberDeserializer<u8>),
    UInt16(NumberDeserializer<u16>),
    UInt32(NumberDeserializer<u32>),
    UInt64(NumberDeserializer<u64>),
    Float32(NumberDeserializer<f32>),
    Float64(NumberDeserializer<f64>),

    Date(DateDeserializer<i32>),
    Interval(DateDeserializer<i64>),
    Timestamp(TimestampDeserializer),
    String(StringDeserializer),
    // TODO
    // Array(ArrayDeserializer),
    // Struct(StructDeserializer),
    Variant(VariantDeserializer),
}
