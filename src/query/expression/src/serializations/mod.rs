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

mod array;
mod boolean;
mod date;
mod empty_array;
pub mod helper;
mod null;
mod nullable;
mod number;
mod string;
mod timestamp;
mod tuple;
mod variant;

pub use array::ArraySerializer;
pub use boolean::BooleanSerializer;
use common_io::prelude::FormatSettings;
pub use date::DateSerializer;
pub use empty_array::EmptyArraySerializer;
use enum_dispatch::enum_dispatch;
pub use helper::escape::write_escaped_string;
pub use helper::json::write_json_string;
pub use null::NullSerializer;
pub use nullable::NullableSerializer;
pub use number::NumberSerializer;
use serde_json::Value;
pub use string::StringSerializer;
pub use timestamp::TimestampSerializer;
pub use tuple::TupleSerializer;
pub use variant::VariantSerializer;

use crate::types::number::F32;
use crate::types::number::F64;

#[enum_dispatch]
pub trait TypeSerializer: Send + Sync {
    fn need_quote(&self) -> bool {
        false
    }
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings);

    fn write_field_escaped(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _quote: u8,
    ) {
        self.write_field(row_index, buf, format);
    }

    fn write_field_quoted(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: u8,
    ) {
        let need_quote = self.need_quote();
        if need_quote {
            buf.push(quote);
            self.write_field_escaped(row_index, buf, format, quote);
            buf.push(quote);
        } else {
            self.write_field(row_index, buf, format);
        }
    }

    fn write_field_json(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.write_field_quoted(row_index, buf, format, b'\"');
    }

    fn serialize_field(&self, row_index: usize, format: &FormatSettings) -> Result<String, String> {
        let mut buf = Vec::with_capacity(100);
        self.write_field(row_index, &mut buf, format);
        String::from_utf8(buf).map_err(|_| "fail to serialize field".to_string())
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>, String> {
        unimplemented!()
    }
}

#[derive(Clone)]
#[enum_dispatch(TypeSerializer)]
pub enum TypeSerializerImpl {
    Null(NullSerializer),
    Nullable(NullableSerializer),
    Boolean(BooleanSerializer),
    Int8(NumberSerializer<i8>),
    Int16(NumberSerializer<i16>),
    Int32(NumberSerializer<i32>),
    Int64(NumberSerializer<i64>),
    UInt8(NumberSerializer<u8>),
    UInt16(NumberSerializer<u16>),
    UInt32(NumberSerializer<u32>),
    UInt64(NumberSerializer<u64>),
    Float32(NumberSerializer<F32>),
    Float64(NumberSerializer<F64>),
    Date(DateSerializer),
    Timestamp(TimestampSerializer),
    String(StringSerializer),
    Array(ArraySerializer),
    EmptyArray(EmptyArraySerializer),
    Tuple(TupleSerializer),
    Variant(VariantSerializer),
}
