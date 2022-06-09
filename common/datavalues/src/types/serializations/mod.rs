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

mod array;
mod boolean;
mod const_;
mod date;
mod null;
mod nullable;
mod number;
mod string;
mod struct_;
mod timestamp;
mod variant;

pub use array::ArraySerializer;
pub use boolean::BooleanSerializer;
use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_io::prelude::FormatSettings;
pub use const_::ConstSerializer;
pub use date::DateSerializer;
use enum_dispatch::enum_dispatch;
pub use null::NullSerializer;
pub use nullable::NullableSerializer;
pub use number::NumberSerializer;
use opensrv_clickhouse::types::column::ArcColumnData;
use serde_json::Value;
pub use string::StringSerializer;
pub use struct_::StructSerializer;
pub use timestamp::TimestampSerializer;
pub use variant::VariantSerializer;

#[enum_dispatch]
pub trait TypeSerializer<'a>: Send + Sync {
    fn need_quote(&self) -> bool {
        false
    }
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings);
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
        }
        self.write_field(row_index, buf, format);
        if need_quote {
            buf.push(quote);
        }
    }

    fn serialize_field(&self, row_index: usize, format: &FormatSettings) -> Result<String> {
        let mut buf = Vec::with_capacity(100);
        self.write_field(row_index, &mut buf, format);
        String::from_utf8(buf).map_err_to_code(ErrorCode::BadBytes, || "fail to serialize field")
    }

    fn serialize_json(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        unimplemented!()
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        _size: usize,
    ) -> Result<ArcColumnData> {
        unimplemented!()
    }
    fn serialize_clickhouse_column(&self, _format: &FormatSettings) -> Result<ArcColumnData> {
        unimplemented!()
    }

    fn serialize_json_object(
        &self,
        _valids: Option<&Bitmap>,
        _format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }
}

#[derive(Clone)]
#[enum_dispatch(TypeSerializer)]
pub enum TypeSerializerImpl<'a> {
    Const(ConstSerializer<'a>),

    Null(NullSerializer),
    Nullable(NullableSerializer<'a>),
    Boolean(BooleanSerializer),
    Int8(NumberSerializer<'a, i8>),
    Int16(NumberSerializer<'a, i16>),
    Int32(NumberSerializer<'a, i32>),
    Int64(NumberSerializer<'a, i64>),
    UInt8(NumberSerializer<'a, u8>),
    UInt16(NumberSerializer<'a, u16>),
    UInt32(NumberSerializer<'a, u32>),
    UInt64(NumberSerializer<'a, u64>),
    Float32(NumberSerializer<'a, f32>),
    Float64(NumberSerializer<'a, f64>),

    Date(DateSerializer<'a, i32>),
    Interval(DateSerializer<'a, i64>),
    Timestamp(TimestampSerializer<'a>),
    String(StringSerializer<'a>),
    Array(ArraySerializer<'a>),
    Struct(StructSerializer<'a>),
    Variant(VariantSerializer<'a>),
}
