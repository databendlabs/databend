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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use enum_dispatch::enum_dispatch;
use opensrv_clickhouse::types::column::ArcColumnData;
use serde_json::Value;

use crate::prelude::*;
mod array;
mod boolean;
mod date;
mod null;
mod nullable;
mod number;
mod string;
mod struct_;
mod timestamp;
mod variant;

pub use array::*;
pub use boolean::*;
pub use date::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use struct_::*;
pub use timestamp::*;
pub use variant::*;

#[enum_dispatch]
pub trait TypeSerializer: Send + Sync {
    fn serialize_value(&self, value: &DataValue, format: Arc<FormatSettings>) -> Result<String>;
    fn serialize_json(&self, column: &ColumnRef, format: Arc<FormatSettings>)
        -> Result<Vec<Value>>;
    fn serialize_column(
        &self,
        column: &ColumnRef,
        format: Arc<FormatSettings>,
    ) -> Result<Vec<String>>;
    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
        _format: Arc<FormatSettings>,
    ) -> Result<ArcColumnData>;

    fn serialize_json_object(
        &self,
        _column: &ColumnRef,
        _valids: Option<&Bitmap>,
        _format: Arc<FormatSettings>,
    ) -> Result<Vec<Value>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }

    fn serialize_json_object_suppress_error(
        &self,
        _column: &ColumnRef,
        _format: Arc<FormatSettings>,
    ) -> Result<Vec<Option<Value>>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }
}

#[derive(Debug, Clone)]
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
    Float32(NumberSerializer<f32>),
    Float64(NumberSerializer<f64>),

    Date(DateSerializer<i32>),
    Interval(DateSerializer<i64>),
    Timestamp(TimestampSerializer),
    String(StringSerializer),
    Array(ArraySerializer),
    Struct(StructSerializer),
    Variant(VariantSerializer),
}
