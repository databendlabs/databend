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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use enum_dispatch::enum_dispatch;
use opensrv_clickhouse::types::column::ArcColumnData;
use serde_json::Value;

use crate::prelude::*;
mod array;
mod boolean;
mod date;
mod date_time;
mod null;
mod nullable;
mod number;
mod string;
mod struct_;
mod variant;

pub use array::*;
pub use boolean::*;
pub use date::*;
pub use date_time::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use struct_::*;
pub use variant::*;

#[enum_dispatch]
pub trait TypeSerializer: Send + Sync {
    fn serialize_value(&self, value: &DataValue) -> Result<String>;
    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>>;
    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>>;
    fn serialize_clickhouse_format(&self, column: &ColumnRef) -> Result<ArcColumnData>;

    fn serialize_json_object(
        &self,
        _column: &ColumnRef,
        _valids: Option<&Bitmap>,
    ) -> Result<Vec<Value>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }

    fn serialize_json_object_suppress_error(
        &self,
        _column: &ColumnRef,
    ) -> Result<Vec<Option<Value>>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }
}

#[enum_dispatch(TypeSerializer)]
pub enum TypeSerializerImpl {
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
    DateTime(DateTimeSerializer<i64>),
    String(StringSerializer),
    Array(ArraySerializer),
    Struct(StructSerializer),
    Variant(VariantSerializer),
}

mod test {
    use super::*;

    #[test]
    fn test1() {
        let c = StringSerializer {};
        let d: TypeSerializerImpl = c.into();
        let c: std::result::Result<StringSerializer, &str> = d.try_into();
        assert!(c.is_ok());
    }
}
