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

#[derive(Debug, Clone)]
pub enum TypeSerializerImpl {
    Null(NullSerializer),
    Nullable(NullableSerializer),
    Boolean(BooleanSerializer),
    Int8(Int8Serializer),
    Int16(Int16Serializer),
    Int32(Int32Serializer),
    Int64(Int64Serializer),
    UInt8(UInt8Serializer),
    UInt16(UInt16Serializer),
    UInt32(UInt32Serializer),
    UInt64(UInt64Serializer),
    Float32(Float32Serializer),
    Float64(Float64Serializer),

    Date(Date32Serializer),
    Interval(IntervalSerializer),
    Timestamp(TimestampSerializer),
    String(StringSerializer),
    Array(ArraySerializer),
    Struct(StructSerializer),
    Variant(VariantSerializer),
}

#[macro_export]
macro_rules! for_all_serializers {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Null, NullSerializer},
            { Nullable, NullableSerializer},
            { Boolean, BooleanSerializer},
            { Int8, Int8Serializer},
            { Int16, Int16Serializer},
            { Int32, Int32Serializer},
            { Int64, Int64Serializer},
            { UInt8, UInt8Serializer},
            { UInt16, UInt16Serializer},
            { UInt32, UInt32Serializer},
            { UInt64, UInt64Serializer},
            { Float32, Float32Serializer},
            { Float64, Float64Serializer},

            { Date, Date32Serializer},
            { Interval, IntervalSerializer},
            { Timestamp, TimestampSerializer},
            { String, StringSerializer},
            { Array, ArraySerializer},
            { Struct, StructSerializer},
            { Variant, VariantSerializer}
        }
    };
}

macro_rules! impl_from {
    ([], $( { $Abc: ident, $DT: ident} ),*) => {
        $(
             /// Implement `TypeSerializer -> TypeSerializerImpl`
            impl From<$DT> for TypeSerializerImpl {
                fn from(dt: $DT) -> Self {
                    TypeSerializerImpl::$Abc(dt)
                }
            }

            /// Implement `TypeSerializerImpl -> TypeSerializer`
            impl TryFrom<TypeSerializerImpl> for $DT {
                type Error = ErrorCode;

                fn try_from(array: TypeSerializerImpl) -> std::result::Result<Self, Self::Error> {
                    match array {
                        TypeSerializerImpl::$Abc(array) => Ok(array),
                        _ => Err(ErrorCode::IllegalDataType(format!("expected to be data_type: {:?}",  stringify!(TypeSerializerImpl::$Abc) ))),
                    }
                }
            }
        )*
    }
}

for_all_serializers! { impl_from }

macro_rules! impl_serializer {
    ([], $( { $Abc: ident, $DT: ident} ),*) => {
        impl TypeSerializerImpl {
            pub fn serialize_value(&self, value: &DataValue) -> Result<String> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_value(value),
                    )*
                }
            }
            pub fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_json(column),
                    )*
                }
            }

            pub fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_column(column),
                    )*
                }
            }

            pub fn serialize_clickhouse_format(&self, column: &ColumnRef) -> Result<ArcColumnData> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_clickhouse_format(column),
                    )*
                }
            }

            pub fn serialize_json_object(
                &self,
                column: &ColumnRef,
                valids: Option<&Bitmap>,
            ) -> Result<Vec<Value>> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_json_object(column, valids),
                    )*
                }
            }

            pub fn serialize_json_object_suppress_error(
                &self,
                column: &ColumnRef,
            ) -> Result<Vec<Option<Value>>> {
                match self {
                    $(
                        TypeSerializerImpl::$Abc(a) => a.serialize_json_object_suppress_error(column),
                    )*
                }
            }
        }
    };
}

for_all_serializers! { impl_serializer }
