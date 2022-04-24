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

use std::any::Any;
use std::collections::BTreeMap;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::Result;
use enum_dispatch::enum_dispatch;

use super::type_array::ArrayType;
use super::type_boolean::BooleanType;
use super::type_date::DateType;
use super::type_datetime::DateTimeType;
use super::type_id::TypeID;
use super::type_nullable::NullableType;
use super::type_primitive::*;
use super::type_string::StringType;
use super::type_struct::StructType;
use crate::prelude::*;
use crate::TypeDeserializer;
use crate::TypeSerializer;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[enum_dispatch(DataType)]
pub enum DataTypeImpl {
    Nullable(NullableType),
    Boolean(BooleanType),
    Int8(Int8Type),
    Int16(Int16Type),
    Int32(Int32Type),
    Int64(Int64Type),
    UInt8(UInt8Type),
    UInt16(UInt16Type),
    UInt32(UInt32Type),
    UInt64(UInt64Type),
    Float32(Float32Type),
    Float64(Float64Type),
    Date(DateType),
    DateTime(DateTimeType),
    String(StringType),
    Struct(StructType),
    Array(ArrayType),
    Variant(VariantType),
}
