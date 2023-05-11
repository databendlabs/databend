// Copyright 2021 Datafuse Labs
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

use enum_dispatch::enum_dispatch;

use super::type_array::ArrayType;
use super::type_boolean::BooleanType;
use super::type_date::DateType;
use super::type_id::TypeID;
use super::type_nullable::NullableType;
use super::type_primitive::*;
use super::type_string::StringType;
use super::type_struct::StructType;
use super::type_timestamp::TimestampType;
use crate::prelude::*;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

#[derive(Clone, Debug, Hash, serde::Deserialize, serde::Serialize)]
#[allow(clippy::derived_hash_with_manual_eq)]
#[serde(tag = "type")]
#[enum_dispatch(DataType)]
pub enum DataTypeImpl {
    Null(NullType),
    Nullable(NullableType),
    Boolean(BooleanType),
    Int8(PrimitiveDataType<i8>),
    Int16(PrimitiveDataType<i16>),
    Int32(PrimitiveDataType<i32>),
    Int64(PrimitiveDataType<i64>),
    UInt8(PrimitiveDataType<u8>),
    UInt16(PrimitiveDataType<u16>),
    UInt32(PrimitiveDataType<u32>),
    UInt64(PrimitiveDataType<u64>),
    Float32(PrimitiveDataType<f32>),
    Float64(PrimitiveDataType<f64>),
    Date(DateType),
    Timestamp(TimestampType),
    String(StringType),
    Struct(StructType),
    Array(ArrayType),
    Variant(VariantType),
    VariantArray(VariantArrayType),
    VariantObject(VariantObjectType),
    Interval(IntervalType),
}

#[enum_dispatch]
pub trait DataType: std::fmt::Debug + Sync + Send
where Self: Sized
{
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_null(&self) -> bool {
        self.data_type_id() == TypeID::Null
    }

    fn name(&self) -> String;

    fn can_inside_nullable(&self) -> bool {
        true
    }
}

pub fn wrap_nullable(data_type: &DataTypeImpl) -> DataTypeImpl {
    if !data_type.can_inside_nullable() {
        return data_type.clone();
    }
    NullableType::new_impl(data_type.clone())
}

pub fn remove_nullable(data_type: &DataTypeImpl) -> DataTypeImpl {
    if matches!(data_type.data_type_id(), TypeID::Nullable) {
        let nullable: NullableType = data_type.to_owned().try_into().unwrap();
        return nullable.inner_type().clone();
    }
    data_type.clone()
}
