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

use std::collections::BTreeMap;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use dyn_clone::DynClone;
use enum_dispatch::enum_dispatch;

use super::type_array::ArrayType;
use super::type_boolean::BooleanType;
use super::type_date::DateType;
use super::type_id::TypeID;
use super::type_nullable::NullableType;
use super::type_primitive::Float32Type;
use super::type_primitive::Float64Type;
use super::type_primitive::Int16Type;
use super::type_primitive::Int32Type;
use super::type_primitive::Int64Type;
use super::type_primitive::Int8Type;
use super::type_primitive::UInt16Type;
use super::type_primitive::UInt32Type;
use super::type_primitive::UInt64Type;
use super::type_primitive::UInt8Type;
use super::type_string::StringType;
use super::type_struct::StructType;
use super::type_timestamp::TimestampType;
use crate::prelude::*;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

#[derive(Clone, Debug, Hash, serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_hash_xor_eq)]
#[serde(tag = "type")]
#[enum_dispatch(DataType)]
pub enum DataTypeImpl {
    Null(NullType),
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
pub trait DataType: std::fmt::Debug + Sync + Send + DynClone
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

    /// arrow_type did not have nullable sign, it's nullable sign is in the field
    fn arrow_type(&self) -> ArrowType;

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        None
    }

    fn to_arrow_field(&self, name: &str) -> ArrowField {
        let ret = ArrowField::new(name, self.arrow_type(), self.is_nullable());
        if let Some(meta) = self.custom_arrow_meta() {
            ret.with_metadata(meta)
        } else {
            ret
        }
    }
}

pub fn from_arrow_type(dt: &ArrowType) -> DataTypeImpl {
    match dt {
        ArrowType::Null => DataTypeImpl::Null(NullType {}),
        ArrowType::UInt8 => DataTypeImpl::UInt8(UInt8Type::default()),
        ArrowType::UInt16 => DataTypeImpl::UInt16(UInt16Type::default()),
        ArrowType::UInt32 => DataTypeImpl::UInt32(UInt32Type::default()),
        ArrowType::UInt64 => DataTypeImpl::UInt64(UInt64Type::default()),
        ArrowType::Int8 => DataTypeImpl::Int8(Int8Type::default()),
        ArrowType::Int16 => DataTypeImpl::Int16(Int16Type::default()),
        ArrowType::Int32 => DataTypeImpl::Int32(Int32Type::default()),
        ArrowType::Int64 => DataTypeImpl::Int64(Int64Type::default()),
        ArrowType::Boolean => DataTypeImpl::Boolean(BooleanType::default()),
        ArrowType::Float32 => DataTypeImpl::Float32(Float32Type::default()),
        ArrowType::Float64 => DataTypeImpl::Float64(Float64Type::default()),

        // TODO support other list
        ArrowType::List(f) | ArrowType::LargeList(f) | ArrowType::FixedSizeList(f, _) => {
            let inner = from_arrow_field(f);
            DataTypeImpl::Array(ArrayType::create(inner))
        }

        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::Utf8 | ArrowType::LargeUtf8 => {
            DataTypeImpl::String(StringType::default())
        }

        ArrowType::Timestamp(_, _) => TimestampType::new_impl(),

        ArrowType::Date32 | ArrowType::Date64 => DataTypeImpl::Date(DateType::default()),

        ArrowType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name.clone()).collect();
            let types = fields.iter().map(from_arrow_field).collect();

            DataTypeImpl::Struct(StructType::create(Some(names), types))
        }
        ArrowType::Extension(custom_name, _, _) => match custom_name.as_str() {
            "Variant" => DataTypeImpl::Variant(VariantType::default()),
            "VariantArray" => DataTypeImpl::VariantArray(VariantArrayType::default()),
            "VariantObject" => DataTypeImpl::VariantObject(VariantObjectType::default()),
            _ => unimplemented!("data_type: {:?}", dt),
        },

        // this is safe, because we define the datatype firstly
        _ => {
            unimplemented!("data_type: {:?}", dt)
        }
    }
}

pub fn from_arrow_field(f: &ArrowField) -> DataTypeImpl {
    let dt = f.data_type();
    let ty = from_arrow_type(dt);

    let is_nullable = f.is_nullable;
    if is_nullable && ty.can_inside_nullable() {
        NullableType::new_impl(ty)
    } else {
        ty
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
