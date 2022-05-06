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
use common_exception::ErrorCode;
use common_exception::Result;
use dyn_clone::DynClone;

use super::type_array::ArrayType;
use super::type_boolean::BooleanType;
use super::type_date::DateType;
use super::type_id::TypeID;
use super::type_interval::IntervalType;
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
use super::type_variant::VariantType;
use super::type_variant_array::VariantArrayType;
use super::type_variant_object::VariantObjectType;
use crate::prelude::*;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
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

pub trait DataType: std::fmt::Debug + Sync + Send + DynClone {
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_null(&self) -> bool {
        self.data_type_id() == TypeID::Null
    }

    fn name(&self) -> String;

    /// Returns the name to display in the SQL describe
    fn sql_name(&self) -> String {
        self.name().to_uppercase()
    }

    fn aliases(&self) -> &[&str] {
        &[]
    }

    fn as_any(&self) -> &dyn Any;

    fn default_value(&self) -> DataValue;

    fn can_inside_nullable(&self) -> bool {
        true
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef>;

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef>;

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

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn>;
    fn create_serializer(&self) -> TypeSerializerImpl;
    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl;
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
        ArrowType::LargeList(f) => {
            let inner = from_arrow_field(f);
            DataTypeImpl::Array(ArrayType::create(inner))
        }

        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::Utf8 | ArrowType::LargeUtf8 => {
            DataTypeImpl::String(StringType::default())
        }

        ArrowType::Timestamp(_, _) => DataTypeImpl::Timestamp(TimestampType::create(0)),
        ArrowType::Date32 | ArrowType::Date64 => DataTypeImpl::Date(DateType::default()),

        ArrowType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name.clone()).collect();
            let types = fields.iter().map(from_arrow_field).collect();

            DataTypeImpl::Struct(StructType::create(names, types))
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
    if let Some(custom_name) = f.metadata.get(ARROW_EXTENSION_NAME) {
        let metadata = f.metadata.get(ARROW_EXTENSION_META).cloned();
        match custom_name.as_str() {
            "Date" => return DateType::new_impl(),
            "Timestamp" => match metadata {
                Some(meta) => {
                    let mut chars = meta.chars();
                    let precision = chars.next().unwrap().to_digit(10).unwrap();
                    return TimestampType::new_impl(precision as usize);
                }
                None => return TimestampType::new_impl(0),
            },
            "Interval" => return IntervalType::new_impl(metadata.unwrap().into()),
            "Variant" => return VariantType::new_impl(),
            "VariantArray" => return VariantArrayType::new_impl(),
            "VariantObject" => return VariantObjectType::new_impl(),
            _ => {}
        }
    }

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
        let nullable = data_type.as_any().downcast_ref::<NullableType>().unwrap();
        return nullable.inner_type().clone();
    }
    data_type.clone()
}

pub fn format_data_type_sql(data_type: &DataTypeImpl) -> String {
    let notnull_type = remove_nullable(data_type);
    match data_type.is_nullable() {
        true => format!("{} NULL", notnull_type.sql_name()),
        false => notnull_type.sql_name(),
    }
}

pub trait ToDataType {
    fn to_data_type() -> DataTypeImpl;
}

macro_rules! impl_to_data_type {
    ([], $( { $S: ident, $TY: ident} ),*) => {
        $(
            paste::paste!{
                impl ToDataType for $S {
                    fn to_data_type() -> DataTypeImpl {
                        [<$TY Type>]::new_impl()
                    }
                }
            }
        )*
    }
}

for_all_scalar_varints! { impl_to_data_type }

#[macro_export]
macro_rules! for_all_data_types{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Null, NullType},
            { Nullable, NullableType},
            { Boolean, BooleanType},
            { UInt8, UInt8Type},
            { UInt16, UInt16Type},
            { UInt32, UInt32Type},
            { UInt64, UInt64Type},
            { Int8, Int8Type},
            { Int16, Int16Type},
            { Int32, Int32Type},
            { Int64, Int64Type},
            { Float32, Float32Type},
            { Float64, Float64Type},
            { Date, DateType},
            { Timestamp, TimestampType},
            { String, StringType},
            { Struct, StructType},
            { Array,  ArrayType},
            { Variant,  VariantType},
            { VariantArray, VariantArrayType},
            { VariantObject, VariantObjectType},
            { Interval, IntervalType}
        }
    };
}

macro_rules! impl_from {
    ([], $( { $Abc: ident, $DT: ident} ),*) => {
        $(
             /// Implement `DataType -> DataTypeImpl`
            impl From<$DT> for DataTypeImpl {
                fn from(dt: $DT) -> Self {
                    DataTypeImpl::$Abc(dt)
                }
            }

            /// Implement `DataTypeImpl -> DataType`
            impl TryFrom<DataTypeImpl> for $DT {
                type Error = ErrorCode;

                fn try_from(array: DataTypeImpl) -> std::result::Result<Self, Self::Error> {
                    match array {
                        DataTypeImpl::$Abc(array) => Ok(array),
                        _ => Err(ErrorCode::IllegalDataType(format!("expected to be data_type: {:?}",  stringify!(DataTypeImpl::$Abc) ))),
                    }
                }
            }
        )*
    }
}

for_all_data_types! { impl_from }

macro_rules! impl_data_type {
    ([], $( { $Abc: ident, $DT: ident} ),*) => {
        impl DataTypeImpl {
            pub fn data_type_id(&self) -> TypeID {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.data_type_id(),
                    )*
                }
            }

            pub fn is_nullable(&self) -> bool {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.is_nullable(),
                    )*
                }
            }

            pub fn is_null(&self) -> bool {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.is_null(),
                    )*
                }
            }

            pub fn name(&self) -> String {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.name(),
                    )*
                }
            }

            /// Returns the name to display in the SQL describe
            pub fn sql_name(&self) -> String {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.sql_name(),
                    )*
                }
            }

            pub fn aliases(&self) -> &[&str] {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.aliases(),
                    )*
                }
            }

            pub fn as_any(&self) -> &dyn Any {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.as_any(),
                    )*
                }
            }

            pub fn default_value(&self) -> DataValue {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.default_value(),
                    )*
                }
            }

            pub fn can_inside_nullable(&self) -> bool {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.can_inside_nullable(),
                    )*
                }
            }

            pub fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.create_constant_column(data, size),
                    )*
                }
            }

            pub fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.create_column(data),
                    )*
                }
            }

            /// arrow_type did not have nullable sign, it's nullable sign is in the field
            pub fn arrow_type(&self) -> ArrowType {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.arrow_type(),
                    )*
                }
            }

            pub fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.custom_arrow_meta(),
                    )*
                }
            }

            pub fn to_arrow_field(&self, name: &str) -> ArrowField {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.to_arrow_field(name),
                    )*
                }
            }

            pub fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn>{
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.create_mutable(capacity),
                    )*
                }
            }

            pub fn create_serializer(&self) -> TypeSerializerImpl {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.create_serializer(),
                    )*
                }
            }

            pub fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
                match self {
                    $(
                        DataTypeImpl::$Abc(a) => a.create_deserializer(capacity),
                    )*
                }
            }
        }
    }
}

for_all_data_types! { impl_data_type }
