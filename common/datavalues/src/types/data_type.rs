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
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::Result;
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
use crate::TypeDeserializer;
use crate::TypeSerializer;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

pub type DataTypePtr = Arc<dyn DataType>;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
    TimeStamp(TimestampType),
    String(StringType),
    Struct(StructType),
    Array(ArrayType),
    Variant(VariantType),
}

#[typetag::serde(tag = "type")]
#[enum_dispatch]
pub trait DataType: std::fmt::Debug + Sync + Send + DynClone {
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_null(&self) -> bool {
        self.data_type_id() == TypeID::Null
    }

    fn name(&self) -> &str;

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
    fn create_serializer(&self) -> Box<dyn TypeSerializer>;
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer>;
}

pub fn from_arrow_type(dt: &ArrowType) -> DataTypePtr {
    match dt {
        ArrowType::Null => Arc::new(NullType {}),
        ArrowType::UInt8 => Arc::new(UInt8Type::default()),
        ArrowType::UInt16 => Arc::new(UInt16Type::default()),
        ArrowType::UInt32 => Arc::new(UInt32Type::default()),
        ArrowType::UInt64 => Arc::new(UInt64Type::default()),
        ArrowType::Int8 => Arc::new(Int8Type::default()),
        ArrowType::Int16 => Arc::new(Int16Type::default()),
        ArrowType::Int32 => Arc::new(Int32Type::default()),
        ArrowType::Int64 => Arc::new(Int64Type::default()),
        ArrowType::Boolean => Arc::new(BooleanType::default()),
        ArrowType::Float32 => Arc::new(Float32Type::default()),
        ArrowType::Float64 => Arc::new(Float64Type::default()),

        // TODO support other list
        ArrowType::LargeList(f) => {
            let inner = from_arrow_field(f);
            Arc::new(ArrayType::create(inner))
        }

        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::Utf8 | ArrowType::LargeUtf8 => {
            Arc::new(StringType::default())
        }

        ArrowType::Timestamp(_, tz) => Arc::new(TimestampType::create(0, tz.clone())),
        ArrowType::Date32 | ArrowType::Date64 => Arc::new(DateType::default()),

        ArrowType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name.clone()).collect();
            let types = fields.iter().map(from_arrow_field).collect();

            Arc::new(StructType::create(names, types))
        }
        ArrowType::Extension(custom_name, _, _) => match custom_name.as_str() {
            "Variant" => Arc::new(VariantType::default()),
            "VariantArray" => Arc::new(VariantArrayType::default()),
            "VariantObject" => Arc::new(VariantObjectType::default()),
            _ => unimplemented!("data_type: {:?}", dt),
        },

        // this is safe, because we define the datatype firstly
        _ => {
            unimplemented!("data_type: {:?}", dt)
        }
    }
}

pub fn from_arrow_field(f: &ArrowField) -> DataTypePtr {
    if let Some(custom_name) = f.metadata.get(ARROW_EXTENSION_NAME) {
        let metadata = f.metadata.get(ARROW_EXTENSION_META).cloned();
        match custom_name.as_str() {
            "Date" => return DateType::arc(),
            "TimeStamp" => match metadata {
                Some(meta) => {
                    let mut chars = meta.chars();
                    let precision = chars.next().unwrap().to_digit(10).unwrap();
                    let tz = chars.collect::<String>();
                    return TimestampType::arc(precision as usize, Some(tz));
                }
                None => return TimestampType::arc(0, None),
            },
            "Interval" => return IntervalType::arc(metadata.unwrap().into()),
            "Variant" => return VariantType::arc(),
            "VariantArray" => return VariantArrayType::arc(),
            "VariantObject" => return VariantObjectType::arc(),
            _ => {}
        }
    }

    let dt = f.data_type();
    let ty = from_arrow_type(dt);

    let is_nullable = f.is_nullable;
    if is_nullable && ty.can_inside_nullable() {
        NullableType::arc(ty)
    } else {
        ty
    }
}

pub trait ToDataType {
    fn to_data_type() -> DataTypePtr;
}

macro_rules! impl_to_data_type {
    ([], $( { $S: ident, $TY: ident} ),*) => {
        $(
            paste::paste!{
                impl ToDataType for $S {
                    fn to_data_type() -> DataTypePtr {
                        [<$TY Type>]::arc()
                    }
                }
            }
        )*
    }
}

for_all_scalar_varints! { impl_to_data_type }

pub fn wrap_nullable(data_type: &DataTypePtr) -> DataTypePtr {
    if !data_type.can_inside_nullable() {
        return data_type.clone();
    }
    NullableType::arc(data_type.clone())
}

pub fn remove_nullable(data_type: &DataTypePtr) -> DataTypePtr {
    if matches!(data_type.data_type_id(), TypeID::Nullable) {
        let nullable = data_type.as_any().downcast_ref::<NullableType>().unwrap();
        return nullable.inner_type().clone();
    }
    data_type.clone()
}

pub fn format_data_type_sql(data_type: &DataTypePtr) -> String {
    let notnull_type = remove_nullable(data_type);
    match data_type.is_nullable() {
        true => format!("{} NULL", notnull_type.sql_name()),
        false => notnull_type.sql_name(),
    }
}
