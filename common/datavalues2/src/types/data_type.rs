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

use super::data_type_boolean::DataTypeBoolean;
use super::data_type_date::DataTypeDate;
use super::data_type_date32::DataTypeDate32;
use super::data_type_datetime::DataTypeDateTime;
use super::data_type_datetime64::DataTypeDateTime64;
use super::data_type_list::DataTypeList;
use super::data_type_nullable::DataTypeNothing;
use super::data_type_nullable::DataTypeNullable;
use super::data_type_numeric::DataTypeFloat32;
use super::data_type_numeric::DataTypeFloat64;
use super::data_type_numeric::DataTypeInt16;
use super::data_type_numeric::DataTypeInt32;
use super::data_type_numeric::DataTypeInt64;
use super::data_type_numeric::DataTypeInt8;
use super::data_type_numeric::DataTypeUInt16;
use super::data_type_numeric::DataTypeUInt32;
use super::data_type_numeric::DataTypeUInt64;
use super::data_type_numeric::DataTypeUInt8;
use super::data_type_string::DataTypeString;
use super::data_type_struct::DataTypeStruct;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::TypeDeserializer;
use crate::TypeSerializer;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

pub type DataTypePtr = Arc<dyn IDataType>;

#[typetag::serde(tag = "type")]
pub trait IDataType: std::fmt::Debug + Sync + Send + DynClone {
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any;

    fn default_value(&self) -> DataValue;

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef>;

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

    fn create_serializer(&self) -> Box<dyn TypeSerializer>;
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer>;
}

pub fn from_arrow_type(dt: &ArrowType) -> DataTypePtr {
    match dt {
        ArrowType::Null => Arc::new(DataTypeNullable::create(Arc::new(DataTypeNothing {}))),
        ArrowType::UInt8 => Arc::new(DataTypeUInt8::default()),
        ArrowType::UInt16 => Arc::new(DataTypeUInt16::default()),
        ArrowType::UInt32 => Arc::new(DataTypeUInt32::default()),
        ArrowType::UInt64 => Arc::new(DataTypeUInt64::default()),
        ArrowType::Int8 => Arc::new(DataTypeInt8::default()),
        ArrowType::Int16 => Arc::new(DataTypeInt16::default()),
        ArrowType::Int32 => Arc::new(DataTypeInt32::default()),
        ArrowType::Int64 => Arc::new(DataTypeInt64::default()),
        ArrowType::Boolean => Arc::new(DataTypeBoolean::default()),
        ArrowType::Float32 => Arc::new(DataTypeFloat32::default()),
        ArrowType::Float64 => Arc::new(DataTypeFloat64::default()),

        ArrowType::FixedSizeList(f, size) => {
            let inner = from_arrow_field(f);
            let name = f.name();
            Arc::new(DataTypeList::create(name.clone(), *size, inner))
        }
        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::Utf8 | ArrowType::LargeUtf8 => {
            Arc::new(DataTypeString::default())
        }

        ArrowType::Timestamp(_, tz) => Arc::new(DataTypeDateTime::create(tz.clone())),
        ArrowType::Date32 => Arc::new(DataTypeDate::default()),
        ArrowType::Date64 => Arc::new(DataTypeDate32::default()),

        ArrowType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name().to_string()).collect();
            let types = fields.iter().map(|f| from_arrow_field(f)).collect();

            Arc::new(DataTypeStruct::create(names, types))
        }

        // this is safe, because we define the datatype firstly
        _ => {
            unimplemented!("data_type: {:?}", dt)
        }
    }
}

pub fn from_arrow_field(f: &ArrowField) -> DataTypePtr {
    if let Some(m) = f.metadata() {
        if let Some(custom_name) = m.get("ARROW:extension:databend_name") {
            let metatada = m.get("ARROW:extension:databend_metadata");
            match custom_name.as_str() {
                "Date" | "Date16" => return Arc::new(DataTypeDate::default()),
                "Date32" => return Arc::new(DataTypeDate32::default()),
                "DateTime" | "DateTime32" => return Arc::new(DataTypeDateTime::default()),
                "DateTime64" => return Arc::new(DataTypeDateTime64::default()),
                _ => {}
            }
        }
    }

    let dt = f.data_type();
    from_arrow_type(dt)
}
