// Copyright 2020 Datafuse Labs.
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

use core::fmt;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;

use crate::DataField;

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub enum DataType {
    Null,
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (16 bits), it's physical type is UInt16
    Date16,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits), it's physical type is UInt32
    Date32,

    /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in seconds, it's physical type is UInt32
    DateTime32,

    List(Box<DataField>),
    Struct(Vec<DataField>),
    String,
}

impl DataType {
    pub fn to_arrow(&self) -> ArrowDataType {
        use DataType::*;
        match self {
            Null => ArrowDataType::Null,
            Boolean => ArrowDataType::Boolean,
            UInt8 => ArrowDataType::UInt8,
            UInt16 => ArrowDataType::UInt16,
            UInt32 => ArrowDataType::UInt32,
            UInt64 => ArrowDataType::UInt64,
            Int8 => ArrowDataType::Int8,
            Int16 => ArrowDataType::Int16,
            Int32 => ArrowDataType::Int32,
            Int64 => ArrowDataType::Int64,
            Float32 => ArrowDataType::Float32,
            Float64 => ArrowDataType::Float64,
            Date16 => ArrowDataType::Extension(
                "Date16".to_string(),
                Box::new(ArrowDataType::UInt16),
                None,
            ),
            Date32 => ArrowDataType::Extension(
                "Date32".to_string(),
                Box::new(ArrowDataType::UInt32),
                None,
            ),
            DateTime32 => ArrowDataType::Extension(
                "DateTime32".to_string(),
                Box::new(ArrowDataType::UInt32),
                None,
            ),
            List(dt) => ArrowDataType::LargeList(Box::new(dt.to_arrow())),
            Struct(fs) => {
                let arrows_fields = fs.iter().map(|f| f.to_arrow()).collect();
                ArrowDataType::Struct(arrows_fields)
            }
            String => ArrowDataType::LargeBinary,
        }
    }
}

impl PartialEq<ArrowDataType> for DataType {
    fn eq(&self, other: &ArrowDataType) -> bool {
        let arrow_type = self.to_arrow();
        &arrow_type == other
    }
}

impl From<&ArrowDataType> for DataType {
    fn from(dt: &ArrowDataType) -> DataType {
        match dt {
            ArrowDataType::Null => DataType::Null,
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::List(f) | ArrowDataType::LargeList(f) => {
                let f: DataField = (f.as_ref()).into();
                DataType::List(Box::new(f))
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::String,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,

            ArrowDataType::Timestamp(_, _) => DataType::DateTime32,
            ArrowDataType::Date32 => DataType::Date16,
            ArrowDataType::Date64 => DataType::Date32,

            ArrowDataType::Extension(name, _arrow_type, _extra) => match name.as_str() {
                "Date16" => DataType::Date16,
                "Date32" => DataType::Date32,
                "DateTime32" => DataType::DateTime32,
                _ => unimplemented!("data_type: {}", dt),
            },

            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!("data_type: {}", dt)
            }
        }
    }
}

pub fn get_physical_arrow_type(data_type: &ArrowDataType) -> &ArrowDataType {
    if let ArrowDataType::Extension(_name, arrow_type, _extra) = data_type {
        return get_physical_arrow_type(arrow_type.as_ref());
    }

    return data_type;
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
