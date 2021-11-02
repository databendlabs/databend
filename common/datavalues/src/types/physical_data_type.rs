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

use crate::DataField;
use crate::DataType;

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub enum PhysicalDataType {
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
    List(Box<DataField>),
    Struct(Vec<DataField>),
    String,
}

impl From<DataType> for PhysicalDataType {
    fn from(t: DataType) -> Self {
        use PhysicalDataType::*;
        match t {
            DataType::Null => Null,
            DataType::Boolean => Boolean,
            DataType::UInt8 => UInt8,
            DataType::UInt16 | DataType::Date16 => UInt16,
            DataType::UInt32 | DataType::DateTime32(_) => UInt32,
            DataType::UInt64 => UInt64,
            DataType::Int8 => Int8,
            DataType::Int16 => Int16,
            DataType::Int32 | DataType::Date32 => Int32,
            DataType::Int64 => Int64,
            DataType::Float32 => Float32,
            DataType::Float64 => Float64,
            DataType::List(x) => List(x),
            DataType::Struct(x) => Struct(x),
            DataType::String => String,
            DataType::Interval(_) => Int64,
        }
    }
}

impl From<PhysicalDataType> for DataType {
    fn from(t: PhysicalDataType) -> Self {
        use DataType::*;
        match t {
            PhysicalDataType::Null => Null,
            PhysicalDataType::Boolean => Boolean,
            PhysicalDataType::UInt8 => UInt8,
            PhysicalDataType::UInt16 => UInt16,
            PhysicalDataType::UInt32 => UInt32,
            PhysicalDataType::UInt64 => UInt64,
            PhysicalDataType::Int8 => Int8,
            PhysicalDataType::Int16 => Int16,
            PhysicalDataType::Int32 => Int32,
            PhysicalDataType::Int64 => Int64,
            PhysicalDataType::Float32 => Float32,
            PhysicalDataType::Float64 => Float64,
            PhysicalDataType::List(x) => List(x),
            PhysicalDataType::Struct(x) => Struct(x),
            PhysicalDataType::String => String,
        }
    }
}

// convert from DataType to PhysicalDataType then convert from PhysicalDataType to DataType
pub fn data_type_physical(data_type: DataType) -> DataType {
    let t: PhysicalDataType = data_type.into();
    t.into()
}
