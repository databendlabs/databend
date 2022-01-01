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
            DataType::Boolean(_) => Boolean,
            DataType::UInt8(_) => UInt8,
            DataType::UInt16(_) | DataType::Date16(_) => UInt16,
            DataType::UInt32(_) | DataType::DateTime32(_, _) => UInt32,
            DataType::UInt64(_) | DataType::DateTime64(_, _, _) => UInt64,
            DataType::Int8(_) => Int8,
            DataType::Int16(_) => Int16,
            DataType::Int32(_) | DataType::Date32(_) => Int32,
            DataType::Int64(_) => Int64,
            DataType::Float32(_) => Float32,
            DataType::Float64(_) => Float64,
            DataType::List(x) => List(x),
            DataType::Struct(x) => Struct(x),
            DataType::String(_) => String,
            DataType::Interval(_, _) => Int64,
        }
    }
}

impl From<PhysicalDataType> for DataType {
    fn from(t: PhysicalDataType) -> Self {
        use DataType::*;
        match t {
            PhysicalDataType::Null => Null,
            PhysicalDataType::Boolean => Boolean(true),
            PhysicalDataType::UInt8 => UInt8(true),
            PhysicalDataType::UInt16 => UInt16(true),
            PhysicalDataType::UInt32 => UInt32(true),
            PhysicalDataType::UInt64 => UInt64(true),
            PhysicalDataType::Int8 => Int8(true),
            PhysicalDataType::Int16 => Int16(true),
            PhysicalDataType::Int32 => Int32(true),
            PhysicalDataType::Int64 => Int64(true),
            PhysicalDataType::Float32 => Float32(true),
            PhysicalDataType::Float64 => Float64(true),
            PhysicalDataType::List(x) => List(x),
            PhysicalDataType::Struct(x) => Struct(x),
            PhysicalDataType::String => String(true),
        }
    }
}

// convert from DataType to PhysicalDataType then convert from PhysicalDataType to DataType
pub fn data_type_physical(data_type: DataType) -> DataType {
    let t: PhysicalDataType = data_type.into();
    t.into()
}
