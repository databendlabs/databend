// Copyright [2021] [Jorge C Leitao]
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

use parquet_format_safe::Type;
#[cfg(feature = "serde_types")]
use serde::Deserialize;
#[cfg(feature = "serde_types")]
use serde::Serialize;

use crate::error::Error;

/// The set of all physical types representable in Parquet
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Deserialize, Serialize))]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(usize),
}

impl TryFrom<(Type, Option<i32>)> for PhysicalType {
    type Error = Error;

    fn try_from((type_, length): (Type, Option<i32>)) -> Result<Self, Self::Error> {
        Ok(match type_ {
            Type::BOOLEAN => PhysicalType::Boolean,
            Type::INT32 => PhysicalType::Int32,
            Type::INT64 => PhysicalType::Int64,
            Type::INT96 => PhysicalType::Int96,
            Type::FLOAT => PhysicalType::Float,
            Type::DOUBLE => PhysicalType::Double,
            Type::BYTE_ARRAY => PhysicalType::ByteArray,
            Type::FIXED_LEN_BYTE_ARRAY => {
                let length = length
                    .ok_or_else(|| Error::oos("Length must be defined for FixedLenByteArray"))?;
                PhysicalType::FixedLenByteArray(length.try_into()?)
            }
            _ => return Err(Error::oos("Unknown type")),
        })
    }
}

impl From<PhysicalType> for (Type, Option<i32>) {
    fn from(physical_type: PhysicalType) -> Self {
        match physical_type {
            PhysicalType::Boolean => (Type::BOOLEAN, None),
            PhysicalType::Int32 => (Type::INT32, None),
            PhysicalType::Int64 => (Type::INT64, None),
            PhysicalType::Int96 => (Type::INT96, None),
            PhysicalType::Float => (Type::FLOAT, None),
            PhysicalType::Double => (Type::DOUBLE, None),
            PhysicalType::ByteArray => (Type::BYTE_ARRAY, None),
            PhysicalType::FixedLenByteArray(length) => {
                (Type::FIXED_LEN_BYTE_ARRAY, Some(length as i32))
            }
        }
    }
}
