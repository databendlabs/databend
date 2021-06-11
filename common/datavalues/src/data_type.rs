// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use core::fmt;
use std::convert::TryFrom;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;

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
    Utf8,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32,
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits).
    Date64,
    Time64(TimeUnit),
    List(ArrowDataType),
    Duration(TimeUnit),
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
            Utf8 => ArrowDataType::LargeUtf8,
            Date32 => ArrowDataType::Date32,
            Date64 => ArrowDataType::Date64,
            Time64(tu) => ArrowDataType::Time64(tu.clone()),
            List(dt) => ArrowDataType::List(Box::new(Field::new("", dt.clone(), true))),
            Duration(tu) => ArrowDataType::Duration(tu.clone()),
        }
    }
}

impl PartialEq<ArrowDataType> for DataType {
    fn eq(&self, other: &ArrowDataType) -> bool {
        let arrow_type = self.to_arrow();
        &arrow_type == other
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ErrorCode;

    fn try_from(dt: &ArrowDataType) -> Result<Self> {
        match dt {
            ArrowDataType::Null => Ok(DataType::Null),
            ArrowDataType::UInt8 => Ok(DataType::UInt8),
            ArrowDataType::UInt16 => Ok(DataType::UInt16),
            ArrowDataType::UInt32 => Ok(DataType::UInt32),
            ArrowDataType::UInt64 => Ok(DataType::UInt64),
            ArrowDataType::Int8 => Ok(DataType::Int8),
            ArrowDataType::Int16 => Ok(DataType::Int16),
            ArrowDataType::Int32 => Ok(DataType::Int32),
            ArrowDataType::Int64 => Ok(DataType::Int64),
            ArrowDataType::LargeUtf8 => Ok(DataType::Utf8),
            ArrowDataType::Boolean => Ok(DataType::Boolean),
            ArrowDataType::Float32 => Ok(DataType::Float32),
            ArrowDataType::Float64 => Ok(DataType::Float64),
            ArrowDataType::LargeList(f) => Ok(DataType::List(f.data_type().clone())),
            ArrowDataType::Date32 => Ok(DataType::Date32),
            ArrowDataType::Date64 => Ok(DataType::Date64),
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            }
            ArrowDataType::Duration(TimeUnit::Nanosecond) => {
                Ok(DataType::Duration(TimeUnit::Nanosecond))
            }
            ArrowDataType::Duration(TimeUnit::Millisecond) => {
                Ok(DataType::Duration(TimeUnit::Millisecond))
            }
            ArrowDataType::Utf8 => Ok(DataType::Utf8),
            dt => Err(ErrorCode::IllegalDataType(format!(
                "Arrow datatype {:?} not supported by Datafuse",
                dt
            ))),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
