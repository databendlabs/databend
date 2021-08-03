// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use core::fmt;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::IntervalUnit as ArrowIntervalUnit;
use common_arrow::arrow::datatypes::TimeUnit as ArrowTimeUnit;

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
    Utf8,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32,
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits).
    Date64,
    Timestamp(TimeUnit, Option<String>),
    Interval(IntervalUnit),
    List(Box<DataField>),
    Struct(Vec<DataField>),
    Binary,
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

impl TimeUnit {
    pub fn to_arrow(&self) -> ArrowTimeUnit {
        unsafe { std::mem::transmute(self.clone()) }
    }

    pub fn from_arrow(iu: &ArrowTimeUnit) -> Self {
        unsafe { std::mem::transmute(iu.clone()) }
    }
}

/// YEAR_MONTH or DAY_TIME interval in SQL style.
#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (8-bytes in total).
    DayTime,
}

impl IntervalUnit {
    pub fn to_arrow(&self) -> ArrowIntervalUnit {
        unsafe { std::mem::transmute(self.clone()) }
    }

    pub fn from_arrow(iu: &ArrowIntervalUnit) -> Self {
        unsafe { std::mem::transmute(iu.clone()) }
    }
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
            Timestamp(tu, f) => ArrowDataType::Timestamp(tu.to_arrow(), f.clone()),
            Interval(tu) => ArrowDataType::Interval(tu.to_arrow()),
            List(dt) => ArrowDataType::LargeList(Box::new(dt.to_arrow())),
            Struct(fs) => {
                let arrows_fields = fs.iter().map(|f| f.to_arrow()).collect();
                ArrowDataType::Struct(arrows_fields)
            }
            Binary => ArrowDataType::LargeBinary,
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
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,

            ArrowDataType::Timestamp(tu, f) => {
                DataType::Timestamp(TimeUnit::from_arrow(tu), f.clone())
            }

            ArrowDataType::Interval(fu) => DataType::Interval(IntervalUnit::from_arrow(fu)),

            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::Utf8,
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::Binary,

            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!()
            }
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
