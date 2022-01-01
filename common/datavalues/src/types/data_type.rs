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

use core::fmt;

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_macros::MallocSizeOf;

use crate::DataField;
use crate::PhysicalDataType;

pub type Nullable = bool;

#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, MallocSizeOf,
)]
pub enum DataType {
    Null,
    Boolean(Nullable),
    UInt8(Nullable),
    UInt16(Nullable),
    UInt32(Nullable),
    UInt64(Nullable),
    Int8(Nullable),
    Int16(Nullable),
    Int32(Nullable),
    Int64(Nullable),
    Float32(Nullable),
    Float64(Nullable),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (16 bits), it's physical type is UInt16
    Date16(Nullable),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits), it's physical type is Int32
    Date32(Nullable),

    /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in seconds, it's physical type is UInt32
    /// Option<String> indicates the timezone, if it's None, it's UTC
    DateTime32(Nullable, Option<String>),

    /// A 64-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in nanoseconds, it's physical type is UInt64
    /// The time resolution is determined by the precision parameter, range from 0 to 9
    /// Typically are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds)
    /// Option<String> indicates the timezone, if it's None, it's UTC
    DateTime64(Nullable, u32, Option<String>),

    Interval(Nullable, IntervalUnit),

    List(Box<DataField>),
    Struct(Vec<DataField>),
    String(Nullable),
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    MallocSizeOf,
)]
pub enum IntervalUnit {
    YearMonth,
    DayTime,
}

impl fmt::Display for IntervalUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match self {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
        };
        write!(f, "{}", str)
    }
}

impl DataType {
    pub fn to_physical_type(&self) -> PhysicalDataType {
        self.clone().into()
    }

    #[inline]
    pub fn is_boolean(&self) -> bool {
        matches!(self, DataType::Boolean(_))
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        matches!(
            self,
            DataType::Null
                | DataType::Boolean(true)
                | DataType::UInt8(true)
                | DataType::UInt16(true)
                | DataType::UInt32(true)
                | DataType::UInt64(true)
                | DataType::Int8(true)
                | DataType::Int16(true)
                | DataType::Int32(true)
                | DataType::Int64(true)
                | DataType::Float32(true)
                | DataType::Float64(true)
                | DataType::Date16(true)
                | DataType::Date32(true)
                | DataType::DateTime32(true, _)
                | DataType::DateTime64(true, _, _)
                | DataType::Interval(true, _)
                | DataType::String(true)
        )
    }

    #[must_use]
    #[inline]
    pub fn enforce_nullable(&self) -> DataType {
        match self {
            DataType::Boolean(false) => DataType::Boolean(true),
            DataType::UInt8(false) => DataType::UInt8(true),
            DataType::UInt16(false) => DataType::UInt16(true),
            DataType::UInt32(false) => DataType::UInt32(true),
            DataType::UInt64(false) => DataType::UInt64(true),
            DataType::Int8(false) => DataType::Int8(true),
            DataType::Int16(false) => DataType::Int16(true),
            DataType::Int32(false) => DataType::Int32(true),
            DataType::Int64(false) => DataType::Int64(true),
            DataType::Float32(false) => DataType::Float32(true),
            DataType::Float64(false) => DataType::Float64(true),
            DataType::Date16(false) => DataType::Date16(true),
            DataType::Date32(false) => DataType::Date32(true),
            DataType::DateTime32(false, s) => DataType::DateTime32(true, s.clone()),
            DataType::DateTime64(false, a, b) => DataType::DateTime64(true, *a, b.clone()),
            DataType::Interval(false, a) => DataType::Interval(true, a.clone()),
            DataType::String(false) => DataType::String(true),
            _ => self.clone(),
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::String(_))
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::UInt8(_)
                | DataType::UInt16(_)
                | DataType::UInt32(_)
                | DataType::UInt64(_)
        )
    }

    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8(_) | DataType::Int16(_) | DataType::Int32(_) | DataType::Int64(_)
        )
    }

    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            DataType::UInt8(_) | DataType::UInt16(_) | DataType::UInt32(_) | DataType::UInt64(_)
        )
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        matches!(self, DataType::Float32(_) | DataType::Float64(_))
    }

    #[inline]
    pub fn is_date_or_date_time(&self) -> bool {
        matches!(
            self,
            DataType::Date16(_)
                | DataType::Date32(_)
                | DataType::DateTime32(_, _)
                | DataType::DateTime64(_, _, _),
        )
    }

    /// Determine if a DataType is signed numeric or not
    #[inline]
    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::Float32(_)
                | DataType::Float64(_)
        )
    }

    /// Determine if a DataType is numeric or not
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_signed_numeric()
            || matches!(
                self,
                DataType::UInt8(_)
                    | DataType::UInt16(_)
                    | DataType::UInt32(_)
                    | DataType::UInt64(_)
            )
    }

    #[inline]
    pub fn is_interval(&self) -> bool {
        matches!(self, DataType::Interval(_, _))
    }

    #[inline]
    pub fn numeric_byte_size(&self) -> Result<usize> {
        match self {
            DataType::Int8(_) | DataType::UInt8(_) => Ok(1),
            DataType::Int16(_) | DataType::UInt16(_) => Ok(2),
            DataType::Int32(_) | DataType::UInt32(_) | DataType::Float32(_) => Ok(4),
            DataType::Int64(_) | DataType::UInt64(_) | DataType::Float64(_) => Ok(8),
            _ => Result::Err(ErrorCode::BadArguments(format!(
                "Function number_byte_size argument must be numeric types, but got {:?}",
                self
            ))),
        }
    }

    pub fn to_arrow(&self) -> ArrowDataType {
        use DataType::*;
        match self {
            Null => ArrowDataType::Null,
            Boolean(_) => ArrowDataType::Boolean,
            UInt8(_) => ArrowDataType::UInt8,
            UInt16(_) => ArrowDataType::UInt16,
            UInt32(_) => ArrowDataType::UInt32,
            UInt64(_) => ArrowDataType::UInt64,
            Int8(_) => ArrowDataType::Int8,
            Int16(_) => ArrowDataType::Int16,
            Int32(_) => ArrowDataType::Int32,
            Int64(_) => ArrowDataType::Int64,
            Float32(_) => ArrowDataType::Float32,
            Float64(_) => ArrowDataType::Float64,
            Date16(_) => ArrowDataType::UInt16,
            Date32(_) => ArrowDataType::Int32,
            // we don't use DataType::Extension because extension types are not supported in parquet
            DateTime32(_, _) => ArrowDataType::UInt32,
            DateTime64(_, _, _) => ArrowDataType::UInt64,
            List(dt) => ArrowDataType::LargeList(Box::new(dt.to_arrow())),
            Struct(fs) => {
                let arrows_fields = fs.iter().map(|f| f.to_arrow()).collect();
                ArrowDataType::Struct(arrows_fields)
            }
            String(_) => ArrowDataType::LargeBinary,
            Interval(_, _) => ArrowDataType::Int64,
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
            ArrowDataType::UInt8 => DataType::UInt8(true),
            ArrowDataType::UInt16 => DataType::UInt16(true),
            ArrowDataType::UInt32 => DataType::UInt32(true),
            ArrowDataType::UInt64 => DataType::UInt64(true),
            ArrowDataType::Int8 => DataType::Int8(true),
            ArrowDataType::Int16 => DataType::Int16(true),
            ArrowDataType::Int32 => DataType::Int32(true),
            ArrowDataType::Int64 => DataType::Int64(true),
            ArrowDataType::Boolean => DataType::Boolean(true),
            ArrowDataType::Float32 => DataType::Float32(true),
            ArrowDataType::Float64 => DataType::Float64(true),
            ArrowDataType::List(f) | ArrowDataType::LargeList(f) => {
                let f: DataField = (f.as_ref()).into();
                DataType::List(Box::new(f))
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::String(true),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String(true),

            ArrowDataType::Timestamp(_, tz) => DataType::DateTime32(true, tz.clone()),
            ArrowDataType::Date32 => DataType::Date16(true),
            ArrowDataType::Date64 => DataType::Date32(true),

            ArrowDataType::Extension(name, _arrow_type, extra) => match name.as_str() {
                "Date16" => DataType::Date16(true),
                "Date32" => DataType::Date32(true),
                "DateTime32" => DataType::DateTime32(true, extra.clone()),
                "DateTime64" => DataType::DateTime64(true, 3, extra.clone()),
                _ => unimplemented!("data_type: {}", dt),
            },

            ArrowDataType::Struct(fields) => {
                let fields: Vec<DataField> = fields.iter().map(|f| f.into()).collect();
                DataType::Struct(fields)
            }

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

    data_type
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean(_) => write!(f, "Boolean"),
            Self::UInt8(_) => write!(f, "UInt8"),
            Self::UInt16(_) => write!(f, "UInt16"),
            Self::UInt32(_) => write!(f, "UInt32"),
            Self::UInt64(_) => write!(f, "UInt64"),
            Self::Int8(_) => write!(f, "Int8"),
            Self::Int16(_) => write!(f, "Int16"),
            Self::Int32(_) => write!(f, "Int32"),
            Self::Int64(_) => write!(f, "Int64"),
            Self::Float32(_) => write!(f, "Float32"),
            Self::Float64(_) => write!(f, "Float64"),
            Self::Date16(_) => write!(f, "Date16"),
            Self::Date32(_) => write!(f, "Date32"),
            Self::DateTime32(_, arg0) => {
                if let Some(tz) = arg0 {
                    write!(f, "DateTime32({:?})", tz)
                } else {
                    write!(f, "DateTime32")
                }
            }
            Self::DateTime64(_, arg0, arg1) => {
                if let Some(tz) = arg1 {
                    write!(f, "DateTime64({:?}, {:?})", arg0, tz)
                } else {
                    write!(f, "DateTime64({:?})", arg0)
                }
            }
            Self::List(arg0) => f.debug_tuple("List").field(arg0).finish(),
            Self::Struct(arg0) => f.debug_tuple("Struct").field(arg0).finish(),
            Self::String(_) => write!(f, "String"),
            Self::Interval(_, unit) => write!(f, "Interval({})", unit),
        }
    }
}
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
