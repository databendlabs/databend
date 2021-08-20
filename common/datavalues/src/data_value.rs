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

// Borrow from apache/arrow/rust/datafusion/src/functions.rs
// See notice.md

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::arrays::ListBooleanArrayBuilder;
use crate::arrays::ListBuilderTrait;
use crate::arrays::ListPrimitiveArrayBuilder;
use crate::arrays::ListUtf8ArrayBuilder;
use crate::prelude::*;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DataField;

/// A specific value of a data type.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum DataValue {
    /// Base type.
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Binary(Option<Vec<u8>>),
    Utf8(Option<String>),

    /// Datetime.
    /// Date stored as a signed 32bit int
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int
    Date64(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>),
    /// Interval with YearMonth unit
    IntervalYearMonth(Option<i32>),
    /// Interval with DayTime unit
    IntervalDayTime(Option<i64>),

    // Container struct.
    List(Option<Vec<DataValue>>, DataType),
    Struct(Vec<DataValue>),
}

pub type DataValueRef = Arc<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            DataValue::Boolean(None)
                | DataValue::Int8(None)
                | DataValue::Int16(None)
                | DataValue::Int32(None)
                | DataValue::Int64(None)
                | DataValue::UInt8(None)
                | DataValue::UInt16(None)
                | DataValue::UInt32(None)
                | DataValue::UInt64(None)
                | DataValue::Float32(None)
                | DataValue::Float64(None)
                | DataValue::Binary(None)
                | DataValue::Utf8(None)
                | DataValue::Date32(None)
                | DataValue::Date64(None)
                | DataValue::Null
                | DataValue::TimestampMillisecond(None)
                | DataValue::TimestampMicrosecond(None)
                | DataValue::TimestampNanosecond(None)
                | DataValue::List(None, _)
        )
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DataValue::Null => DataType::Null,
            DataValue::Boolean(_) => DataType::Boolean,
            DataValue::Int8(_) => DataType::Int8,
            DataValue::Int16(_) => DataType::Int16,
            DataValue::Int32(_) => DataType::Int32,
            DataValue::Int64(_) => DataType::Int64,
            DataValue::UInt8(_) => DataType::UInt8,
            DataValue::UInt16(_) => DataType::UInt16,
            DataValue::UInt32(_) => DataType::UInt32,
            DataValue::UInt64(_) => DataType::UInt64,
            DataValue::Float32(_) => DataType::Float32,
            DataValue::Float64(_) => DataType::Float64,
            DataValue::Utf8(_) => DataType::Utf8,
            DataValue::Date32(_) => DataType::Date32,
            DataValue::Date64(_) => DataType::Date64,
            DataValue::TimestampSecond(_) => DataType::Timestamp(TimeUnit::Second, None),
            DataValue::TimestampMillisecond(_) => DataType::Timestamp(TimeUnit::Millisecond, None),
            DataValue::TimestampMicrosecond(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            DataValue::TimestampNanosecond(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataValue::IntervalYearMonth(_) => DataType::Interval(IntervalUnit::YearMonth),
            DataValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            DataValue::List(_, data_type) => {
                DataType::List(Box::new(DataField::new("item", data_type.clone(), true)))
            }
            DataValue::Struct(v) => {
                let fields = v
                    .iter()
                    .enumerate()
                    .map(|(i, x)| {
                        let typ = x.data_type();
                        DataField::new(format!("item_{}", i).as_str(), typ, true)
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(fields)
            }
            DataValue::Binary(_) => DataType::Binary,
        }
    }

    pub fn to_array(&self) -> Result<Series> {
        self.to_series_with_size(1)
    }

    pub fn to_series_with_size(&self, size: usize) -> Result<Series> {
        match self {
            DataValue::Null => {
                let array = Arc::new(NullArray::new_null(size)) as ArrayRef;
                let array: DFNullArray = array.into();
                Ok(array.into_series())
            }
            DataValue::Boolean(values) => Ok(build_constant_series! {DFBooleanArray, values, size}),
            DataValue::Int8(values) => Ok(build_constant_series! {DFInt8Array, values, size}),
            DataValue::Int16(values) => Ok(build_constant_series! {DFInt16Array, values, size}),
            DataValue::Int32(values) => Ok(build_constant_series! {DFInt32Array, values, size}),
            DataValue::Int64(values) => Ok(build_constant_series! {DFInt64Array, values, size}),
            DataValue::UInt8(values) => Ok(build_constant_series! {DFUInt8Array, values, size}),
            DataValue::UInt16(values) => Ok(build_constant_series! {DFUInt16Array, values, size}),
            DataValue::UInt32(values) => Ok(build_constant_series! {DFUInt32Array, values, size}),
            DataValue::UInt64(values) => Ok(build_constant_series! {DFUInt64Array, values, size}),
            DataValue::Float32(values) => Ok(build_constant_series! {DFFloat32Array, values, size}),
            DataValue::Float64(values) => Ok(build_constant_series! {DFFloat64Array, values, size}),

            DataValue::Utf8(values) => match values {
                None => Ok(DFUtf8Array::full_null(size).into_series()),
                Some(v) => Ok(DFUtf8Array::full(v.deref(), size).into_series()),
            },

            DataValue::Binary(values) => match values {
                None => Ok(DFBinaryArray::full_null(size).into_series()),
                Some(v) => Ok(DFBinaryArray::full(v.deref(), size).into_series()),
            },

            DataValue::List(values, data_type) => match data_type {
                DataType::Int8 => build_list_series! {Int8Type, values, size, data_type },
                DataType::Int16 => build_list_series! {Int16Type, values, size, data_type },
                DataType::Int32 => build_list_series! {Int32Type, values, size, data_type },
                DataType::Int64 => build_list_series! {Int64Type, values, size, data_type },

                DataType::UInt8 => build_list_series! {UInt8Type, values, size, data_type },
                DataType::UInt16 => build_list_series! {UInt16Type, values, size, data_type },
                DataType::UInt32 => build_list_series! {UInt32Type, values, size, data_type },
                DataType::UInt64 => build_list_series! {UInt64Type, values, size, data_type },

                DataType::Float32 => build_list_series! {Float32Type, values, size, data_type },
                DataType::Float64 => build_list_series! {Float64Type, values, size, data_type },

                DataType::Boolean => {
                    let mut builder = ListBooleanArrayBuilder::with_capacity(0, size);
                    match values {
                        Some(v) => {
                            let series = DataValue::try_into_data_array(v, data_type)?;
                            (0..size).for_each(|_| {
                                builder.append_series(&series);
                            });
                        }
                        None => (0..size).for_each(|_| {
                            builder.append_null();
                        }),
                    }
                    Ok(builder.finish().into_series())
                }
                DataType::Utf8 => {
                    let mut builder = ListUtf8ArrayBuilder::with_capacity(0, size);
                    match values {
                        Some(v) => {
                            let series = DataValue::try_into_data_array(v, data_type)?;
                            (0..size).for_each(|_| {
                                builder.append_series(&series);
                            });
                        }
                        None => (0..size).for_each(|_| {
                            builder.append_null();
                        }),
                    }
                    Ok(builder.finish().into_series())
                }
                other => Result::Err(ErrorCode::BadDataValueType(format!(
                    "Unexpected type:{} for DataValue List",
                    other
                ))),
            },
            DataValue::Struct(v) => {
                let mut arrays = vec![];
                let mut fields = vec![];
                for (i, x) in v.iter().enumerate() {
                    let xseries = x.to_series_with_size(size)?;
                    let val_array = xseries.get_array_ref();

                    fields.push(ArrowField::new(
                        format!("item_{}", i).as_str(),
                        val_array.data_type().clone(),
                        false,
                    ));

                    arrays.push(val_array);
                }
                let r = Arc::new(StructArray::from_data(fields, arrays, None)) as ArrayRef;
                Ok(r.into_series())
            }

            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{} for DataValue",
                other
            ))),
        }
    }

    pub fn to_values(&self, size: usize) -> Result<Vec<DataValue>> {
        Ok((0..size).map(|_| self.clone()).collect())
    }

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            DataValue::Int8(Some(v)) => Ok(*v as u64),
            DataValue::Int16(Some(v)) => Ok(*v as u64),
            DataValue::Int32(Some(v)) => Ok(*v as u64),
            DataValue::Int64(Some(v)) => Ok(*v as u64),
            DataValue::UInt8(Some(v)) => Ok(*v as u64),
            DataValue::UInt16(Some(v)) => Ok(*v as u64),
            DataValue::UInt32(Some(v)) => Ok(*v as u64),
            DataValue::UInt64(Some(v)) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other.data_type()
            ))),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            DataValue::Int8(Some(v)) => Ok(*v as i64),
            DataValue::Int16(Some(v)) => Ok(*v as i64),
            DataValue::Int32(Some(v)) => Ok(*v as i64),
            DataValue::Int64(Some(v)) => Ok(*v),
            DataValue::UInt8(Some(v)) => Ok(*v as i64),
            DataValue::UInt16(Some(v)) => Ok(*v as i64),
            DataValue::UInt32(Some(v)) => Ok(*v as i64),
            DataValue::UInt64(Some(v)) => Ok(*v as i64),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get i64 number",
                other.data_type()
            ))),
        }
    }
}

// Did not use std::convert:TryFrom
// Because we do not need custom type error.
pub trait DFTryFrom<T>: Sized {
    fn try_from(value: T) -> Result<Self>;
}

typed_cast_from_data_value_to_std!(Int8, i8);
typed_cast_from_data_value_to_std!(Int16, i16);
typed_cast_from_data_value_to_std!(Int32, i32);
typed_cast_from_data_value_to_std!(Int64, i64);
typed_cast_from_data_value_to_std!(UInt8, u8);
typed_cast_from_data_value_to_std!(UInt16, u16);
typed_cast_from_data_value_to_std!(UInt32, u32);
typed_cast_from_data_value_to_std!(UInt64, u64);
typed_cast_from_data_value_to_std!(Float32, f32);
typed_cast_from_data_value_to_std!(Float64, f64);
typed_cast_from_data_value_to_std!(Boolean, bool);
typed_cast_from_data_value_to_std!(Utf8, String);

std_to_data_value!(Int8, i8);
std_to_data_value!(Int16, i16);
std_to_data_value!(Int32, i32);
std_to_data_value!(Int64, i64);
std_to_data_value!(UInt8, u8);
std_to_data_value!(UInt16, u16);
std_to_data_value!(UInt32, u32);
std_to_data_value!(UInt64, u64);
std_to_data_value!(Float32, f32);
std_to_data_value!(Float64, f64);
std_to_data_value!(Boolean, bool);

impl From<&str> for DataValue {
    fn from(x: &str) -> Self {
        DataValue::Utf8(Some(x.to_string()))
    }
}

impl From<String> for DataValue {
    fn from(x: String) -> Self {
        DataValue::Utf8(Some(x))
    }
}

impl From<Option<String>> for DataValue {
    fn from(x: Option<String>) -> Self {
        DataValue::Utf8(x)
    }
}

impl From<&DataType> for DataValue {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => DataValue::Null,
            DataType::Boolean => DataValue::Boolean(None),
            DataType::Int8 => DataValue::Int8(None),
            DataType::Int16 => DataValue::Int16(None),
            DataType::Int32 => DataValue::Int32(None),
            DataType::Int64 => DataValue::Int64(None),
            DataType::UInt8 => DataValue::UInt8(None),
            DataType::UInt16 => DataValue::UInt16(None),
            DataType::UInt32 => DataValue::UInt32(None),
            DataType::UInt64 => DataValue::UInt64(None),
            DataType::Float32 => DataValue::Float32(None),
            DataType::Float64 => DataValue::Float64(None),
            DataType::Utf8 => DataValue::Utf8(None),
            DataType::Date32 => DataValue::UInt32(None),
            DataType::Date64 => DataValue::UInt64(None),
            DataType::Timestamp(_, _) => DataValue::UInt64(None),
            DataType::Interval(IntervalUnit::YearMonth) => DataValue::UInt32(None),
            DataType::Interval(IntervalUnit::DayTime) => DataValue::UInt64(None),
            DataType::List(f) => DataValue::List(None, f.data_type().clone()),
            DataType::Struct(_) => DataValue::Struct(vec![]),
            DataType::Binary => DataValue::Binary(None),
        }
    }
}

impl From<DataType> for DataValue {
    fn from(data_type: DataType) -> Self {
        DataValue::from(&data_type)
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_null() {
            return write!(f, "NULL");
        }
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(v) => format_data_value_with_option!(f, v),
            DataValue::Float32(v) => format_data_value_with_option!(f, v),
            DataValue::Float64(v) => format_data_value_with_option!(f, v),
            DataValue::Int8(v) => format_data_value_with_option!(f, v),
            DataValue::Int16(v) => format_data_value_with_option!(f, v),
            DataValue::Int32(v) => format_data_value_with_option!(f, v),
            DataValue::Int64(v) => format_data_value_with_option!(f, v),
            DataValue::UInt8(v) => format_data_value_with_option!(f, v),
            DataValue::UInt16(v) => format_data_value_with_option!(f, v),
            DataValue::UInt32(v) => format_data_value_with_option!(f, v),
            DataValue::UInt64(v) => format_data_value_with_option!(f, v),
            DataValue::Utf8(v) => format_data_value_with_option!(f, v),
            DataValue::Binary(None) => write!(f, "NULL"),
            DataValue::Binary(Some(v)) => {
                for c in v {
                    write!(f, "{:02x}", c)?;
                }
                Ok(())
            }
            DataValue::Date32(v) => format_data_value_with_option!(f, v),
            DataValue::Date64(v) => format_data_value_with_option!(f, v),
            DataValue::TimestampSecond(v) => format_data_value_with_option!(f, v),
            DataValue::TimestampMillisecond(v) => format_data_value_with_option!(f, v),
            DataValue::TimestampMicrosecond(v) => format_data_value_with_option!(f, v),
            DataValue::TimestampNanosecond(v) => format_data_value_with_option!(f, v),
            DataValue::IntervalDayTime(v) => format_data_value_with_option!(f, v),
            DataValue::IntervalYearMonth(v) => format_data_value_with_option!(f, v),
            DataValue::List(None, ..) => write!(f, "NULL"),
            DataValue::List(Some(v), ..) => {
                write!(
                    f,
                    "[{}]",
                    v.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            return write!(f, "NULL");
        }
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(v) => format_data_value_with_option!(f, v),
            DataValue::Int8(v) => format_data_value_with_option!(f, v),
            DataValue::Int16(v) => format_data_value_with_option!(f, v),
            DataValue::Int32(v) => format_data_value_with_option!(f, v),
            DataValue::Int64(v) => format_data_value_with_option!(f, v),
            DataValue::UInt8(v) => format_data_value_with_option!(f, v),
            DataValue::UInt16(v) => format_data_value_with_option!(f, v),
            DataValue::UInt32(v) => format_data_value_with_option!(f, v),
            DataValue::UInt64(v) => format_data_value_with_option!(f, v),
            DataValue::Float32(v) => format_data_value_with_option!(f, v),
            DataValue::Float64(v) => format_data_value_with_option!(f, v),
            DataValue::Utf8(v) => format_data_value_with_option!(f, v),
            DataValue::Binary(None) => write!(f, "{}", self),
            DataValue::Binary(Some(_)) => write!(f, "\"{}\"", self),
            DataValue::Date32(_) => write!(f, "Date32(\"{}\")", self),
            DataValue::Date64(_) => write!(f, "Date64(\"{}\")", self),
            DataValue::IntervalDayTime(_) => {
                write!(f, "IntervalDayTime(\"{}\")", self)
            }
            DataValue::IntervalYearMonth(_) => {
                write!(f, "IntervalYearMonth(\"{}\")", self)
            }
            DataValue::TimestampSecond(_) => write!(f, "TimestampSecond({})", self),
            DataValue::TimestampMillisecond(_) => {
                write!(f, "TimestampMillisecond({})", self)
            }
            DataValue::TimestampMicrosecond(_) => {
                write!(f, "TimestampMicrosecond({})", self)
            }
            DataValue::TimestampNanosecond(_) => {
                write!(f, "TimestampNanosecond({})", self)
            }
            DataValue::List(_, _) => write!(f, "[{}]", self),
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}

impl BinarySer for DataValue {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        let bs = serde_json::to_string(self)?;
        writer.write_string(bs)
    }

    fn serialize_to_buf<W: BufMut>(&self, writer: &mut W) -> Result<()> {
        let bs = serde_json::to_string(self)?;
        writer.write_string(bs)
    }
}

impl BinaryDe for DataValue {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let str = reader.read_string()?;
        let value: DataValue = serde_json::from_str(&str)?;
        Ok(value)
    }
}
