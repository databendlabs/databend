// Copyright 2023 Datafuse Labs.
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

use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::error::{ConvertError, Error, Result};

// Thu 1970-01-01 is R.D. 719163
const DAYS_FROM_CE: i32 = 719_163;

#[cfg(feature = "flight-sql")]
use {
    arrow_array::{
        Array as ArrowArray, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    arrow_schema::{DataType as ArrowDataType, Field as ArrowField, TimeUnit},
    std::sync::Arc,
};

use crate::schema::{DataType, NumberDataType};

#[derive(Clone, Debug, PartialEq)]
pub enum NumberValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    String(String),
    Number(NumberValue),
    // TODO:(everpcpc) Decimal(DecimalValue),
    // Decimal(String),
    /// Microseconds from 1970-01-01 00:00:00 UTC
    Timestamp(i64),
    Date(i32),
    // Array(Vec<Value>),
    // Map(Vec<(Value, Value)>),
    // Tuple(Vec<Value>),
    // Variant,
    // Generic(usize, Vec<u8>),
}

impl Value {
    pub(crate) fn get_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::String,
            Self::Number(n) => match n {
                NumberValue::Int8(_) => DataType::Number(NumberDataType::Int8),
                NumberValue::Int16(_) => DataType::Number(NumberDataType::Int16),
                NumberValue::Int32(_) => DataType::Number(NumberDataType::Int32),
                NumberValue::Int64(_) => DataType::Number(NumberDataType::Int64),
                NumberValue::UInt8(_) => DataType::Number(NumberDataType::UInt8),
                NumberValue::UInt16(_) => DataType::Number(NumberDataType::UInt16),
                NumberValue::UInt32(_) => DataType::Number(NumberDataType::UInt32),
                NumberValue::UInt64(_) => DataType::Number(NumberDataType::UInt64),
                NumberValue::Float32(_) => DataType::Number(NumberDataType::Float32),
                NumberValue::Float64(_) => DataType::Number(NumberDataType::Float64),
            },
            // Self::Decimal(_) => DataType::Decimal,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::Date(_) => DataType::Date,
            // TODO:(everpcpc) fix nested type
            // Self::Array(v) => DataType::Array(Box::new(v[0].get_type())),
            // Self::Map(_) => DataType::Map(Box::new(DataType::Null)),
            // Self::Tuple(_) => DataType::Tuple(vec![]),
            // Self::Variant => DataType::Variant,
        }
    }
}

impl TryFrom<(&DataType, &str)> for Value {
    type Error = Error;

    fn try_from((t, v): (&DataType, &str)) -> Result<Self> {
        match t {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::Boolean(v == "1")),
            DataType::String => Ok(Self::String(v.to_string())),
            DataType::Number(NumberDataType::Int8) => {
                Ok(Self::Number(NumberValue::Int8(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int16) => {
                Ok(Self::Number(NumberValue::Int16(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int32) => {
                Ok(Self::Number(NumberValue::Int32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int64) => {
                Ok(Self::Number(NumberValue::Int64(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt8) => {
                Ok(Self::Number(NumberValue::UInt8(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt16) => {
                Ok(Self::Number(NumberValue::UInt16(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt32) => {
                Ok(Self::Number(NumberValue::UInt32(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt64) => {
                Ok(Self::Number(NumberValue::UInt64(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float32) => {
                Ok(Self::Number(NumberValue::Float32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float64) => {
                Ok(Self::Number(NumberValue::Float64(v.parse()?)))
            }
            // DataType::Decimal => Ok(Self::Decimal(v)),
            DataType::Timestamp => Ok(Self::Timestamp(
                chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.6f")?
                    .timestamp_micros(),
            )),
            DataType::Date => Ok(Self::Date(
                chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")?.num_days_from_ce() - DAYS_FROM_CE,
            )),
            // TODO:(everpcpc) handle complex types
            _ => Ok(Self::String(v.to_string())),
        }
    }
}

#[cfg(feature = "flight-sql")]
impl TryFrom<(&ArrowField, &Arc<dyn ArrowArray>, usize)> for Value {
    type Error = Error;
    fn try_from(
        (field, array, seq): (&ArrowField, &Arc<dyn ArrowArray>, usize),
    ) -> std::result::Result<Self, Self::Error> {
        match field.data_type() {
            ArrowDataType::Null => Ok(Value::Null),
            ArrowDataType::Boolean => match array.as_any().downcast_ref::<BooleanArray>() {
                Some(array) => Ok(Value::Boolean(array.value(seq))),
                None => Err(ConvertError::new("bool", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int8 => match array.as_any().downcast_ref::<Int8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int8(array.value(seq)))),
                None => Err(ConvertError::new("int8", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int16 => match array.as_any().downcast_ref::<Int16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int16(array.value(seq)))),
                None => Err(ConvertError::new("int16", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int32 => match array.as_any().downcast_ref::<Int32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int32(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int64 => match array.as_any().downcast_ref::<Int64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int64(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt8 => match array.as_any().downcast_ref::<UInt8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt8(array.value(seq)))),
                None => Err(ConvertError::new("uint8", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt16 => match array.as_any().downcast_ref::<UInt16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt16(array.value(seq)))),
                None => Err(ConvertError::new("uint16", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt32 => match array.as_any().downcast_ref::<UInt32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt32(array.value(seq)))),
                None => Err(ConvertError::new("uint32", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt64 => match array.as_any().downcast_ref::<UInt64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt64(array.value(seq)))),
                None => Err(ConvertError::new("uint64", format!("{:?}", array)).into()),
            },
            ArrowDataType::Float32 => match array.as_any().downcast_ref::<Float32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float32(array.value(seq)))),
                None => Err(ConvertError::new("float32", format!("{:?}", array)).into()),
            },
            ArrowDataType::Float64 => match array.as_any().downcast_ref::<Float64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float64(array.value(seq)))),
                None => Err(ConvertError::new("float64", format!("{:?}", array)).into()),
            },

            ArrowDataType::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
                Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                None => Err(ConvertError::new("binary", format!("{:?}", array)).into()),
            },
            ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
                match array.as_any().downcast_ref::<LargeBinaryArray>() {
                    Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                    None => Err(ConvertError::new("large binary", format!("{:?}", array)).into()),
                }
            }
            ArrowDataType::Utf8 => match array.as_any().downcast_ref::<StringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("string", format!("{:?}", array)).into()),
            },
            ArrowDataType::LargeUtf8 => match array.as_any().downcast_ref::<LargeStringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("large string", format!("{:?}", array)).into()),
            },
            // we only support timestamp in microsecond in databend
            ArrowDataType::Timestamp(unit, tz) => {
                match array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Some(array) => {
                        if unit != &TimeUnit::Microsecond {
                            return Err(ConvertError::new("timestamp", format!("{:?}", array))
                                .with_message(format!(
                                    "unsupported timestamp unit: {:?}, only support microsecond",
                                    unit
                                ))
                                .into());
                        }
                        let ts = array.value(seq);
                        match tz {
                            None => Ok(Value::Timestamp(ts)),
                            Some(tz) => Err(ConvertError::new("timestamp", format!("{:?}", array))
                                .with_message(format!("non-UTC timezone not supported: {:?}", tz))
                                .into()),
                        }
                    }
                    None => Err(ConvertError::new("timestamp", format!("{:?}", array)).into()),
                }
            }
            ArrowDataType::Date32 => match array.as_any().downcast_ref::<Date32Array>() {
                Some(array) => Ok(Value::Date(array.value(seq))),
                None => Err(ConvertError::new("date", format!("{:?}", array)).into()),
            },
            ArrowDataType::Date64
            | ArrowDataType::Time32(_)
            | ArrowDataType::Time64(_)
            | ArrowDataType::Interval(_)
            | ArrowDataType::Duration(_) => {
                Err(ConvertError::new("unsupported data type", format!("{:?}", array)).into())
            }
            // ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            //     let v = array.as_list_opt::<i64>().unwrap().value(seq);
            //     Ok(Value::String(format!("{:?}", v)))
            // }
            // Struct(Vec<Field>),
            // Decimal128(u8, i8),
            // Decimal256(u8, i8),
            // Map(Box<Field>, bool),
            // RunEndEncoded(Box<Field>, Box<Field>),
            _ => Err(ConvertError::new("unsupported data type", format!("{:?}", array)).into()),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::String(s) => Ok(s),
            _ => Err(ConvertError::new("string", format!("{:?}", val)).into()),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Boolean(b) => Ok(b),
            Value::Number(n) => Ok(n != NumberValue::Int8(0)),
            _ => Err(ConvertError::new("bool", format!("{:?}", val)).into()),
        }
    }
}

// This macro implements TryFrom for NumberValue
macro_rules! impl_try_from_number_value {
    ($($t:ty),*) => {
        $(
            impl TryFrom<Value> for $t {
                type Error = Error;
                fn try_from(val: Value) -> Result<Self> {
                    match val {
                        Value::Number(NumberValue::Int8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float64(i)) => Ok(i as $t),
                        Value::Date(i) => Ok(i as $t),
                        Value::Timestamp(i) => Ok(i as $t),
                        _ => Err(ConvertError::new("number", format!("{:?}", val)).into()),
                    }
                }
            }
        )*
    };
}

impl_try_from_number_value!(u8);
impl_try_from_number_value!(u16);
impl_try_from_number_value!(u32);
impl_try_from_number_value!(u64);
impl_try_from_number_value!(i8);
impl_try_from_number_value!(i16);
impl_try_from_number_value!(i32);
impl_try_from_number_value!(i64);
impl_try_from_number_value!(f32);
impl_try_from_number_value!(f64);

impl TryFrom<Value> for NaiveDateTime {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Timestamp(i) => {
                let secs = i / 1_000_000;
                let nanos = ((i % 1_000_000) * 1000) as u32;
                let t = NaiveDateTime::from_timestamp_opt(secs, nanos);
                match t {
                    Some(t) => Ok(t),
                    None => Err(ConvertError::new("NaiveDateTime", "".to_string()).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDateTime", format!("{}", val)).into()),
        }
    }
}

impl TryFrom<Value> for NaiveDate {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                let d = NaiveDate::from_num_days_from_ce_opt(days);
                match d {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::new("NaiveDate", "".to_string()).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDate", format!("{}", val)).into()),
        }
    }
}

impl std::fmt::Display for NumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberValue::Int8(i) => write!(f, "{}", i),
            NumberValue::Int16(i) => write!(f, "{}", i),
            NumberValue::Int32(i) => write!(f, "{}", i),
            NumberValue::Int64(i) => write!(f, "{}", i),
            NumberValue::UInt8(i) => write!(f, "{}", i),
            NumberValue::UInt16(i) => write!(f, "{}", i),
            NumberValue::UInt32(i) => write!(f, "{}", i),
            NumberValue::UInt64(i) => write!(f, "{}", i),
            NumberValue::Float32(i) => write!(f, "{}", i),
            NumberValue::Float64(i) => write!(f, "{}", i),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Number(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
            Value::Timestamp(i) => {
                let secs = i / 1_000_000;
                let nanos = ((i % 1_000_000) * 1000) as u32;
                let t = NaiveDateTime::from_timestamp_opt(secs, nanos).unwrap_or_default();
                write!(f, "{}", t)
            }
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                let d = NaiveDate::from_num_days_from_ce_opt(days).unwrap_or_default();
                write!(f, "{}", d)
            }
        }
    }
}
