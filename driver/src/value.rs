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

use anyhow::{anyhow, Error, Ok, Result};

use chrono::{Datelike, NaiveDate, NaiveDateTime};

#[cfg(feature = "flight-sql")]
use {
    arrow::array::AsArray,
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
    Decimal(String),
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
            Self::Decimal(_) => DataType::Decimal,
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

impl TryFrom<(DataType, String)> for Value {
    type Error = Error;

    fn try_from((t, v): (DataType, String)) -> Result<Self> {
        match t {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::Boolean(v == "1")),
            DataType::String => Ok(Self::String(v)),
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
            DataType::Decimal => Ok(Self::Decimal(v)),
            DataType::Timestamp => Ok(Self::Timestamp(
                chrono::NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.6f")?
                    .timestamp_micros(),
            )),
            DataType::Date => Ok(Self::Date(
                // 719_163 is the number of days from 0000-01-01 to 1970-01-01
                chrono::NaiveDate::parse_from_str(&v, "%Y-%m-%d")?.num_days_from_ce() - 719_163,
            )),
            // TODO:(everpcpc) handle complex types
            _ => Ok(Self::String(v)),
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
                None => Err(anyhow!("cannot convert {:?} to boolean", array)),
            },
            ArrowDataType::Int8 => match array.as_any().downcast_ref::<Int8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int8(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to int8", array)),
            },
            ArrowDataType::Int16 => match array.as_any().downcast_ref::<Int16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int16(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to int16", array)),
            },
            ArrowDataType::Int32 => match array.as_any().downcast_ref::<Int32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int32(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to int32", array)),
            },
            ArrowDataType::Int64 => match array.as_any().downcast_ref::<Int64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int64(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to int64", array)),
            },
            ArrowDataType::UInt8 => match array.as_any().downcast_ref::<UInt8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt8(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to uint8", array)),
            },
            ArrowDataType::UInt16 => match array.as_any().downcast_ref::<UInt16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt16(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to uint16", array)),
            },
            ArrowDataType::UInt32 => match array.as_any().downcast_ref::<UInt32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt32(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to uint32", array)),
            },
            ArrowDataType::UInt64 => match array.as_any().downcast_ref::<UInt64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt64(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to uint64", array)),
            },
            ArrowDataType::Float32 => match array.as_any().downcast_ref::<Float32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float32(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to float32", array)),
            },
            ArrowDataType::Float64 => match array.as_any().downcast_ref::<Float64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float64(array.value(seq)))),
                None => Err(anyhow!("cannot convert {:?} to float64", array)),
            },

            ArrowDataType::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
                Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                None => Err(anyhow!("cannot convert {:?} to binary", array)),
            },
            ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
                match array.as_any().downcast_ref::<LargeBinaryArray>() {
                    Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                    None => Err(anyhow!("cannot convert {:?} to large binary", array)),
                }
            }
            ArrowDataType::Utf8 => match array.as_any().downcast_ref::<StringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(anyhow!("cannot convert {:?} to string", array)),
            },
            ArrowDataType::LargeUtf8 => match array.as_any().downcast_ref::<LargeStringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(anyhow!("cannot convert {:?} to large string", array)),
            },
            // we only support timestamp in microsecond in databend
            ArrowDataType::Timestamp(unit, tz) => {
                match array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Some(array) => {
                        if unit != &TimeUnit::Microsecond {
                            return Err(anyhow!(
                                "unsupported timestamp unit: {:?}, only support microsecond",
                                unit
                            ));
                        }
                        let ts = array.value(seq);
                        match tz {
                            None => Ok(Value::Timestamp(ts)),
                            Some(tz) => Err(anyhow!("non-UTC timezone not supported: {:?}", tz)),
                        }
                    }
                    None => Err(anyhow!("cannot convert {:?} to timestamp", array)),
                }
            }
            ArrowDataType::Date32 => match array.as_any().downcast_ref::<Date32Array>() {
                Some(array) => Ok(Value::Date(array.value(seq))),
                None => Err(anyhow!("cannot convert {:?} to date", array)),
            },
            ArrowDataType::Date64
            | ArrowDataType::Time32(_)
            | ArrowDataType::Time64(_)
            | ArrowDataType::Interval(_)
            | ArrowDataType::Duration(_) => {
                Err(anyhow!("unsupported data type: {:?}", array.data_type()))
            }
            ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
                let v = array.as_list_opt::<i64>().unwrap().value(seq);
                Ok(Value::String(format!("{:?}", v)))
            }
            // Struct(Vec<Field>),
            // Decimal128(u8, i8),
            // Decimal256(u8, i8),
            // Map(Box<Field>, bool),
            // RunEndEncoded(Box<Field>, Box<Field>),
            _ => Err(anyhow!("unsupported data type: {:?}", array.data_type())),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::String(s) => Ok(s),
            _ => Err(anyhow!("Error converting value to String")),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Boolean(b) => Ok(b),
            Value::Number(n) => Ok(n != NumberValue::Int8(0)),
            _ => Err(anyhow!("Error converting value to bool")),
        }
    }
}

impl TryFrom<Value> for i8 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Int8(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to i8")),
        }
    }
}

impl TryFrom<Value> for i16 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Int16(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to i16")),
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Int32(i)) => Ok(i),
            Value::Date(i) => Ok(i),
            _ => Err(anyhow!("Error converting value to i32")),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Int64(i)) => Ok(i),
            Value::Timestamp(i) => Ok(i),
            _ => Err(anyhow!("Error converting value to i64")),
        }
    }
}

impl TryFrom<Value> for u8 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::UInt8(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to u8")),
        }
    }
}

impl TryFrom<Value> for u16 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::UInt16(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to u16")),
        }
    }
}

impl TryFrom<Value> for u32 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::UInt32(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to u32")),
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::UInt64(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to u64")),
        }
    }
}

impl TryFrom<Value> for f32 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Float32(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to f32")),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Number(NumberValue::Float64(i)) => Ok(i),
            _ => Err(anyhow!("Error converting value to f64")),
        }
    }
}

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
                    None => Err(anyhow!("Empty timestamp")),
                }
            }
            _ => Err(anyhow!("Error converting value to NaiveDateTime")),
        }
    }
}

impl TryFrom<Value> for NaiveDate {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Date(i) => {
                let days = i + 719_163;
                let d = NaiveDate::from_num_days_from_ce_opt(days);
                match d {
                    Some(d) => Ok(d),
                    None => Err(anyhow!("Empty date")),
                }
            }
            _ => Err(anyhow!("Error converting value to NaiveDate")),
        }
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::Number(NumberValue::UInt8(v))
    }
}
