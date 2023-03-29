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

use anyhow::{anyhow, Error, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime};

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
    Timestamp(i64),
    Date(i32),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Tuple(Vec<Value>),
    Variant,
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
            Self::Array(_) => DataType::Array(Box::new(DataType::Null)),
            Self::Map(_) => DataType::Map(Box::new(DataType::Null)),
            Self::Tuple(_) => DataType::Tuple(vec![]),
            Self::Variant => DataType::Variant,
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
                    .timestamp_nanos(),
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
                let secs = i / 1_000_000_000;
                let nanos = (i % 1_000_000_000) as u32;
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
