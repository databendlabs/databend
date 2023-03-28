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
use enum_as_inner::EnumAsInner;

use crate::schema::DataType;

#[derive(Debug, EnumAsInner)]
pub enum NumberValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(f32),
    Float64(f64),
}

#[derive(Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    String(String),
    Number(NumberValue),
    // TODO:(everpcpc) Decimal(DecimalValue),
    Decimal(String),
    Timestamp(u64),
    Date(u16),
    Nullable(Box<Value>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Tuple(Vec<Value>),
    Variant,
    Generic(usize, Vec<u8>),
}

impl TryFrom<(DataType, String)> for Value {
    type Error = Error;

    fn try_from((t, v): (DataType, String)) -> Result<Self> {
        match t {
            DataType::String => Ok(Self::String(v)),
            // TODO:(everpcpc) parse other types
            _ => Ok(Self::String(v)),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::String(s) => Ok(s),
            _ => Err(anyhow!("Error converting value to string")),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Boolean(b) => Ok(b),
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
