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

// Borrow from apache/arrow/rust/datafusion/src/functions.rs
// See notice.md

use std::fmt;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_macros::MallocSizeOf;

use crate::prelude::*;

/// A specific value of a data type.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, MallocSizeOf)]
pub enum DataValue {
    /// Base type.
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(Vec<u8>),

    // Container struct.
    Array(Vec<DataValue>),
    Struct(Vec<DataValue>),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, MallocSizeOf)]
pub enum ValueType {
    Null,
    Boolean,
    UInt64,
    Int64,
    Float64,
    String,
    Array,
    Struct,
}

pub type DataValueRef = Arc<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    pub fn custom_display(&self, single_quote: bool) -> String {
        let s = self.to_string();
        if single_quote {
            if let DataValue::String(_) = self {
                return format!("'{}'", s);
            }
        }
        s
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            DataValue::Null => ValueType::Null,
            DataValue::Boolean(_) => ValueType::Boolean,
            DataValue::Int64(_) => ValueType::Int64,
            DataValue::UInt64(_) => ValueType::UInt64,
            DataValue::Float64(_) => ValueType::Float64,
            DataValue::String(_) => ValueType::String,
            DataValue::Array(_) => ValueType::Array,
            DataValue::Struct(_) => ValueType::Struct,
        }
    }
    // convert to minialized data type
    pub fn data_type(&self) -> DataTypePtr {
        match self {
            DataValue::Null => Arc::new(NullType {}),
            DataValue::Boolean(_) => BooleanType::arc(),
            DataValue::Int64(n) => {
                if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 {
                    return Int8Type::arc();
                }
                if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 {
                    return Int16Type::arc();
                }
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    return Int32Type::arc();
                }
                Int64Type::arc()
            }
            DataValue::UInt64(n) => {
                if *n <= u8::MAX as u64 {
                    return UInt8Type::arc();
                }
                if *n <= u16::MAX as u64 {
                    return UInt16Type::arc();
                }
                if *n <= u32::MAX as u64 {
                    return UInt32Type::arc();
                }
                UInt64Type::arc()
            }
            DataValue::Float64(_) => Float64Type::arc(),
            DataValue::String(_) => StringType::arc(),
            DataValue::Array(x) => {
                let inner_type = if x.is_empty() {
                    UInt8Type::arc()
                } else {
                    x[0].data_type()
                };
                Arc::new(ArrayType::create(inner_type))
            }
            DataValue::Struct(x) => {
                let names = (0..x.len()).map(|i| format!("{}", i)).collect::<Vec<_>>();
                let types = x.iter().map(|v| v.data_type()).collect::<Vec<_>>();
                Arc::new(StructType::create(names, types))
            }
        }
    }

    // convert to maxialized data type
    pub fn max_data_type(&self) -> DataTypePtr {
        match self {
            DataValue::Null => Arc::new(NullType {}),
            DataValue::Boolean(_) => BooleanType::arc(),
            DataValue::Int64(_) => Int64Type::arc(),
            DataValue::UInt64(_) => UInt64Type::arc(),
            DataValue::Float64(_) => Float64Type::arc(),
            DataValue::String(_) => StringType::arc(),
            DataValue::Array(x) => {
                let inner_type = if x.is_empty() {
                    UInt8Type::arc()
                } else {
                    x[0].data_type()
                };
                Arc::new(ArrayType::create(inner_type))
            }
            DataValue::Struct(x) => {
                let names = (0..x.len()).map(|i| format!("{}", i)).collect::<Vec<_>>();
                let types = x.iter().map(|v| v.data_type()).collect::<Vec<_>>();
                Arc::new(StructType::create(names, types))
            }
        }
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(self, DataValue::Int64(_) | DataValue::UInt64(_))
    }

    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        matches!(self, DataValue::Int64(_))
    }

    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(self, DataValue::UInt64(_))
    }

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            DataValue::Int64(v) if *v >= 0 => Ok(*v as u64),
            DataValue::UInt64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            DataValue::Int64(v) => Ok(*v),
            DataValue::UInt64(v) => Ok(*v as i64),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get i64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            DataValue::Boolean(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get boolean",
                other.value_type()
            ))),
        }
    }

    pub fn as_f64(&self) -> Result<f64> {
        match self {
            DataValue::Int64(v) => Ok(*v as f64),
            DataValue::UInt64(v) => Ok(*v as f64),
            DataValue::Float64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get f64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_string(&self) -> Result<Vec<u8>> {
        match self {
            DataValue::Int64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::UInt64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::Float64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::String(v) => Ok(v.to_owned()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get string",
                other.value_type()
            ))),
        }
    }

    pub fn as_const_column(&self, data_type: &DataTypePtr, size: usize) -> Result<ColumnRef> {
        data_type.create_constant_column(self, size)
    }

    #[allow(clippy::needless_late_init)]
    pub fn try_from_literal(literal: &str, radix: Option<u32>) -> Result<DataValue> {
        let radix = radix.unwrap_or(10);
        let ret = if literal.starts_with(char::from_u32(45).unwrap()) {
            match i64::from_str_radix(literal, radix) {
                Ok(n) => DataValue::Int64(n),
                Err(_) => DataValue::Float64(literal.parse::<f64>()?),
            }
        } else {
            match u64::from_str_radix(literal, radix) {
                Ok(n) => DataValue::UInt64(n),
                Err(_) => DataValue::Float64(literal.parse::<f64>()?),
            }
        };

        Ok(ret)
    }
}

// Did not use std::convert:TryFrom
// Because we do not need custom type error.
pub trait DFTryFrom<T>: Sized {
    fn try_from(value: T) -> Result<Self>;
}

impl DFTryFrom<DataValue> for Vec<u8> {
    fn try_from(value: DataValue) -> Result<Self> {
        match value {
            DataValue::String(value) => Ok(value),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error:  Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

try_cast_data_value_to_std!(u8, as_u64);
try_cast_data_value_to_std!(u16, as_u64);
try_cast_data_value_to_std!(u32, as_u64);
try_cast_data_value_to_std!(u64, as_u64);

try_cast_data_value_to_std!(i8, as_i64);
try_cast_data_value_to_std!(i16, as_i64);
try_cast_data_value_to_std!(i32, as_i64);
try_cast_data_value_to_std!(i64, as_i64);

try_cast_data_value_to_std!(f32, as_f64);
try_cast_data_value_to_std!(f64, as_f64);
try_cast_data_value_to_std!(bool, as_bool);

std_to_data_value!(Int64, i8, i64);
std_to_data_value!(Int64, i16, i64);
std_to_data_value!(Int64, i32, i64);
std_to_data_value!(Int64, i64, i64);
std_to_data_value!(UInt64, u8, u64);
std_to_data_value!(UInt64, u16, u64);
std_to_data_value!(UInt64, u32, u64);
std_to_data_value!(UInt64, u64, u64);
std_to_data_value!(Float64, f32, f64);
std_to_data_value!(Float64, f64, f64);
std_to_data_value!(Boolean, bool, bool);

impl From<&[u8]> for DataValue {
    fn from(x: &[u8]) -> Self {
        DataValue::String(x.to_vec())
    }
}

impl From<Option<&[u8]>> for DataValue {
    fn from(x: Option<&[u8]>) -> Self {
        let x = x.map(|c| c.to_vec());
        DataValue::from(x)
    }
}

impl From<Vec<u8>> for DataValue {
    fn from(x: Vec<u8>) -> Self {
        DataValue::String(x)
    }
}

impl From<Option<Vec<u8>>> for DataValue {
    fn from(x: Option<Vec<u8>>) -> Self {
        match x {
            Some(v) => DataValue::String(v),
            None => DataValue::Null,
        }
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::UInt64(v) => write!(f, "{}", v),
            DataValue::String(v) => match std::str::from_utf8(v) {
                Ok(v) => write!(f, "{}", v),
                Err(_e) => {
                    for c in v {
                        write!(f, "{:02x}", c)?;
                    }
                    Ok(())
                }
            },
            DataValue::Array(v) => {
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
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::UInt64(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::String(_) => write!(f, "{}", self),
            DataValue::Array(_) => write!(f, "{}", self),
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}
