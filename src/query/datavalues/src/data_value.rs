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

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::VariantValue;

/// A specific value of a data type.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
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

    // Custom type.
    Variant(VariantValue),
}

impl Eq for DataValue {}

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

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            DataValue::Int64(v) if *v >= 0 => Ok(*v as u64),
            DataValue::UInt64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get u64 number",
                other
            ))),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            DataValue::Int64(v) => Ok(*v),
            DataValue::UInt64(v) => Ok(*v as i64),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get i64 number",
                other
            ))),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            DataValue::Boolean(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get boolean",
                other
            ))),
        }
    }

    pub fn as_f64(&self) -> Result<f64> {
        match self {
            DataValue::Int64(v) => Ok(*v as f64),
            DataValue::UInt64(v) => Ok(*v as f64),
            DataValue::Float64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get f64 number",
                other
            ))),
        }
    }

    pub fn as_string(&self) -> Result<Vec<u8>> {
        match self {
            DataValue::Int64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::UInt64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::Float64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::String(v) => Ok(v.to_owned()),
            DataValue::Variant(v) => Ok(v.to_string().into_bytes()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get string",
                other
            ))),
        }
    }

    pub fn as_array(&self) -> Result<Vec<DataValue>> {
        match self {
            DataValue::Array(vals) => Ok(vals.to_vec()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get array values",
                other
            ))),
        }
    }

    pub fn as_struct(&self) -> Result<Vec<DataValue>> {
        match self {
            DataValue::Struct(vals) => Ok(vals.to_vec()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected:{:?} to get struct values",
                other
            ))),
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            DataValue::Null => {}
            DataValue::Boolean(v) => v.hash(state),
            DataValue::UInt64(v) => v.hash(state),
            DataValue::Int64(v) => v.hash(state),
            DataValue::Float64(v) => v.to_bits().hash(state),
            DataValue::String(v) => v.hash(state),
            DataValue::Array(v) => v.hash(state),
            DataValue::Struct(v) => v.hash(state),
            DataValue::Variant(v) => v.hash(state),
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
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataValue::Struct(v) => {
                write!(
                    f,
                    "({})",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataValue::Variant(v) => write!(f, "{}", v),
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
            DataValue::Struct(_) => write!(f, "{}", self),
            DataValue::Variant(v) => write!(f, "{}", v),
        }
    }
}
