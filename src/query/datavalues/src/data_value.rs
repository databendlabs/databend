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

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use ordered_float::OrderedFloat;
use serde_json::json;

use crate::prelude::*;
use crate::type_coercion::merge_types;

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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ValueType {
    Null,
    Boolean,
    UInt64,
    Int64,
    Float64,
    String,
    Array,
    Struct,
    Variant,
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
            DataValue::Variant(_) => ValueType::Variant,
        }
    }

    /// Get the minimal memory sized data type.
    pub fn data_type(&self) -> DataTypeImpl {
        match self {
            DataValue::Null => DataTypeImpl::Null(NullType {}),
            DataValue::Boolean(_) => BooleanType::new_impl(),
            DataValue::Int64(n) => {
                if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 {
                    return Int8Type::new_impl();
                }
                if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 {
                    return Int16Type::new_impl();
                }
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    return Int32Type::new_impl();
                }
                Int64Type::new_impl()
            }
            DataValue::UInt64(n) => {
                if *n <= u8::MAX as u64 {
                    return UInt8Type::new_impl();
                }
                if *n <= u16::MAX as u64 {
                    return UInt16Type::new_impl();
                }
                if *n <= u32::MAX as u64 {
                    return UInt32Type::new_impl();
                }
                UInt64Type::new_impl()
            }
            DataValue::Float64(_) => Float64Type::new_impl(),
            DataValue::String(_) => StringType::new_impl(),
            DataValue::Array(vals) => {
                let inner_type = if vals.is_empty() {
                    NullType::new_impl()
                } else {
                    vals.iter()
                        .fold(Ok(vals[0].data_type()), |acc, v| {
                            merge_types(&acc?, &v.data_type())
                        })
                        .unwrap()
                };
                ArrayType::new_impl(inner_type)
            }
            DataValue::Struct(vals) => {
                let types = vals.iter().map(|v| v.data_type()).collect::<Vec<_>>();
                StructType::new_impl(None, types)
            }
            DataValue::Variant(_) => VariantType::new_impl(),
        }
    }

    /// Get the maximum memory sized data type
    pub fn max_data_type(&self) -> DataTypeImpl {
        match self {
            DataValue::Null => DataTypeImpl::Null(NullType {}),
            DataValue::Boolean(_) => BooleanType::new_impl(),
            DataValue::Int64(_) => Int64Type::new_impl(),
            DataValue::UInt64(_) => UInt64Type::new_impl(),
            DataValue::Float64(_) => Float64Type::new_impl(),
            DataValue::String(_) => StringType::new_impl(),
            DataValue::Array(vals) => {
                let inner_type = if vals.is_empty() {
                    NullType::new_impl()
                } else {
                    vals.iter()
                        .fold(Ok(vals[0].max_data_type()), |acc, v| {
                            merge_types(&acc?, &v.max_data_type())
                        })
                        .unwrap()
                };
                ArrayType::new_impl(inner_type)
            }
            DataValue::Struct(vals) => {
                let types = vals.iter().map(|v| v.max_data_type()).collect::<Vec<_>>();
                StructType::new_impl(None, types)
            }
            DataValue::Variant(_) => VariantType::new_impl(),
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

    #[inline]
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataValue::Int64(_) | DataValue::UInt64(_) | DataValue::Float64(_)
        )
    }

    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, DataValue::Float64(_))
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
            DataValue::Variant(v) => Ok(v.to_string().into_bytes()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get string",
                other.value_type()
            ))),
        }
    }

    pub fn as_array(&self) -> Result<Vec<DataValue>> {
        match self {
            DataValue::Array(vals) => Ok(vals.to_vec()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get array values",
                other.value_type()
            ))),
        }
    }

    pub fn as_struct(&self) -> Result<Vec<DataValue>> {
        match self {
            DataValue::Struct(vals) => Ok(vals.to_vec()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get struct values",
                other.value_type()
            ))),
        }
    }

    pub fn as_const_column(&self, data_type: &DataTypeImpl, size: usize) -> Result<ColumnRef> {
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

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.value_type() == other.value_type() {
            return match (self, other) {
                (DataValue::Null, DataValue::Null) => Ordering::Equal,
                (DataValue::Boolean(v1), DataValue::Boolean(v2)) => v1.cmp(v2),
                (DataValue::UInt64(v1), DataValue::UInt64(v2)) => v1.cmp(v2),
                (DataValue::Int64(v1), DataValue::Int64(v2)) => v1.cmp(v2),
                (DataValue::Float64(v1), DataValue::Float64(v2)) => {
                    OrderedFloat::from(*v1).cmp(&OrderedFloat::from(*v2))
                }
                (DataValue::String(v1), DataValue::String(v2)) => v1.cmp(v2),
                (DataValue::Array(v1), DataValue::Array(v2)) => {
                    for (l, r) in v1.iter().zip(v2) {
                        let cmp = l.cmp(r);
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                    }
                    v1.len().cmp(&v2.len())
                }
                (DataValue::Struct(v1), DataValue::Struct(v2)) => {
                    for (l, r) in v1.iter().zip(v2.iter()) {
                        let cmp = l.cmp(r);
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                    }
                    v1.len().cmp(&v2.len())
                }
                (DataValue::Variant(v1), DataValue::Variant(v2)) => v1.cmp(v2),
                _ => unreachable!(),
            };
        }

        if self.is_null() {
            return Ordering::Greater;
        }

        if other.is_null() {
            return Ordering::Less;
        }

        if !self.is_numeric() || !other.is_numeric() {
            panic!(
                "Cannot compare different types with {:?} and {:?}",
                self.value_type(),
                other.value_type()
            );
        }

        if self.is_float() || other.is_float() {
            return OrderedFloat::from(self.as_f64().unwrap())
                .cmp(&OrderedFloat::from(other.as_f64().unwrap()));
        }

        self.as_i64().unwrap().cmp(&other.as_i64().unwrap())
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

impl DFTryFrom<DataValue> for VariantValue {
    fn try_from(value: DataValue) -> Result<Self> {
        match value {
            DataValue::Null => Ok(VariantValue::from(serde_json::Value::Null)),
            DataValue::Boolean(v) => Ok(VariantValue::from(json!(v))),
            DataValue::Int64(v) => Ok(VariantValue::from(json!(v))),
            DataValue::UInt64(v) => Ok(VariantValue::from(json!(v))),
            DataValue::Float64(v) => Ok(VariantValue::from(json!(v))),
            DataValue::String(v) => Ok(VariantValue::from(json!(v))),
            DataValue::Array(v) => Ok(VariantValue::from(json!(v))),
            DataValue::Struct(v) => Ok(VariantValue::from(json!(v))),
            DataValue::Variant(v) => Ok(v),
        }
    }
}

impl DFTryFrom<&DataValue> for VariantValue {
    fn try_from(value: &DataValue) -> Result<Self> {
        match value {
            DataValue::Null => Ok(VariantValue::from(serde_json::Value::Null)),
            DataValue::Boolean(v) => Ok(VariantValue::from(json!(*v as bool))),
            DataValue::Int64(v) => Ok(VariantValue::from(json!(*v as i64))),
            DataValue::UInt64(v) => Ok(VariantValue::from(json!(*v as u64))),
            DataValue::Float64(v) => Ok(VariantValue::from(json!(*v as f64))),
            DataValue::String(v) => Ok(VariantValue::from(json!(
                String::from_utf8(v.to_vec()).unwrap()
            ))),
            DataValue::Array(v) => Ok(VariantValue::from(json!(*v))),
            DataValue::Struct(v) => Ok(VariantValue::from(json!(*v))),
            DataValue::Variant(v) => Ok(v.to_owned()),
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

impl From<VariantValue> for DataValue {
    fn from(x: VariantValue) -> Self {
        DataValue::Variant(x)
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

/// SQL style format
pub fn format_datavalue_sql(value: &DataValue) -> String {
    match value {
        DataValue::String(_) | DataValue::Variant(_) => format!("'{}'", value),
        DataValue::Float64(value) => format!("'{:?}'", value),
        _ => value.to_string(),
    }
}
