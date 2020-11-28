// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datavalues::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    NullArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use crate::datavalues::{DataArrayRef, DataType};
use crate::error::{FuseQueryError, FuseQueryResult};

/// A specific value of a data type.
#[derive(Clone, PartialEq)]
pub enum DataValue {
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
    String(Option<String>),
}

pub type DataValueRef = Box<DataValue>;

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
                | DataValue::String(None)
        )
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DataValue::Null => (DataType::Null),
            DataValue::Boolean(_) => (DataType::Boolean),
            DataValue::Int8(_) => (DataType::Int8),
            DataValue::Int16(_) => (DataType::Int16),
            DataValue::Int32(_) => (DataType::Int32),
            DataValue::Int64(_) => (DataType::Int64),
            DataValue::UInt8(_) => (DataType::UInt8),
            DataValue::UInt16(_) => (DataType::UInt16),
            DataValue::UInt32(_) => (DataType::UInt32),
            DataValue::UInt64(_) => (DataType::UInt64),
            DataValue::Float32(_) => (DataType::Float32),
            DataValue::Float64(_) => (DataType::Float64),
            DataValue::String(_) => (DataType::Utf8),
        }
    }

    pub fn to_array(&self, size: usize) -> FuseQueryResult<DataArrayRef> {
        match self {
            DataValue::Null => Ok(Arc::new(NullArray::new(size))),
            DataValue::Boolean(v) => {
                Ok(Arc::new(BooleanArray::from(vec![*v; size])) as DataArrayRef)
            }
            DataValue::Int8(v) => Ok(Arc::new(Int8Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::Int16(v) => Ok(Arc::new(Int16Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::UInt8(v) => Ok(Arc::new(UInt8Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::UInt16(v) => Ok(Arc::new(UInt16Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::UInt32(v) => Ok(Arc::new(UInt32Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::UInt64(v) => Ok(Arc::new(UInt64Array::from(vec![*v; size])) as DataArrayRef),
            DataValue::Float32(v) => {
                Ok(Arc::new(Float32Array::from(vec![*v; size])) as DataArrayRef)
            }
            DataValue::Float64(v) => {
                Ok(Arc::new(Float64Array::from(vec![*v; size])) as DataArrayRef)
            }
            DataValue::String(v) => Ok(Arc::new(StringArray::from(vec![v.as_deref(); size]))),
        }
    }
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);
impl_try_from!(Int32, i32);
impl_try_from!(Int64, i64);
impl_try_from!(UInt8, u8);
impl_try_from!(UInt16, u16);
impl_try_from!(UInt32, u32);
impl_try_from!(UInt64, u64);
impl_try_from!(Float32, f32);
impl_try_from!(Float64, f64);
impl_try_from!(Boolean, bool);

impl TryFrom<&DataType> for DataValue {
    type Error = FuseQueryError;

    fn try_from(data_type: &DataType) -> FuseQueryResult<Self> {
        Ok(match data_type {
            DataType::Null => (DataValue::Null),
            DataType::Boolean => (DataValue::Boolean(None)),
            DataType::Int8 => (DataValue::Int8(None)),
            DataType::Int16 => (DataValue::Int16(None)),
            DataType::Int32 => (DataValue::Int32(None)),
            DataType::Int64 => (DataValue::Int64(None)),
            DataType::UInt8 => (DataValue::UInt8(None)),
            DataType::UInt16 => (DataValue::UInt16(None)),
            DataType::UInt32 => (DataValue::UInt32(None)),
            DataType::UInt64 => (DataValue::UInt64(None)),
            DataType::Float32 => (DataValue::Float32(None)),
            DataType::Float64 => (DataValue::Float64(None)),
            _ => {
                return Err(FuseQueryError::Internal(format!(
                    "Unsupported try_from() for data type: {:?}",
                    data_type
                )))
            }
        })
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "Null"),
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
            DataValue::String(v) => format_data_value_with_option!(f, v),
        }
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "Null"),
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
            DataValue::String(v) => format_data_value_with_option!(f, v),
        }
    }
}
