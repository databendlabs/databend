// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datavalues::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use crate::datavalues::{DataArrayRef, DataType};
use crate::error::{Error, Result};

/// A specific value of a data type.
#[derive(Clone, PartialEq)]
pub enum DataValue {
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

    pub fn data_type(&self) -> Result<DataType> {
        match self {
            DataValue::Boolean(_) => Ok(DataType::Boolean),
            DataValue::Int8(_) => Ok(DataType::Int8),
            DataValue::Int16(_) => Ok(DataType::Int16),
            DataValue::Int32(_) => Ok(DataType::Int32),
            DataValue::Int64(_) => Ok(DataType::Int64),
            DataValue::UInt8(_) => Ok(DataType::UInt8),
            DataValue::UInt16(_) => Ok(DataType::UInt16),
            DataValue::UInt32(_) => Ok(DataType::UInt32),
            DataValue::UInt64(_) => Ok(DataType::UInt64),
            DataValue::Float32(_) => Ok(DataType::Float32),
            DataValue::Float64(_) => Ok(DataType::Float64),
            DataValue::String(_) => Ok(DataType::Utf8),
        }
    }

    pub fn to_array(&self) -> Result<DataArrayRef> {
        match self {
            DataValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v])) as DataArrayRef),
            DataValue::Int8(v) => Ok(Arc::new(Int8Array::from(vec![*v])) as DataArrayRef),
            DataValue::Int16(v) => Ok(Arc::new(Int16Array::from(vec![*v])) as DataArrayRef),
            DataValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v])) as DataArrayRef),
            DataValue::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v])) as DataArrayRef),
            DataValue::UInt8(v) => Ok(Arc::new(UInt8Array::from(vec![*v])) as DataArrayRef),
            DataValue::UInt16(v) => Ok(Arc::new(UInt16Array::from(vec![*v])) as DataArrayRef),
            DataValue::UInt32(v) => Ok(Arc::new(UInt32Array::from(vec![*v])) as DataArrayRef),
            DataValue::UInt64(v) => Ok(Arc::new(UInt64Array::from(vec![*v])) as DataArrayRef),
            DataValue::Float32(v) => Ok(Arc::new(Float32Array::from(vec![*v])) as DataArrayRef),
            DataValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v])) as DataArrayRef),
            DataValue::String(v) => Ok(Arc::new(StringArray::from(vec![v.as_deref()]))),
        }
    }
}

impl TryFrom<&DataType> for DataValue {
    type Error = Error;

    fn try_from(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
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
                return Err(Error::Unsupported(format!(
                    "Unsupported try_from() for data type: {:?}",
                    data_type
                )))
            }
        })
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
