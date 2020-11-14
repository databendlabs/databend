// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};

use crate::datavalues::{DataArrayRef, DataType};
use crate::error::{Error, Result};

/// A specific value of a data type.
#[derive(Clone, PartialEq)]
pub enum DataValue {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    UInt64(Option<u64>),
    Float64(Option<f64>),
    String(Option<String>),
}

pub type DataValueRef = Box<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            DataValue::Boolean(None)
                | DataValue::Int64(None)
                | DataValue::UInt64(None)
                | DataValue::Float64(None)
                | DataValue::String(None)
        )
    }

    pub fn data_type(&self) -> Result<DataType> {
        match self {
            DataValue::Boolean(_) => Ok(DataType::Boolean),
            DataValue::Int64(_) => Ok(DataType::Int64),
            DataValue::UInt64(_) => Ok(DataType::UInt64),
            DataValue::Float64(_) => Ok(DataType::Float64),
            DataValue::String(_) => Ok(DataType::Utf8),
        }
    }

    pub fn to_array(&self) -> Result<DataArrayRef> {
        match self {
            DataValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v])) as DataArrayRef),
            DataValue::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v])) as DataArrayRef),
            DataValue::UInt64(v) => Ok(Arc::new(UInt64Array::from(vec![*v])) as DataArrayRef),
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
            DataType::Int64 => (DataValue::Int64(None)),
            DataType::UInt64 => (DataValue::UInt64(None)),
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
            DataValue::Int64(v) => format_data_value_with_option!(f, v),
            DataValue::UInt64(v) => format_data_value_with_option!(f, v),
            DataValue::Float64(v) => format_data_value_with_option!(f, v),
            DataValue::String(v) => format_data_value_with_option!(f, v),
        }
    }
}
