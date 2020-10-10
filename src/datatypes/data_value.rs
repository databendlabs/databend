// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use arrow::array::{BooleanArray, Int64Array};
use std::fmt;
use std::sync::Arc;

use super::*;

/// A specific value of a data type.
#[derive(Clone, PartialEq)]
pub enum DataValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
}

pub type DataValueRef = Box<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    pub fn data_type(&self) -> Result<DataType> {
        match self {
            DataValue::Null => Ok(DataType::Null),
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
            DataValue::String(v) => {
                Ok(Arc::new(StringArray::from(vec![v.as_str()])) as DataArrayRef)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported DataType {:?} in to_array",
                self
            ))),
        }
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "Null"),
            DataValue::Boolean(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::UInt64(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::String(v) => write!(f, "\"{}\"", v),
        }
    }
}
