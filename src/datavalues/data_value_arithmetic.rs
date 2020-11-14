// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataValue;
use crate::error::{Error, Result};

pub fn datavalue_add(left: DataValue, right: DataValue) -> Result<DataValue> {
    Ok(match (&left, &right) {
        (DataValue::Int64(lhs), DataValue::Int64(rhs)) => {
            typed_data_value_add!(lhs, rhs, Int64, i64)
        }
        (DataValue::UInt64(lhs), DataValue::UInt64(rhs)) => {
            typed_data_value_add!(lhs, rhs, UInt64, u64)
        }
        (DataValue::Float64(lhs), DataValue::Float64(rhs)) => {
            typed_data_value_add!(lhs, rhs, Float64, f64)
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported scalar_add() for data type: left:{:?}, right:{:?}",
                left.data_type()?,
                right.data_type()?
            )))
        }
    })
}
