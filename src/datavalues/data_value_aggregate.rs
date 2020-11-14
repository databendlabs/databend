// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataValue;
use crate::error::{Error, Result};

pub fn datavalue_max(left: DataValue, right: DataValue) -> Result<DataValue> {
    Ok(match (&left, &right) {
        (DataValue::Int64(lhs), DataValue::Int64(rhs)) => {
            typed_data_value_min_max!(lhs, rhs, Int64, max)
        }
        (DataValue::UInt64(lhs), DataValue::UInt64(rhs)) => {
            typed_data_value_min_max!(lhs, rhs, UInt64, max)
        }
        (DataValue::Float64(lhs), DataValue::Float64(rhs)) => {
            typed_data_value_min_max!(lhs, rhs, Float64, max)
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported datavalue_max() for data type: left:{:?}, right:{:?}",
                left.data_type()?,
                right.data_type()?
            )))
        }
    })
}
