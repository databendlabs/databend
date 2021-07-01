// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

impl DataValue {
    pub fn try_from_literal(literal: &str) -> Result<DataValue> {
        match literal.parse::<i64>() {
            Ok(n) => {
                if n >= 0 {
                    Ok(DataValue::UInt64(Some(n as u64)))
                } else {
                    Ok(DataValue::Int64(Some(n)))
                }
            }
            Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
        }
    }

    /// Convert data value vectors to data array.
    pub fn try_into_data_array(values: &[DataValue]) -> Result<Series> {
        match values[0].data_type() {
            DataType::Int8 => {
                try_build_array! {PrimitiveArrayBuilder, Int8Type, Int8, values}
            }
            DataType::Int16 => try_build_array! {PrimitiveArrayBuilder, Int16Type, Int16, values},
            DataType::Int32 => try_build_array! {PrimitiveArrayBuilder, Int32Type, Int32, values},
            DataType::Int64 => try_build_array! {PrimitiveArrayBuilder, Int64Type, Int64, values},
            DataType::UInt8 => try_build_array! {PrimitiveArrayBuilder, UInt8Type, UInt8, values},
            DataType::UInt16 => {
                try_build_array! {PrimitiveArrayBuilder, UInt16Type, UInt16, values}
            }
            DataType::UInt32 => {
                try_build_array! {PrimitiveArrayBuilder, UInt32Type, UInt32, values}
            }
            DataType::UInt64 => {
                try_build_array! {PrimitiveArrayBuilder, UInt64Type, UInt64, values}
            }
            DataType::Float32 => {
                try_build_array! {PrimitiveArrayBuilder, Float32Type, Float32, values}
            }
            DataType::Float64 => {
                try_build_array! {PrimitiveArrayBuilder, Float64Type, Float64, values}
            }
            DataType::Boolean => try_build_array! {values},
            DataType::Utf8 => try_build_array! {Utf8, values},
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{} for DataValue List",
                other
            ))),
        }
    }
}
