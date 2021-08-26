// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

impl DataValue {
    pub fn custom_display(&self, single_quote: bool) -> String {
        let s = self.to_string();
        if single_quote {
            if let DataValue::Utf8(Some(_)) = self {
                return format!("'{}'", s);
            }
        }
        s
    }

    pub fn try_from_literal(literal: &str) -> Result<DataValue> {
        match literal.parse::<i64>() {
            Ok(n) => {
                if n >= 0 {
                    let n = literal.parse::<u64>()?;
                    if n <= u8::MAX as u64 {
                        return Ok(DataValue::UInt8(Some(n as u8)));
                    } else if n <= u16::MAX as u64 {
                        return Ok(DataValue::UInt16(Some(n as u16)));
                    } else if n <= u32::MAX as u64 {
                        return Ok(DataValue::UInt32(Some(n as u32)));
                    } else {
                        return Ok(DataValue::UInt64(Some(n as u64)));
                    }
                }

                if n >= i8::MIN as i64 {
                    Ok(DataValue::Int8(Some(n as i8)))
                } else if n >= u16::MIN as i64 {
                    Ok(DataValue::Int16(Some(n as i16)))
                } else if n >= u32::MIN as i64 {
                    Ok(DataValue::Int32(Some(n as i32)))
                } else {
                    Ok(DataValue::Int64(Some(n as i64)))
                }
            }
            Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
        }
    }

    /// Convert data value vectors to data array.
    pub fn try_into_data_array(values: &[DataValue], data_type: &DataType) -> Result<Series> {
        match data_type {
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
