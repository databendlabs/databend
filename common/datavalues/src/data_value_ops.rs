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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

impl DataValue {
    pub fn custom_display(&self, single_quote: bool) -> String {
        let s = self.to_string();
        if single_quote {
            if let DataValue::String(Some(_)) = self {
                return format!("'{}'", s);
            }
        }
        s
    }

    #[allow(clippy::needless_late_init)]
    pub fn try_from_literal(literal: &str, radix: Option<u32>) -> Result<DataValue> {
        let radix = radix.unwrap_or(10);

        let result;
        if literal.starts_with(char::from_u32(45).unwrap()) {
            result = match i64::from_str_radix(literal, radix) {
                Ok(n) => {
                    if n >= i8::MIN as i64 {
                        return Ok(DataValue::Int8(Some(n as i8)));
                    }
                    if n >= i16::MIN as i64 {
                        return Ok(DataValue::Int16(Some(n as i16)));
                    }
                    if n >= i32::MIN as i64 {
                        return Ok(DataValue::Int32(Some(n as i32)));
                    }
                    return Ok(DataValue::Int64(Some(n as i64)));
                }
                Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
            };
        } else {
            result = match u64::from_str_radix(literal, radix) {
                Ok(n) => {
                    if n <= u8::MAX as u64 {
                        return Ok(DataValue::UInt8(Some(n as u8)));
                    }
                    if n <= u16::MAX as u64 {
                        return Ok(DataValue::UInt16(Some(n as u16)));
                    }
                    if n <= u32::MAX as u64 {
                        return Ok(DataValue::UInt32(Some(n as u32)));
                    }
                    return Ok(DataValue::UInt64(Some(n as u64)));
                }
                Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
            };
        }
        result
    }

    /// Convert data value vectors to data array.
    pub fn try_into_data_array(values: &[DataValue], data_type: &DataType) -> Result<Series> {
        match data_type {
            DataType::Int8 => {
                try_build_array! {PrimitiveArrayBuilder, i8, Int8, values}
            }
            DataType::Int16 => try_build_array! {PrimitiveArrayBuilder, i16, Int16, values},
            DataType::Int32 => try_build_array! {PrimitiveArrayBuilder, i32, Int32, values},
            DataType::Int64 => try_build_array! {PrimitiveArrayBuilder, i64, Int64, values},
            DataType::UInt8 => try_build_array! {PrimitiveArrayBuilder, u8, UInt8, values},
            DataType::UInt16 => {
                try_build_array! {PrimitiveArrayBuilder, u16, UInt16, values}
            }
            DataType::UInt32 => {
                try_build_array! {PrimitiveArrayBuilder, u32, UInt32, values}
            }
            DataType::UInt64 => {
                try_build_array! {PrimitiveArrayBuilder, u64, UInt64, values}
            }
            DataType::Float32 => {
                try_build_array! {PrimitiveArrayBuilder, f32, Float32, values}
            }
            DataType::Float64 => {
                try_build_array! {PrimitiveArrayBuilder, f64, Float64, values}
            }
            DataType::Boolean => try_build_array! {values},
            DataType::String => try_build_array! {String, values},
            DataType::Date16 => {
                try_build_array! {PrimitiveArrayBuilder, u16, UInt16, values}
            }
            DataType::Date32 => {
                try_build_array! {PrimitiveArrayBuilder, i32, Int32, values}
            }
            DataType::DateTime32(_) => {
                try_build_array! {PrimitiveArrayBuilder, u32, UInt32, values}
            }
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{} for DataValue List",
                other
            ))),
        }
    }
}
