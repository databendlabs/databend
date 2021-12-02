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

    pub fn try_from_literal(literal: &str) -> Result<DataValue> {
        let mut s = literal;
        let negative_flag = s.starts_with(char::from_u32(45).unwrap());
        if negative_flag {
            s = &s[1..];
        }

        match s.parse::<u64>() {
            Ok(n) => {
                if negative_flag {
                    // when the real number is i8::MIN or i16::MIN or i32::MIN or i64::MIN
                    // execute `n as i8`、`n as i16`、`n as i32`、`n as i64`,
                    // rust will automatically convert it to a negative number
                    if n == i8::MAX as u64 + 1 {
                        return Ok(DataValue::Int8(Some(n as i8)));
                    } else if n == i16::MAX as u64 + 1 {
                        return Ok(DataValue::Int16(Some(n as i16)));
                    } else if n == i32::MAX as u64 + 1 {
                        return Ok(DataValue::Int32(Some(n as i32)));
                    } else if n == i64::MAX as u64 + 1 {
                        return Ok(DataValue::Int64(Some(n as i64)));
                    }

                    let num = -(n as i64);
                    if num >= i8::MIN as i64 {
                        return Ok(DataValue::Int8(Some(num as i8)));
                    } else if num >= u16::MIN as i64 {
                        return Ok(DataValue::Int16(Some(num as i16)));
                    } else if num >= u32::MIN as i64 {
                        return Ok(DataValue::Int32(Some(num as i32)));
                    } else {
                        return Ok(DataValue::Int64(Some(num as i64)));
                    }
                }

                if n <= u8::MAX as u64 {
                    Ok(DataValue::UInt8(Some(n as u8)))
                } else if n <= u16::MAX as u64 {
                    Ok(DataValue::UInt16(Some(n as u16)))
                } else if n <= u32::MAX as u64 {
                    Ok(DataValue::UInt32(Some(n as u32)))
                } else {
                    Ok(DataValue::UInt64(Some(n as u64)))
                }
            }
            Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
        }
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
