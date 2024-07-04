// Copyright 2021 Datafuse Labs
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

use std::fmt::Debug;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use ordered_float::OrderedFloat;
use volo_thrift::MaybeException;

use crate::hive_table::HIVE_DEFAULT_PARTITION;

pub(crate) fn str_field_to_scalar(value: &str, data_type: &DataType) -> Result<Scalar> {
    match data_type {
        DataType::Nullable(c) => {
            if value == HIVE_DEFAULT_PARTITION {
                Ok(Scalar::Null)
            } else {
                str_field_to_scalar(value, c.as_ref())
            }
        }
        DataType::String => Ok(Scalar::String(value.to_string())),
        DataType::Number(num_ty) => match num_ty {
            NumberDataType::UInt8 => {
                let num = value.parse::<u8>().unwrap();
                Ok(Scalar::Number(NumberScalar::UInt8(num)))
            }
            NumberDataType::UInt16 => {
                let num = value.parse::<u16>().unwrap();
                Ok(Scalar::Number(NumberScalar::UInt16(num)))
            }
            NumberDataType::UInt32 => {
                let num = value.parse::<u32>().unwrap();
                Ok(Scalar::Number(NumberScalar::UInt32(num)))
            }
            NumberDataType::UInt64 => {
                let num = value.parse::<u64>().unwrap();
                Ok(Scalar::Number(NumberScalar::UInt64(num)))
            }
            NumberDataType::Int8 => {
                let num = value.parse::<i8>().unwrap();
                Ok(Scalar::Number(NumberScalar::Int8(num)))
            }
            NumberDataType::Int16 => {
                let num = value.parse::<i16>().unwrap();
                Ok(Scalar::Number(NumberScalar::Int16(num)))
            }
            NumberDataType::Int32 => {
                let num = value.parse::<i32>().unwrap();
                Ok(Scalar::Number(NumberScalar::Int32(num)))
            }
            NumberDataType::Int64 => {
                let num = value.parse::<i64>().unwrap();
                Ok(Scalar::Number(NumberScalar::Int64(num)))
            }
            NumberDataType::Float32 => {
                let num = value.parse::<f32>().unwrap();
                Ok(Scalar::Number(NumberScalar::Float32(OrderedFloat(num))))
            }
            NumberDataType::Float64 => {
                let num = value.parse::<f64>().unwrap();
                Ok(Scalar::Number(NumberScalar::Float64(OrderedFloat(num))))
            }
        },
        _ => Err(ErrorCode::Unimplemented(format!(
            "generate scalar failed, {:?}",
            data_type
        ))),
    }
}

/// Format a thrift error into iceberg error.
///
/// Please only throw this error when you are sure that the error is caused by thrift.
pub fn from_thrift_error(error: impl std::error::Error) -> ErrorCode {
    ErrorCode::Internal(format!(
        "thrift error: {:?}, please check your thrift client config",
        error
    ))
}

/// Format a thrift exception into iceberg error.
pub fn from_thrift_exception<T, E: Debug>(value: MaybeException<T, E>) -> Result<T, ErrorCode> {
    match value {
        MaybeException::Ok(v) => Ok(v),
        MaybeException::Exception(err) => Err(ErrorCode::Internal(format!(
            "thrift error: {err:?}, please check your thrift client config"
        ))),
    }
}
