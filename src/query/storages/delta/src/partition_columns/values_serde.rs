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

// TODO: support other data types

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use deltalake::kernel::Add;
use ordered_float::OrderedFloat;

pub fn str_to_scalar(value: &str, data_type: &DataType) -> Result<Scalar> {
    if value.is_empty() {
        if let DataType::Nullable(_) = data_type {
            return Ok(Scalar::Null);
        }
    }
    match data_type {
        DataType::Nullable(t) => str_to_scalar(value, t),
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
            "can not use type {} as delta partition",
            data_type
        ))),
    }
}

pub fn get_partition_values(add: &Add, fields: &[&TableField]) -> Result<Vec<Scalar>> {
    let mut values = Vec::with_capacity(fields.len());
    for f in fields {
        match add.partition_values.get(&f.name) {
            Some(Some(v)) => values.push(str_to_scalar(v, &f.data_type().into())?),
            Some(None) => values.push(Scalar::Null),
            None => {
                return Err(ErrorCode::BadArguments(format!(
                    "partition value for column {} not found",
                    &f.name
                )));
            }
        }
    }
    Ok(values)
}
