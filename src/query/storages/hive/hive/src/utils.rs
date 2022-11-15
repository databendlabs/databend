// Copyright 2022 Datafuse Labs.
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
use common_expression::types::number::NumberScalar;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Column;
use common_expression::Scalar;
use ordered_float::OrderedFloat;

pub(crate) fn str_field_to_column(
    num_rows: usize,
    value: String,
    data_type: &DataType,
) -> Result<Column> {
    match data_type {
        DataType::String => {
            let data = (0..num_rows)
                .map(|_| value.as_bytes().to_vec())
                .collect::<Vec<Vec<u8>>>();
            Ok(Column::from_data(data))
        }
        DataType::Number(num_ty) => match num_ty {
            NumberDataType::UInt8 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<u8>().unwrap())
                    .collect::<Vec<u8>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::UInt16 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<u16>().unwrap())
                    .collect::<Vec<u16>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::UInt32 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<u32>().unwrap())
                    .collect::<Vec<u32>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::UInt64 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<u64>().unwrap())
                    .collect::<Vec<u64>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Int8 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<i8>().unwrap())
                    .collect::<Vec<i8>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Int16 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<i16>().unwrap())
                    .collect::<Vec<i16>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Int32 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<i32>().unwrap())
                    .collect::<Vec<i32>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Int64 => {
                let data = (0..num_rows)
                    .map(|_| value.parse::<i64>().unwrap())
                    .collect::<Vec<i64>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Float32 => {
                let data = (0..num_rows)
                    .map(|_| OrderedFloat(value.parse::<f32>().unwrap()))
                    .collect::<Vec<OrderedFloat<f32>>>();
                Ok(Column::from_data(data))
            }
            NumberDataType::Float64 => {
                let data = (0..num_rows)
                    .map(|_| OrderedFloat(value.parse::<f64>().unwrap()))
                    .collect::<Vec<OrderedFloat<f64>>>();
                Ok(Column::from_data(data))
            }
        },
        _ => Err(ErrorCode::Unimplemented(format!(
            "generate column failed, {:?}",
            data_type
        ))),
    }
}

pub(crate) fn str_field_to_scalar(value: &str, data_type: &DataType) -> Result<Scalar> {
    match data_type {
        DataType::String => Ok(Scalar::String(value.as_bytes().to_vec())),
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
