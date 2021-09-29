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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::DataType as SQLDataType;

pub struct SQLCommon;

impl SQLCommon {
    /// Maps the SQL type to the corresponding Arrow `DataType`
    pub fn make_data_type(sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::Int(_) => Ok(DataType::Int32),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16),
            SQLDataType::Char(_) => Ok(DataType::String),
            SQLDataType::Varchar(_) => Ok(DataType::String),
            SQLDataType::String => Ok(DataType::String),
            SQLDataType::Text => Ok(DataType::String),
            SQLDataType::Decimal(_, _) => Ok(DataType::Float64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::Date => Ok(DataType::Date16),
            SQLDataType::Timestamp => Ok(DataType::DateTime32(None)),

            //custom types for databend
            // Custom(ObjectName([Ident { value: "uint8", quote_style: None }])
            SQLDataType::Custom(obj) if !obj.0.is_empty() => {
                match obj.0[0].value.to_uppercase().as_str() {
                    "UINT8" => Ok(DataType::UInt8),
                    "UINT16" => Ok(DataType::UInt16),
                    "UINT32" => Ok(DataType::UInt32),
                    "UINT64" => Ok(DataType::UInt64),

                    "INT8" => Ok(DataType::Int8),
                    "INT16" => Ok(DataType::Int16),
                    "INT32" => Ok(DataType::Int32),
                    "INT64" => Ok(DataType::Int64),
                    "FLOAT32" => Ok(DataType::Float32),
                    "FLOAT64" => Ok(DataType::Float64),
                    "STRING" => Ok(DataType::String),

                    _ => Result::Err(ErrorCode::IllegalDataType(format!(
                        "The SQL data type {:?} is not implemented",
                        sql_type
                    ))),
                }
            }
            _ => Result::Err(ErrorCode::IllegalDataType(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            ))),
        }
    }
}
