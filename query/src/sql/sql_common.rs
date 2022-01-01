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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::DataType as SQLDataType;

pub struct SQLCommon;

impl SQLCommon {
    /// Maps the SQL type to the corresponding Arrow `DataType`
    pub fn make_data_type(sql_type: &SQLDataType, nullable: bool) -> Result<DataType> {
        match sql_type {
            SQLDataType::BigInt(_) => Ok(DataType::Int64(nullable)),
            SQLDataType::Int(_) => Ok(DataType::Int32(nullable)),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8(nullable)),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16(nullable)),
            SQLDataType::Char(_) => Ok(DataType::String(nullable)),
            SQLDataType::Varchar(_) => Ok(DataType::String(nullable)),
            SQLDataType::String => Ok(DataType::String(nullable)),
            SQLDataType::Text => Ok(DataType::String(nullable)),
            SQLDataType::Decimal(_, _) => Ok(DataType::Float64(nullable)),
            SQLDataType::Float(_) => Ok(DataType::Float32(nullable)),
            SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64(nullable)),
            SQLDataType::Boolean => Ok(DataType::Boolean(nullable)),
            SQLDataType::Date => Ok(DataType::Date16(nullable)),
            SQLDataType::Timestamp => Ok(DataType::DateTime32(nullable, None)),

            //custom types for databend
            // Custom(ObjectName([Ident { value: "uint8", quote_style: None }])
            SQLDataType::Custom(obj) if !obj.0.is_empty() => {
                match obj.0[0].value.to_uppercase().as_str() {
                    "UINT8" => Ok(DataType::UInt8(nullable)),
                    "UINT16" => Ok(DataType::UInt16(nullable)),
                    "UINT32" => Ok(DataType::UInt32(nullable)),
                    "UINT64" => Ok(DataType::UInt64(nullable)),

                    "INT8" => Ok(DataType::Int8(nullable)),
                    "INT16" => Ok(DataType::Int16(nullable)),
                    "INT32" => Ok(DataType::Int32(nullable)),
                    "INT64" => Ok(DataType::Int64(nullable)),
                    "FLOAT32" => Ok(DataType::Float32(nullable)),
                    "FLOAT64" => Ok(DataType::Float64(nullable)),
                    "STRING" => Ok(DataType::String(nullable)),
                    "DATE16" => Ok(DataType::Date16(nullable)),
                    "DATE32" => Ok(DataType::Date32(nullable)),
                    "DATETIME" => Ok(DataType::DateTime32(nullable, None)),
                    "DATETIME32" => Ok(DataType::DateTime32(nullable, None)),
                    // TODO parse precision
                    "DATETIME64" => Ok(DataType::DateTime64(nullable, 3, None)),
                    "SIGNED" => Ok(DataType::Int64(nullable)),
                    "UNSIGNED" => Ok(DataType::UInt64(nullable)),

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
