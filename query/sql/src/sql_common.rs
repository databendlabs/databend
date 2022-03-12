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
    pub fn make_data_type(sql_type: &SQLDataType) -> Result<DataTypePtr> {
        match sql_type {
            SQLDataType::BigInt(_) => Ok(i64::to_data_type()),
            SQLDataType::Int(_) => Ok(i32::to_data_type()),
            SQLDataType::SmallInt(_) => Ok(i16::to_data_type()),
            SQLDataType::TinyInt(_) => Ok(i8::to_data_type()),
            SQLDataType::Char(_)
            | SQLDataType::Varchar(_)
            | SQLDataType::String
            | SQLDataType::Text => Ok(Vu8::to_data_type()),

            SQLDataType::Decimal(_, _) => Ok(f64::to_data_type()),
            SQLDataType::Float(_) => Ok(f32::to_data_type()),
            SQLDataType::Real | SQLDataType::Double => Ok(f64::to_data_type()),
            SQLDataType::Boolean => Ok(bool::to_data_type()),
            SQLDataType::Date => Ok(Date16Type::arc()),
            SQLDataType::Timestamp => Ok(DateTime32Type::arc(None)),

            //custom types for databend
            // Custom(ObjectName([Ident { value: "uint8", quote_style: None }])
            SQLDataType::Custom(obj) if !obj.0.is_empty() => {
                match obj.0[0].value.to_uppercase().as_str() {
                    "SIGNED" => Ok(i64::to_data_type()),
                    "UNSIGNED" => Ok(u64::to_data_type()),

                    name => {
                        let factory = TypeFactory::instance();
                        let data_type = factory.get(name)?;
                        Ok(data_type.clone())
                    }
                }
            }
            _ => Result::Err(ErrorCode::IllegalDataType(format!(
                "The SQL data type {sql_type:?} is not implemented",
            ))),
        }
    }
}
