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

use std::collections::HashSet;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::DataType as SQLDataType;
use unicode_segmentation::UnicodeSegmentation;

pub struct SQLCommon;

impl SQLCommon {
    /// Maps the SQL type to the corresponding Arrow `DataType`
    pub fn make_data_type(sql_type: &SQLDataType) -> Result<DataTypeImpl> {
        match sql_type {
            SQLDataType::TinyInt(_) => Ok(i8::to_data_type()),
            SQLDataType::UnsignedTinyInt(_) => Ok(u8::to_data_type()),
            SQLDataType::SmallInt(_) => Ok(i16::to_data_type()),
            SQLDataType::UnsignedSmallInt(_) => Ok(u16::to_data_type()),
            SQLDataType::Int(_) => Ok(i32::to_data_type()),
            SQLDataType::UnsignedInt(_) => Ok(u32::to_data_type()),
            SQLDataType::BigInt(_) => Ok(i64::to_data_type()),
            SQLDataType::UnsignedBigInt(_) => Ok(u64::to_data_type()),
            SQLDataType::Char(_)
            | SQLDataType::Varchar(_)
            | SQLDataType::String
            | SQLDataType::Text => Ok(Vu8::to_data_type()),

            SQLDataType::Float(_) => Ok(f32::to_data_type()),
            SQLDataType::Decimal(_, _) => Ok(f64::to_data_type()),
            SQLDataType::Real | SQLDataType::Double => Ok(f64::to_data_type()),
            SQLDataType::Boolean => Ok(bool::to_data_type()),
            SQLDataType::Date => Ok(DateType::new_impl()),
            // default precision is 6, microseconds
            SQLDataType::Timestamp(_) | SQLDataType::DateTime(_) => Ok(TimestampType::new_impl()),

            SQLDataType::Array(sql_type, nullable) => {
                let inner_data_type = Self::make_data_type(sql_type)?;
                if *nullable {
                    if inner_data_type.is_null() {
                        return Result::Err(ErrorCode::IllegalDataType(
                            "The SQL data type ARRAY(NULL, NULL) is invalid",
                        ));
                    }
                    Ok(ArrayType::new_impl(NullableType::new_impl(inner_data_type)))
                } else {
                    Ok(ArrayType::new_impl(inner_data_type))
                }
            }
            SQLDataType::Tuple(names, sql_types) => {
                let mut inner_data_types = Vec::with_capacity(sql_types.len());
                for sql_type in sql_types {
                    let inner_data_type = Self::make_data_type(sql_type)?;
                    inner_data_types.push(inner_data_type);
                }
                match names {
                    Some(names) => {
                        let mut names_set = HashSet::with_capacity(names.len());
                        for name in names.iter() {
                            if !names_set.insert(name.value.clone()) {
                                return Result::Err(ErrorCode::IllegalDataType(
                                    "The names of tuple elements must be unique",
                                ));
                            }
                        }
                        let inner_names = names.iter().map(|v| v.value.clone()).collect::<Vec<_>>();
                        Ok(StructType::new_impl(Some(inner_names), inner_data_types))
                    }
                    None => Ok(StructType::new_impl(None, inner_data_types)),
                }
            }

            // Custom types for databend:
            // Custom(ObjectName([Ident { value: "uint8", quote_style: None }])
            SQLDataType::Custom(obj) if !obj.0.is_empty() => {
                match obj.0[0].value.to_uppercase().as_str() {
                    "SIGNED" => Ok(i64::to_data_type()),
                    "UNSIGNED" => Ok(u64::to_data_type()),

                    name => {
                        let factory = TypeFactory::instance();
                        let data_type = factory.get(name)?;
                        Ok(data_type)
                    }
                }
            }
            _ => Result::Err(ErrorCode::IllegalDataType(format!(
                "The SQL data type {sql_type:?} is not implemented",
            ))),
        }
    }

    pub fn short_sql(query: &str) -> String {
        let query = query.trim_start();
        if query.len() >= 64 && query[..6].eq_ignore_ascii_case("INSERT") {
            // keep first 64 graphemes
            String::from_utf8(
                query
                    .graphemes(true)
                    .take(64)
                    .flat_map(|g| g.as_bytes().iter())
                    .copied() // copied converts &u8 into u8
                    .chain(b"...".iter().copied())
                    .collect::<Vec<u8>>(),
            )
            .unwrap() // by construction, this cannot panic as we extracted unicode grapheme
        } else {
            query.to_string()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sql_common::SQLCommon;

    const LONG_INSERT_WITH_UNICODE_AT_TRUNCATION_POINT: &str =
        "INSERT INTO `test` VALUES ('abcd', 'def'),('abcd', 'def'),('abcé', 'def');";

    #[test]
    #[should_panic]
    fn test_invalid_string_truncation() {
        // This test checks the INSERT statement did panic with byte truncated string.
        // We need to do this to validate that the code of short_sql has fixed this panic!
        format!("{}...", &LONG_INSERT_WITH_UNICODE_AT_TRUNCATION_POINT[..64]);
    }

    #[test]
    fn test_short_sql_truncation_on_unicode() {
        // short insert into statements are not truncated
        assert_eq!(
            SQLCommon::short_sql("INSERT INTO `test` VALUES('abcd', 'def');"),
            "INSERT INTO `test` VALUES('abcd', 'def');"
        );
        // long one are at 64th char...
        let shortned = SQLCommon::short_sql(LONG_INSERT_WITH_UNICODE_AT_TRUNCATION_POINT);
        assert_eq!(shortned.len(), 68); // 64 chars with a multibyte one (é) + ...
        assert_eq!(
            shortned,
            "INSERT INTO `test` VALUES ('abcd', 'def'),('abcd', 'def'),('abcé..."
        );
    }
}
