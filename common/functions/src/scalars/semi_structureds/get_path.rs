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

use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value as JsonValue;
use sqlparser::ast::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct GetPathFunction {
    display_name: String,
}

impl GetPathFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];
        let path_type = args[1];

        if !data_type.data_type_id().is_variant() || !path_type.data_type_id().is_string() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?}, {:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
                path_type.data_type_id()
            )));
        }

        Ok(Box::new(GetPathFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for GetPathFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        NullableType::new_impl(VariantType::new_impl())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let path_keys = parse_path_keys(columns[1].column())?;

        let variant_column: &VariantColumn = if columns[0].column().is_const() {
            let const_column: &ConstColumn = Series::check_get(columns[0].column())?;
            Series::check_get(const_column.inner())?
        } else {
            Series::check_get(columns[0].column())?
        };
        let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(input_rows);
        if columns[0].column().is_const() {
            let value = variant_column.get_data(0);
            for path_key in path_keys.iter() {
                match extract_value_by_path(value.as_ref(), path_key) {
                    Some(child_value) => {
                        builder.append(&VariantValue::from(child_value), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else if columns[1].column().is_const() {
            let path_key = path_keys.get(0).unwrap();
            for value in variant_column.scalar_iter() {
                match extract_value_by_path(value.as_ref(), path_key) {
                    Some(child_value) => {
                        builder.append(&VariantValue::from(child_value), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else {
            for (i, value) in variant_column.scalar_iter().enumerate() {
                let path_key = path_keys.get(i).unwrap();
                match extract_value_by_path(value.as_ref(), path_key) {
                    Some(child_value) => {
                        builder.append(&VariantValue::from(child_value), true);
                    }
                    None => builder.append_null(),
                }
            }
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for GetPathFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

pub fn parse_path_keys(column: &ColumnRef) -> Result<Vec<Vec<DataValue>>> {
    let column: &StringColumn = if column.is_const() {
        let const_column: &ConstColumn = Series::check_get(column)?;
        Series::check_get(const_column.inner())?
    } else {
        Series::check_get(column)?
    };

    let dialect = &GenericDialect {};
    let mut path_keys: Vec<Vec<DataValue>> = vec![];
    for v in column.iter() {
        if v.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Bad compound object's field path name: '' in GET_PATH",
            ));
        }
        let definition = std::str::from_utf8(v).unwrap();
        let mut tokenizer = Tokenizer::new(dialect, definition);
        match tokenizer.tokenize() {
            Ok((tokens, position_map)) => {
                match Parser::new(tokens, position_map, dialect).parse_map_keys() {
                    Ok(values) => {
                        let path_key: Vec<DataValue> = values
                            .iter()
                            .map(|v| match v {
                                Value::Number(value, _) => {
                                    DataValue::try_from_literal(value, None).unwrap()
                                }
                                Value::SingleQuotedString(value) => {
                                    DataValue::String(value.clone().into_bytes())
                                }
                                Value::ColonString(value) => {
                                    DataValue::String(value.clone().into_bytes())
                                }
                                Value::PeriodString(value) => {
                                    DataValue::String(value.clone().into_bytes())
                                }
                                _ => DataValue::Null,
                            })
                            .collect();

                        path_keys.push(path_key);
                    }
                    Err(parse_error) => return Err(ErrorCode::from(parse_error)),
                }
            }
            Err(tokenize_error) => {
                return Err(ErrorCode::SyntaxException(format!(
                    "Can not tokenize definition: {}, Error: {:?}",
                    definition, tokenize_error
                )))
            }
        }
    }
    Ok(path_keys)
}

pub fn extract_value_by_path<'a>(
    value: &'a JsonValue,
    path_key: &'a Vec<DataValue>,
) -> Option<&'a JsonValue> {
    if path_key.is_empty() {
        return None;
    }

    let mut found_value = true;
    let mut value = value;
    for key in path_key.iter() {
        match key {
            DataValue::UInt64(k) => match value.get(*k as usize) {
                Some(child_value) => value = child_value,
                None => {
                    found_value = false;
                    break;
                }
            },
            DataValue::String(k) => match String::from_utf8(k.to_vec()) {
                Ok(k) => match value.get(&k) {
                    Some(child_value) => value = child_value,
                    None => {
                        found_value = false;
                        break;
                    }
                },
                Err(_) => {
                    found_value = false;
                    break;
                }
            },
            _ => {
                found_value = false;
                break;
            }
        }
    }
    if !found_value {
        return None;
    }
    Some(value)
}
