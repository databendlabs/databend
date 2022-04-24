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
use sqlparser::ast::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

pub type GetFunction = GetFunctionImpl<false, false>;

pub type GetIgnoreCaseFunction = GetFunctionImpl<false, true>;

pub type GetPathFunction = GetFunctionImpl<true, false>;

#[derive(Clone)]
pub struct GetFunctionImpl<const BY_PATH: bool, const IGNORE_CASE: bool> {
    display_name: String,
}

impl<const BY_PATH: bool, const IGNORE_CASE: bool> GetFunctionImpl<BY_PATH, IGNORE_CASE> {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        let data_type = args[0];
        let path_type = args[1];

        if (IGNORE_CASE
            && (!data_type.data_type_id().is_variant_or_object()
                || !path_type.data_type_id().is_string()))
            || (BY_PATH
                && (!data_type.data_type_id().is_variant()
                    || !path_type.data_type_id().is_string()))
            || (!data_type.data_type_id().is_variant()
                || (!path_type.data_type_id().is_string()
                    && !path_type.data_type_id().is_unsigned_integer()))
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?}, {:?})",
                display_name.to_uppercase(),
                data_type,
                path_type
            )));
        }

        Ok(Box::new(GetFunctionImpl::<BY_PATH, IGNORE_CASE> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl<const BY_PATH: bool, const IGNORE_CASE: bool> Function
    for GetFunctionImpl<BY_PATH, IGNORE_CASE>
{
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        NullableType::arc(VariantType::arc())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let path_keys = if BY_PATH {
            parse_path_keys(columns[1].column())?
        } else {
            build_path_keys(columns[1].column())?
        };

        extract_value_by_path(columns[0].column(), path_keys, input_rows, IGNORE_CASE)
    }
}

impl<const BY_PATH: bool, const IGNORE_CASE: bool> fmt::Display
    for GetFunctionImpl<BY_PATH, IGNORE_CASE>
{
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

pub fn build_path_keys(column: &ColumnRef) -> Result<Vec<Vec<DataValue>>> {
    if column.is_const() {
        let const_column: &ConstColumn = Series::check_get(column)?;
        return build_path_keys(const_column.inner());
    }

    let mut path_keys: Vec<Vec<DataValue>> = vec![];
    for i in 0..column.len() {
        path_keys.push(vec![column.get(i)]);
    }
    Ok(path_keys)
}

pub fn extract_value_by_path(
    column: &ColumnRef,
    path_keys: Vec<Vec<DataValue>>,
    input_rows: usize,
    ignore_case: bool,
) -> Result<ColumnRef> {
    let column: &VariantColumn = if column.is_const() {
        let const_column: &ConstColumn = Series::check_get(column)?;
        Series::check_get(const_column.inner())?
    } else {
        Series::check_get(column)?
    };

    let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(input_rows);
    for path_key in path_keys.iter() {
        if path_key.is_empty() {
            for _ in 0..column.len() {
                builder.append_null();
            }
            continue;
        }
        for v in column.iter() {
            let mut found_value = true;
            let mut value = v.as_ref();
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
                                // if no exact match value found, return one of the ambiguous matches
                                if ignore_case && value.is_object() {
                                    let mut ignore_case_found_value = false;
                                    let obj = value.as_object().unwrap();
                                    for (_, (child_key, child_value)) in obj.iter().enumerate() {
                                        if k.to_lowercase() == child_key.to_lowercase() {
                                            ignore_case_found_value = true;
                                            value = child_value;
                                            break;
                                        }
                                    }
                                    if ignore_case_found_value {
                                        continue;
                                    }
                                }
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
            if found_value {
                builder.append(&VariantValue::from(value), true);
            } else {
                builder.append_null();
            }
        }
    }
    Ok(builder.build(input_rows))
}
