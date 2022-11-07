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
use common_jsonb::parse_json_path;
use common_jsonb::JsonPath;
use serde_json::Value as JsonValue;

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
        &self.display_name
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
        let json_paths = parse_json_paths(columns[1].column())?;

        let variant_column: &VariantColumn = if columns[0].column().is_const() {
            let const_column: &ConstColumn = Series::check_get(columns[0].column())?;
            Series::check_get(const_column.inner())?
        } else {
            Series::check_get(columns[0].column())?
        };
        let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(input_rows);
        if columns[0].column().is_const() {
            let value = variant_column.get_data(0);
            for json_path in json_paths.iter() {
                match extract_value_by_path(value.as_ref(), json_path) {
                    Some(child_value) => {
                        builder.append(&VariantValue::from(child_value), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else if columns[1].column().is_const() {
            let json_path = json_paths.get(0).unwrap();
            for value in variant_column.scalar_iter() {
                match extract_value_by_path(value.as_ref(), json_path) {
                    Some(child_value) => {
                        builder.append(&VariantValue::from(child_value), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else {
            for (i, value) in variant_column.scalar_iter().enumerate() {
                let json_path = json_paths.get(i).unwrap();
                match extract_value_by_path(value.as_ref(), json_path) {
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

pub fn parse_json_paths(column: &ColumnRef) -> Result<Vec<Vec<JsonPath>>> {
    let column: &StringColumn = if column.is_const() {
        let const_column: &ConstColumn = Series::check_get(column)?;
        Series::check_get(const_column.inner())?
    } else {
        Series::check_get(column)?
    };

    let mut json_paths: Vec<Vec<JsonPath>> = vec![];
    for v in column.iter() {
        if v.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Bad compound object's field path name: '' in GET_PATH",
            ));
        }
        let json_path = parse_json_path(v)
            .map_err(|_| ErrorCode::StrParseError("Invalid json_path".to_string()))?;
        json_paths.push(json_path);
    }
    Ok(json_paths)
}

pub fn extract_value_by_path<'a>(
    value: &'a JsonValue,
    json_path: &'a Vec<JsonPath>,
) -> Option<&'a JsonValue> {
    if json_path.is_empty() {
        return None;
    }

    let mut found_value = true;
    let mut value = value;
    for key in json_path.iter() {
        match key {
            JsonPath::UInt64(k) => match value.get(*k as usize) {
                Some(child_value) => value = child_value,
                None => {
                    found_value = false;
                    break;
                }
            },
            JsonPath::String(k) => match value.get(k.as_ref()) {
                Some(child_value) => value = child_value,
                None => {
                    found_value = false;
                    break;
                }
            },
        }
    }
    if !found_value {
        return None;
    }
    Some(value)
}
