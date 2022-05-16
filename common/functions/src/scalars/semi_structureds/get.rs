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
use common_datavalues::with_match_integer_type_id;
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value as JsonValue;

use crate::scalars::semi_structureds::array_get::ArrayGetFunction;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

pub type GetFunction = GetFunctionImpl<false>;

pub type GetIgnoreCaseFunction = GetFunctionImpl<true>;

#[derive(Clone)]
pub struct GetFunctionImpl<const IGNORE_CASE: bool> {
    display_name: String,
}

impl<const IGNORE_CASE: bool> GetFunctionImpl<IGNORE_CASE> {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];
        let path_type = args[1];

        if data_type.data_type_id().is_array() {
            return ArrayGetFunction::try_create(display_name, args);
        }

        if !data_type.data_type_id().is_variant()
            || (!path_type.data_type_id().is_string() && !path_type.data_type_id().is_integer())
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?}, {:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
                path_type.data_type_id()
            )));
        }

        Ok(Box::new(GetFunctionImpl::<IGNORE_CASE> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl<const IGNORE_CASE: bool> Function for GetFunctionImpl<IGNORE_CASE> {
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
        let variant_column: &VariantColumn = if columns[0].column().is_const() {
            let const_column: &ConstColumn = Series::check_get(columns[0].column())?;
            Series::check_get(const_column.inner())?
        } else {
            Series::check_get(columns[0].column())?
        };

        let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(input_rows);
        let index_type = columns[1].data_type().data_type_id();
        with_match_integer_type_id!(index_type, |$T| {
            let index_column: &PrimitiveColumn<$T> = if columns[1].column().is_const() {
                let const_column: &ConstColumn = Series::check_get(columns[1].column())?;
                Series::check_get(const_column.inner())?
            } else {
                Series::check_get(columns[1].column())?
            };
            if IGNORE_CASE {
                // GET_IGNORE_CASE only support extract value by field_name
                for _ in 0..input_rows {
                    builder.append_null();
                }
            } else {
                if columns[0].column().is_const() {
                    let value = variant_column.get_data(0);
                    for index in index_column.iter() {
                        let index = usize::try_from(*index)?;
                        match value.get(index) {
                            Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                            None => builder.append_null(),
                        }
                    }
                } else if columns[1].column().is_const() {
                    let index = index_column.get(0).as_u64()? as usize;
                    for value in variant_column.scalar_iter() {
                        match value.get(index) {
                            Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                            None => builder.append_null(),
                        }
                    }
                } else {
                    for (i, value) in variant_column.scalar_iter().enumerate() {
                        let index = index_column.get(i).as_u64()? as usize;
                        match value.get(index) {
                            Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                            None => builder.append_null(),
                        }
                    }
                }
            }
        }, {
            let field_name_column: &StringColumn = if columns[1].column().is_const() {
                let const_column: &ConstColumn = Series::check_get(columns[1].column())?;
                Series::check_get(const_column.inner())?
            } else {
                Series::check_get(columns[1].column())?
            };
            if columns[0].column().is_const() {
                let value = variant_column.get_data(0);
                for field_name in field_name_column.iter() {
                    match extract_value_by_field_name(value, field_name, &IGNORE_CASE) {
                        Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                        None => builder.append_null(),
                    }
                }
            } else if columns[1].column().is_const() {
                let field_name = field_name_column.get_data(0);
                for value in variant_column.scalar_iter() {
                    match extract_value_by_field_name(value, field_name, &IGNORE_CASE) {
                        Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                        None => builder.append_null(),
                    }
                }
            } else {
                for (i, value) in variant_column.scalar_iter().enumerate() {
                    let field_name = field_name_column.get_data(i);
                    match extract_value_by_field_name(value, field_name, &IGNORE_CASE) {
                        Some(child_value) => builder.append(&VariantValue::from(child_value), true),
                        None => builder.append_null(),
                    }
                }
            }
        });

        Ok(builder.build(input_rows))
    }
}

impl<const IGNORE_CASE: bool> fmt::Display for GetFunctionImpl<IGNORE_CASE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

fn extract_value_by_field_name<'a>(
    value: &'a JsonValue,
    field_name: &'a [u8],
    ignore_case: &'a bool,
) -> Option<&'a JsonValue> {
    match std::str::from_utf8(field_name) {
        Ok(field_name) => match value.get(field_name) {
            Some(child_value) => Some(child_value),
            None => {
                if *ignore_case && value.is_object() {
                    let obj = value.as_object().unwrap();
                    for (_, (child_key, child_value)) in obj.iter().enumerate() {
                        if field_name.to_lowercase() == child_key.to_lowercase() {
                            return Some(child_value);
                        }
                    }
                }
                None
            }
        },
        Err(_) => None,
    }
}
