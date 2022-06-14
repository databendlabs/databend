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
use common_io::prelude::FormatSettings;

use crate::scalars::semi_structureds::get_path::extract_value_by_path;
use crate::scalars::semi_structureds::get_path::parse_path_keys;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct JsonExtractPathTextFunction {
    display_name: String,
}

impl JsonExtractPathTextFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];
        let path_type = args[1];

        if !path_type.data_type_id().is_string() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?}, {:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
                path_type.data_type_id()
            )));
        }

        Ok(Box::new(JsonExtractPathTextFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for JsonExtractPathTextFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        NullableType::new_impl(StringType::new_impl())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let path_keys = parse_path_keys(columns[1].column())?;

        let data_type = columns[0].field().data_type();
        let serializer = data_type.create_serializer(columns[0].column())?;
        // TODO(veeupup): check if we can use default format_settings
        let format = FormatSettings::default();
        let values = serializer.serialize_json_object(None, &format)?;

        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);
        if columns[0].column().is_const() {
            let value = values.get(0).unwrap();
            for i in 0..input_rows {
                let path_key = path_keys.get(i).unwrap();
                match extract_value_by_path(value, path_key) {
                    Some(child_value) => {
                        builder.append(child_value.to_string().as_bytes(), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else if columns[1].column().is_const() {
            let path_key = path_keys.get(0).unwrap();
            for i in 0..input_rows {
                let value = values.get(i).unwrap();
                match extract_value_by_path(value, path_key) {
                    Some(child_value) => {
                        builder.append(child_value.to_string().as_bytes(), true);
                    }
                    None => builder.append_null(),
                }
            }
        } else {
            for i in 0..input_rows {
                let path_key = path_keys.get(i).unwrap();
                let value = values.get(i).unwrap();
                match extract_value_by_path(value, path_key) {
                    Some(child_value) => {
                        builder.append(child_value.to_string().as_bytes(), true);
                    }
                    None => builder.append_null(),
                }
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for JsonExtractPathTextFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
