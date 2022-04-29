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

use crate::scalars::semi_structureds::get::extract_value_by_path;
use crate::scalars::semi_structureds::get::parse_path_keys;
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
        NullableType::arc(StringType::arc())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let path_keys = parse_path_keys(columns[1].column())?;

        let data_type = columns[0].field().data_type();
        if !data_type.data_type_id().is_string() && !data_type.data_type_id().is_variant() {
            let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);
            for _ in 0..input_rows {
                builder.append_null();
            }
            return Ok(builder.build(input_rows));
        }

        let mut builder = ColumnBuilder::<VariantValue>::with_capacity(input_rows);
        let serializer = data_type.create_serializer();
        match serializer.serialize_json_object(columns[0].column(), None) {
            Ok(values) => {
                for v in values {
                    builder.append(&VariantValue::from(v));
                }
            }
            Err(e) => return Err(e),
        }
        let variant_column = builder.build(input_rows);
        let column = extract_value_by_path(&variant_column, path_keys, input_rows, false)?;

        // convert VariantColumn to StringColumn
        let (_, valids) = column.validity();
        let nullable_column: &NullableColumn = Series::check_get(&column)?;
        let column = nullable_column.inner();
        let c: &VariantColumn = Series::check_get(column)?;

        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);
        for (i, v) in c.iter().enumerate() {
            if let Some(valids) = valids {
                if !valids.get_bit(i) {
                    builder.append_null();
                    continue;
                }
            }
            builder.append(v.as_ref().to_string().as_bytes(), true);
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for JsonExtractPathTextFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
