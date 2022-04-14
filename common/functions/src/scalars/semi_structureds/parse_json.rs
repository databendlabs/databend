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

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

pub type TryParseJsonFunction = ParseJsonFunctionImpl<true>;

pub type ParseJsonFunction = ParseJsonFunctionImpl<false>;

#[derive(Clone)]
pub struct ParseJsonFunctionImpl<const SUPPRESS_PARSE_ERROR: bool> {
    display_name: String,
    result_type: DataTypePtr,
}

impl<const SUPPRESS_PARSE_ERROR: bool> ParseJsonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        let data_type = remove_nullable(args[0]);
        if data_type.data_type_id() == TypeID::VariantArray
            || data_type.data_type_id() == TypeID::VariantObject
        {
            return Err(ErrorCode::BadDataValueType(format!(
                "Invalid argument types for function '{}': ({})",
                display_name,
                data_type.name()
            )));
        }

        let result_type = if args[0].data_type_id() == TypeID::Null {
            NullType::arc()
        } else if args[0].is_nullable() || SUPPRESS_PARSE_ERROR {
            NullableType::arc(VariantType::arc())
        } else {
            VariantType::arc()
        };

        Ok(Box::new(ParseJsonFunctionImpl::<SUPPRESS_PARSE_ERROR> {
            display_name: display_name.to_string(),
            result_type,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default().deterministic().num_arguments(1);
        // Null will cause parse error when SUPPRESS_PARSE_ERROR is false.
        // In this case we need to check null and skip the parsing, so passthrough_null should be false.
        if !SUPPRESS_PARSE_ERROR {
            features = features.disable_passthrough_null()
        }
        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

impl<const SUPPRESS_PARSE_ERROR: bool> Function for ParseJsonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        self.result_type.clone()
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let data_type = columns[0].field().data_type();
        if data_type.data_type_id() == TypeID::Null {
            return NullType::arc().create_constant_column(&DataValue::Null, input_rows);
        }

        let column = columns[0].column();
        if SUPPRESS_PARSE_ERROR {
            let mut builder = NullableColumnBuilder::<JsonValue>::with_capacity(input_rows);
            if data_type.data_type_id().is_numeric()
                || data_type.data_type_id().is_string()
                || data_type.data_type_id() == TypeID::Boolean
                || data_type.data_type_id() == TypeID::Variant
            {
                let serializer = data_type.create_serializer();
                match serializer.serialize_json_object_suppress_error(column) {
                    Ok(values) => {
                        for v in values {
                            match v {
                                Some(v) => builder.append(&v, true),
                                None => builder.append_null(),
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
            } else {
                for _ in 0..input_rows {
                    builder.append_null();
                }
            };
            return Ok(builder.build(input_rows));
        }

        if column.is_nullable() {
            let (_, valids) = column.validity();
            let nullable_column: &NullableColumn = Series::check_get(column)?;
            let column = nullable_column.inner();
            let data_type = remove_nullable(data_type);

            let mut builder = NullableColumnBuilder::<JsonValue>::with_capacity(input_rows);
            if data_type.data_type_id().is_numeric()
                || data_type.data_type_id().is_string()
                || data_type.data_type_id() == TypeID::Boolean
                || data_type.data_type_id() == TypeID::Variant
            {
                let serializer = data_type.create_serializer();
                match serializer.serialize_json_object(column, valids) {
                    Ok(values) => {
                        for (i, v) in values.iter().enumerate() {
                            if let Some(valids) = valids {
                                if !valids.get_bit(i) {
                                    builder.append_null();
                                    continue;
                                }
                            }
                            builder.append(v, true);
                        }
                    }
                    Err(e) => return Err(e),
                }
            } else {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Error parsing JSON: type does not match {:?}",
                    data_type.data_type_id()
                )));
            }
            return Ok(builder.build(input_rows));
        }

        let mut builder = ColumnBuilder::<JsonValue>::with_capacity(input_rows);
        if data_type.data_type_id().is_numeric()
            || data_type.data_type_id().is_string()
            || data_type.data_type_id() == TypeID::Boolean
            || data_type.data_type_id() == TypeID::Variant
        {
            let serializer = data_type.create_serializer();
            match serializer.serialize_json_object(column, None) {
                Ok(values) => {
                    for v in values {
                        builder.append(&v);
                    }
                }
                Err(e) => return Err(e),
            }
        } else {
            return Err(ErrorCode::BadDataValueType(format!(
                "Error parsing JSON: type does not match {:?}",
                data_type.data_type_id()
            )));
        }

        Ok(builder.build(input_rows))
    }
}

impl<const SUPPRESS_PARSE_ERROR: bool> fmt::Display
    for ParseJsonFunctionImpl<SUPPRESS_PARSE_ERROR>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
