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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_physical_primitive_type;
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value as JsonValue;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

pub type TryParseJsonFunction = ParseJsonFunctionImpl<true>;

pub type ParseJsonFunction = ParseJsonFunctionImpl<false>;

#[derive(Clone)]
pub struct ParseJsonFunctionImpl<const SUPPRESS_PARSE_ERROR: bool> {
    display_name: String,
}

impl<const SUPPRESS_PARSE_ERROR: bool> ParseJsonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ParseJsonFunctionImpl::<SUPPRESS_PARSE_ERROR> {
            display_name: display_name.to_string(),
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

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if args[0].data_type_id() == TypeID::Null {
            return Ok(NullType::arc());
        }

        if SUPPRESS_PARSE_ERROR {
            // For invalid input, we suppress parse error and return null. So the return type must be nullable.
            return Ok(Arc::new(NullableType::create(VariantType::arc())));
        }

        if args[0].is_nullable() {
            return Ok(Arc::new(NullableType::create(VariantType::arc())));
        }
        Ok(VariantType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let column = columns[0].column();
        if column.data_type_id() == TypeID::Null {
            return NullType::arc().create_constant_column(&DataValue::Null, input_rows);
        }
        if column.data_type_id() == TypeID::Variant {
            return Ok(column.arc());
        }

        if SUPPRESS_PARSE_ERROR {
            let mut builder = NullableColumnBuilder::<JsonValue>::with_capacity(input_rows);
            with_match_physical_primitive_type!(column.data_type_id().to_physical_type(), |$T| {
                let c: &<$T as Scalar>::ColumnType = Series::check_get(&column)?;
                for v in c.iter() {
                    let v = *v as $T;
                    let x: JsonValue = v.into();
                    builder.append(&x, true);
                }
            }, {
                match column.data_type_id() {
                    TypeID::Boolean => {
                        let c: &BooleanColumn = Series::check_get(column)?;
                        for v in c.iter() {
                            let v = v as bool;
                            let x: JsonValue = v.into();
                            builder.append(&x, true);
                        }
                    }
                    TypeID::String => {
                        let c: &StringColumn = Series::check_get(column)?;
                        for v in c.iter() {
                            match std::str::from_utf8(v) {
                                Ok(v) => {
                                    match serde_json::from_str(v) {
                                        Ok(x) => builder.append(&x, true),
                                        Err(_) => builder.append_null(),
                                    }
                                }
                                Err(_) => builder.append_null(),
                            }
                        }
                    }
                    _ => {
                        for _ in 0..input_rows {
                            builder.append_null();
                        }
                    }
                }
            });
            return Ok(builder.build(input_rows));
        }

        if column.is_nullable() {
            let (_, source_valids) = column.validity();
            let nullable_column: &NullableColumn = Series::check_get(column)?;
            let column = nullable_column.inner();

            if column.data_type_id() == TypeID::Null {
                return NullType::arc().create_constant_column(&DataValue::Null, input_rows);
            }
            if column.data_type_id() == TypeID::Variant {
                return Ok(column.arc());
            }

            let mut builder = NullableColumnBuilder::<JsonValue>::with_capacity(input_rows);
            with_match_physical_primitive_type!(column.data_type_id().to_physical_type(), |$T| {
                let c: &<$T as Scalar>::ColumnType = Series::check_get(&column)?;
                for (i, v) in c.iter().enumerate() {
                    if let Some(source_valids) = source_valids {
                        if !source_valids.get_bit(i) {
                            builder.append_null();
                            continue;
                        }
                    }
                    let v = *v as $T;
                    let x: JsonValue = v.into();
                    builder.append(&x, true);
                }
            }, {
                match column.data_type_id() {
                    TypeID::Boolean => {
                        let c: &BooleanColumn = Series::check_get(column)?;
                        for (i, v) in c.iter().enumerate() {
                            if let Some(source_valids) = source_valids {
                                if !source_valids.get_bit(i) {
                                    builder.append_null();
                                    continue;
                                }
                            }
                            let v = v as bool;
                            let x: JsonValue = v.into();
                            builder.append(&x, true);
                        }
                    }
                    TypeID::String => {
                        let c: &StringColumn = Series::check_get(column)?;
                        for (i, v) in c.iter().enumerate() {
                            if let Some(source_valids) = source_valids {
                                if !source_valids.get_bit(i) {
                                    builder.append_null();
                                    continue;
                                }
                            }
                            match std::str::from_utf8(v) {
                                Ok(v) => {
                                    match serde_json::from_str(v) {
                                        Ok(x) => builder.append(&x, true),
                                        Err(e) => {
                                            return Err(ErrorCode::BadDataValueType(format!(
                                                "Error parsing JSON: {:?}", e
                                            )));
                                        }
                                    }
                                }
                                Err(e) => {
                                    return Err(ErrorCode::BadDataValueType(format!(
                                        "Error parsing JSON: {:?}", e
                                    )));
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Error parsing JSON: type does not match {:?}",
                            column.data_type_id()
                        )));
                    }
                }
            });
            return Ok(builder.build(input_rows));
        }

        let mut builder = ColumnBuilder::<JsonValue>::with_capacity(input_rows);
        with_match_physical_primitive_type!(column.data_type_id().to_physical_type(), |$T| {
            let c: &<$T as Scalar>::ColumnType = Series::check_get(&column)?;
            for v in c.iter() {
                let v = *v as $T;
                let x: JsonValue = v.into();
                builder.append(&x);
            }
        }, {
            match column.data_type_id() {
                TypeID::Boolean => {
                    let c: &BooleanColumn = Series::check_get(column)?;
                    for v in c.iter() {
                        let v = v as bool;
                        let x: JsonValue = v.into();
                        builder.append(&x);
                    }
                }
                TypeID::String => {
                    let c: &StringColumn = Series::check_get(column)?;
                    for v in c.iter() {
                        match std::str::from_utf8(v) {
                            Ok(v) => {
                                match serde_json::from_str(v) {
                                    Ok(x) => builder.append(&x),
                                    Err(e) => {
                                        return Err(ErrorCode::BadDataValueType(format!(
                                            "Error parsing JSON: {:?}", e
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(ErrorCode::BadDataValueType(format!(
                                    "Error parsing JSON: {:?}", e
                                )));
                            }
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "Error parsing JSON: type does not match {:?}",
                        column.data_type_id()
                    )));
                }
            }
        });
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
