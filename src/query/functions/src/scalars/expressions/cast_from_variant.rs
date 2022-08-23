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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues::chrono::Datelike;
use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value as JsonValue;

use super::cast_from_string::string_to_date;
use super::cast_from_string::string_to_timestamp;
use crate::scalars::FunctionContext;

pub fn cast_from_variant(
    column: &ColumnRef,
    data_type: &DataTypeImpl,
    func_ctx: &FunctionContext,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let column = Series::remove_nullable(column);
    let variant_column: &VariantColumn = Series::check_get(&column)?;
    let size = variant_column.len();

    with_match_primitive_type_id!(data_type.data_type_id(), |$T| {
        let mut builder = NullableColumnBuilder::<$T>::with_capacity(size);

        for value in variant_column.iter() {
            match value.as_ref() {
                JsonValue::Null => builder.append_null(),
                JsonValue::Bool(v) => {
                    if *v {
                        builder.append(1 as $T, true);
                    } else {
                        builder.append(0 as $T, true);
                    }
                }
                JsonValue::Number(v) => {
                    if v.is_u64() {
                        let num = v.as_u64().unwrap();
                        builder.append(num as $T, true);
                    } else if v.is_i64() {
                        let num = v.as_i64().unwrap();
                        builder.append(num as $T, true);
                    } else if v.is_f64() {
                        let num = v.as_f64().unwrap();
                        builder.append(num as $T, true);
                    }
                }
                JsonValue::String(v) => {
                    match v.parse::<$T>() {
                        Ok(num) => builder.append(num as $T, true),
                        Err(_) => builder.append_null(),
                    }
                }
                _ => builder.append_null(),
            }
        }
        let column = builder.build(size);
        let nullable_column: &NullableColumn = Series::check_get(&column)?;
        Ok((
            nullable_column.inner().clone(),
            Some(nullable_column.ensure_validity().clone()),
        ))
    }, {
        match data_type.data_type_id() {
            TypeID::Boolean => {
                let mut builder = NullableColumnBuilder::<bool>::with_capacity(size);

                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::Bool(v) => builder.append(*v, true),
                        JsonValue::String(v) => {
                            if v.to_lowercase() == *"true".to_string() {
                                builder.append(true, true);
                            } else if v.to_lowercase() == *"false".to_string() {
                                builder.append(false, true);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            TypeID::String => {
                let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(size);

                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::String(v) => {
                            builder.append(v.as_bytes(), true);
                        },
                        _ => {
                            builder.append(value.to_string().as_bytes(), true);
                        }
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            TypeID::Date => {
                let mut builder = NullableColumnBuilder::<i32>::with_capacity(size);
                let tz = func_ctx.tz;
                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::String(v) => {
                            if let Some(d) = string_to_date(v, &tz) {
                                builder.append((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i32, true);
                            } else {
                                builder.append_null();
                            }
                        },
                        _ => builder.append_null(),
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            TypeID::Timestamp => {
                let mut builder = NullableColumnBuilder::<i64>::with_capacity(size);
                let tz = func_ctx.tz;
                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::String(v) => {
                            if let Some(d) = string_to_timestamp(v, &tz) {
                                builder.append(d.timestamp_micros(), true);
                            } else {
                                builder.append_null();
                            }
                        },
                        _ => builder.append_null(),
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            TypeID::Variant => {
                return Ok((variant_column.arc(), None));
            }
            TypeID::VariantArray => {
                let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(size);

                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::Array(_) => {
                            builder.append(value, true);
                        },
                        _ => {
                            let arr = JsonValue::Array(vec![value.as_ref().clone()]);
                            builder.append(&VariantValue::from(arr), true);
                        }
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            TypeID::VariantObject => {
                let mut builder = NullableColumnBuilder::<VariantValue>::with_capacity(size);

                for value in variant_column.iter() {
                    match value.as_ref() {
                        JsonValue::Null => builder.append_null(),
                        JsonValue::Object(_) => {
                            builder.append(value, true);
                        },
                        _ => return Err(ErrorCode::BadDataValueType(format!(
                            "Failed to cast variant value {} to OBJECT",
                            value
                        ))),
                    }
                }
                let column = builder.build(size);
                let nullable_column: &NullableColumn = Series::check_get(&column)?;
                Ok((
                    nullable_column.inner().clone(),
                    Some(nullable_column.ensure_validity().clone()),
                ))
            }
            _ => {
                Err(ErrorCode::BadDataValueType(format!(
                            "Unsupported cast variant value to {}",
                            data_type.data_type_id()
                )))
            }
        }
    })
}
