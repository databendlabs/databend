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
use super::cast_with_type::new_mutable_bitmap;

pub fn cast_from_variant(
    column: &ColumnRef,
    data_type: &DataTypePtr,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let column = Series::remove_nullable(column);
    let json_column: &VariantColumn = if column.is_const() {
        let const_column: &ConstColumn = Series::check_get(&column)?;
        Series::check_get(const_column.inner())?
    } else {
        Series::check_get(&column)?
    };
    let size = json_column.len();
    let mut bitmap = new_mutable_bitmap(size, true);

    with_match_primitive_type_id!(data_type.data_type_id(), |$T| {
        let mut builder = ColumnBuilder::<$T>::with_capacity(size);

        for (row, value) in json_column.iter().enumerate() {
            match value.as_ref() {
                JsonValue::Null => bitmap.set(row, false),
                JsonValue::Bool(v) => {
                    if *v {
                        builder.append(1 as $T);
                    } else {
                        builder.append(0 as $T);
                    }
                }
                JsonValue::Number(v) => {
                    if v.is_u64() {
                        let num = v.as_u64().unwrap();
                        builder.append(num as $T);
                    } else if v.is_i64() {
                        let num = v.as_i64().unwrap();
                        builder.append(num as $T);
                    } else if v.is_f64() {
                        let num = v.as_f64().unwrap();
                        builder.append(num as $T);
                    }
                }
                JsonValue::String(v) => {
                    match v.parse::<$T>() {
                        Ok(num) => builder.append(num as $T),
                        Err(_) => bitmap.set(row, false),
                    }
                }
                _ => bitmap.set(row, false),
            }
        }
        return Ok((builder.build(size), Some(bitmap.into())));
    }, {
        match data_type.data_type_id() {
            TypeID::Boolean => {
                let mut builder = ColumnBuilder::<bool>::with_capacity(size);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::Bool(v) => builder.append(*v),
                        JsonValue::String(v) => {
                            if v.to_lowercase() == *"true".to_string() {
                                builder.append(true)
                            } else if v.to_lowercase() == *"false".to_string() {
                                builder.append(false)
                            } else {
                                bitmap.set(row, false)
                            }
                        }
                        _ => bitmap.set(row, false),
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            TypeID::String => {
                let mut builder = ColumnBuilder::<Vu8>::with_capacity(size);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::String(v) => {
                            builder.append(v.as_bytes());
                        },
                        _ => {
                            builder.append(value.to_string().as_bytes());
                        }
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            TypeID::Date => {
                let mut builder = ColumnBuilder::<i32>::with_capacity(size);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::String(v) => {
                            if let Some(d) = string_to_date(v) {
                                builder.append((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i32);
                            } else {
                                bitmap.set(row, false);
                            }
                        },
                        _ => bitmap.set(row, false),
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            TypeID::Timestamp => {
                // TODO(veeupup): support datetime with precision
                let mut builder = ColumnBuilder::<i64>::with_capacity(size);
                let datetime = TimestampType::create(0, None);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::String(v) => {
                            if let Some(d) = string_to_timestamp(v) {
                                builder.append(datetime.from_nano_seconds(d.timestamp_nanos()));
                            } else {
                                bitmap.set(row, false);
                            }
                        },
                        _ => bitmap.set(row, false),
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            TypeID::Variant => {
                return Ok((json_column.arc(), None));
            }
            TypeID::VariantArray => {
                let mut builder = ColumnBuilder::<VariantValue>::with_capacity(size);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::Array(_) => {
                            builder.append(value);
                        },
                        _ => {
                            let arr = JsonValue::Array(vec![value.as_ref().clone()]);
                            builder.append(&VariantValue::from(arr));
                        }
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            TypeID::VariantObject => {
                let mut builder = ColumnBuilder::<VariantValue>::with_capacity(size);

                for (row, value) in json_column.iter().enumerate() {
                    match value.as_ref() {
                        JsonValue::Null => bitmap.set(row, false),
                        JsonValue::Object(_) => {
                            builder.append(value);
                        },
                        _ => return Err(ErrorCode::BadDataValueType(format!(
                            "Failed to cast variant value {} to OBJECT",
                            value
                        ))),
                    }
                }
                return Ok((builder.build(size), Some(bitmap.into())));
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                            "Unsupported cast variant value to {}",
                            data_type.data_type_id()
                )));
            }
        }
    });
}
