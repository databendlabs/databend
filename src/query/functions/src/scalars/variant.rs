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

use std::borrow::Cow;

use bstr::ByteSlice;
use chrono::Datelike;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_expression::types::date::string_to_date;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::*;
use common_expression::types::timestamp::string_to_timestamp;
use common_expression::types::variant::cast_scalar_to_variant;
use common_expression::types::variant::cast_scalars_to_variants;
use common_expression::types::variant::JSONB_NULL;
use common_expression::types::BooleanType;
use common_expression::types::DateType;
use common_expression::types::GenericType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::VariantType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::utils::arrow::constant_bitmap;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_2_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;
use common_jsonb::array_length;
use common_jsonb::as_bool;
use common_jsonb::as_f64;
use common_jsonb::as_i64;
use common_jsonb::as_str;
use common_jsonb::get_by_name_ignore_case;
use common_jsonb::get_by_path;
use common_jsonb::is_array;
use common_jsonb::is_object;
use common_jsonb::object_keys;
use common_jsonb::parse_json_path;
use common_jsonb::parse_value;
use common_jsonb::to_bool;
use common_jsonb::to_f64;
use common_jsonb::to_i64;
use common_jsonb::to_str;
use common_jsonb::to_u64;
use common_jsonb::JsonPathRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, ctx| {
            if s.trim().is_empty() {
                output.put_slice(JSONB_NULL);
                output.commit_row();
                return;
            }
            match parse_value(s) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            }
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, VariantType, _, _>(
        "try_parse_json",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<VariantType>>(|s, output, _| {
            if s.trim().is_empty() {
                output.push(JSONB_NULL);
            } else {
                match parse_value(s) {
                    Ok(value) => {
                        output.validity.push(true);
                        value.write_to_vec(&mut output.builder.data);
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, StringType, _, _>(
        "check_json",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, NullableType<StringType>>(|s, output, _| {
            if s.trim().is_empty() {
                output.push_null();
            } else {
                match parse_value(s) {
                    Ok(_) => output.push_null(),
                    Err(e) => output.push(e.to_string().as_bytes()),
                }
            }
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<UInt32Type>>(|val, _| {
            val.and_then(|v| array_length(v).map(|v| v as u32))
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "object_keys",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(object_keys)
        }),
    );

    registry.register_2_arg_core::<NullableType<VariantType>, NullableType<UInt64Type>, NullableType<VariantType>, _, _>(
        "get",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<NullableType<VariantType>, NullableType<UInt64Type>, NullableType<VariantType>>(|val, idx, _| {
            match (val, idx) {
                (Some(val), Some(idx)) => {
                    if val.is_empty() {
                        None
                    } else {
                        let json_path = JsonPathRef::UInt64(idx);
                        get_by_path(val, vec![json_path])
                    }
                }
                (_, _) => None,
            }
        }),
    );

    registry.register_2_arg_core::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>, _, _>(
        "get",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_2_arg::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>>(|val, name, _| {
            match (val, name) {
                (Some(val), Some(name)) => {
                    if val.is_empty() || name.trim().is_empty() {
                        None
                    } else {
                        let name = String::from_utf8(name.to_vec()).map_err(|err| {
                            format!(
                                "Unable convert name '{}' to string: {}",
                                &String::from_utf8_lossy(name),
                                err
                            )
                        }).ok()?;
                        let json_path = JsonPathRef::String(Cow::Borrowed(&name));
                        get_by_path(val, vec![json_path])
                    }
                }
                (_, _) => None,
            }
        }),
    );

    registry.register_2_arg_core::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>, _, _>(
        "get_ignore_case",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_2_arg::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>>(|val, name, _| {
            match (val, name) {
                (Some(val), Some(name)) => {
                    if val.is_empty() || name.trim().is_empty() {
                        None
                    } else {
                        let name = String::from_utf8(name.to_vec()).map_err(|err| {
                            format!(
                                "Unable convert name '{}' to string: {}",
                                &String::from_utf8_lossy(name),
                                err
                            )
                        }).ok()?;
                        get_by_name_ignore_case(val, &name)
                    }
                }
                (_, _) => None,
            }
        }),
    );

    registry.register_2_arg_core::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>, _, _>(
        "get_path",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_2_arg::<NullableType<VariantType>, NullableType<StringType>, NullableType<VariantType>>(|val, path, _| {
            match (val, path) {
                (Some(val), Some(path)) => {
                    if val.is_empty() || path.is_empty() {
                        None
                    } else {
                        let json_paths = parse_json_path(path).map_err(|err| {
                            format!(
                                "Invalid extraction path '{}': {}",
                                &String::from_utf8_lossy(path),
                                err
                            )
                        }).ok()?;
                        get_by_path(val, json_paths)
                    }
                }
                (_, _) => None,
            }
        }),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "json_extract_path_text",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<StringType>>(
            |s, path, output, _ctx| {
                if s.is_empty() || path.is_empty() {
                    output.push_null();
                } else {
                    let value = common_jsonb::parse_value(s);
                    let json_paths = parse_json_path(path);

                    match (value, json_paths) {
                        (Ok(value), Ok(json_paths)) => match value.get_by_path(&json_paths) {
                            Some(val) => {
                                let json_val = format!("{val}");
                                output.push(json_val.as_bytes());
                            }
                            None => output.push_null(),
                        },
                        _ => output.push_null(),
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "as_boolean",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|v, output, _| {
            if v.is_empty() {
                output.push_null();
            } else {
                match as_bool(v) {
                    Some(val) => output.push(val),
                    None => output.push_null(),
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Int64Type, _, _>(
        "as_integer",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Int64Type>>(|v, output, _| {
            if v.is_empty() {
                output.push_null();
            } else {
                match as_i64(v) {
                    Some(val) => output.push(val),
                    None => output.push_null(),
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Float64Type, _, _>(
        "as_float",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Float64Type>>(|v, output, _| {
            if v.is_empty() {
                output.push_null();
            } else {
                match as_f64(v) {
                    Some(val) => output.push(val.into()),
                    None => output.push_null(),
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "as_string",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|v, output, _| {
            if v.is_empty() {
                output.push_null();
            } else {
                match as_str(v) {
                    Some(val) => output.push(val.as_bytes()),
                    None => output.push_null(),
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_array",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, _| {
            if v.is_empty() {
                output.push_null()
            } else if is_array(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_object",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, _| {
            if v.is_empty() {
                output.push_null()
            } else if is_object(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "to_variant",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => {
                let mut buf = Vec::new();
                cast_scalar_to_variant(scalar, ctx.tz, &mut buf);
                Value::Scalar(buf)
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.tz);
                Value::Column(new_col)
            }
        },
    );

    registry.register_combine_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "try_to_variant",
        FunctionProperty::default(),
        |_| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(())),
            })
        },
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => {
                let mut buf = Vec::new();
                cast_scalar_to_variant(scalar, ctx.tz, &mut buf);
                Value::Scalar(Some(buf))
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.tz);
                Value::Column(NullableColumn {
                    validity: constant_bitmap(true, new_col.len()).into(),
                    column: new_col,
                })
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "to_boolean",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| match to_bool(
            val,
        ) {
            Ok(value) => output.push(value),
            Err(err) => {
                ctx.set_error(output.len(), err.to_string());
                output.push(false);
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|val, output, _| {
            match to_bool(val) {
                Ok(value) => {
                    output.validity.push(true);
                    output.builder.push(value);
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            match to_str(val) {
                Ok(value) => output.put_slice(value.as_bytes()),
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            }
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "try_to_string",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|val, output, _| {
            match to_str(val) {
                Ok(value) => {
                    output.validity.push(true);
                    output.builder.put_slice(value.as_bytes());
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, DateType>(|val, output, ctx| {
            match as_str(val).and_then(|val| string_to_date(val.as_bytes(), ctx.tz.tz)) {
                Some(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                None => {
                    ctx.set_error(output.len(), "unable to cast to type `DATE`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "try_to_date",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|val, output, ctx| {
            match as_str(val).and_then(|str_value| string_to_date(str_value.as_bytes(), ctx.tz.tz))
            {
                Some(date) => output.push(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                None => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, TimestampType>(
            |val, output, ctx| match as_str(val)
                .and_then(|val| string_to_timestamp(val.as_bytes(), ctx.tz.tz))
            {
                Some(ts) => output.push(ts.timestamp_micros()),
                None => {
                    ctx.set_error(output.len(), "unable to cast to type `TIMESTAMP`");
                    output.push(0);
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "try_to_timestamp",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |val, output, ctx| match as_str(val) {
                Some(str_val) => {
                    let timestamp = string_to_timestamp(str_val.as_bytes(), ctx.tz.tz)
                        .map(|ts| ts.timestamp_micros());
                    match timestamp {
                        Some(timestamp) => output.push(timestamp),
                        None => output.push_null(),
                    }
                }
                None => output.push_null(),
            },
        ),
    );

    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match dest_type {
            NumberDataType::NUM_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                registry
                    .register_passthrough_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        FunctionProperty::default(),
                        |_| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<VariantType, NumberType<NUM_TYPE>>(
                            move |val, output, ctx| {
                                type Native = <NUM_TYPE as Number>::Native;

                                let value: Option<Native> = if dest_type.is_float() {
                                    to_f64(val).ok().and_then(num_traits::cast::cast)
                                } else if dest_type.is_signed() {
                                    to_i64(val).ok().and_then(num_traits::cast::cast)
                                } else {
                                    to_u64(val).ok().and_then(num_traits::cast::cast)
                                };
                                match value {
                                    Some(value) => output.push(value.into()),
                                    None => {
                                        ctx.set_error(
                                            output.len(),
                                            format!("unable to cast to type {dest_type}",),
                                        );
                                        output.push(NUM_TYPE::default());
                                    }
                                }
                            },
                        ),
                    );

                let name = format!("try_to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        FunctionProperty::default(),
                        |_| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            VariantType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(move |val, output, _| {
                            if dest_type.is_float() {
                                if let Ok(value) = to_f64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else if dest_type.is_signed() {
                                if let Ok(value) = to_i64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else {
                                if let Ok(value) = to_u64(val) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            }
                        }),
                    );
            }
        });
    }
}
