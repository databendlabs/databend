// Copyright 2021 Datafuse Labs
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

use std::collections::HashSet;
use std::sync::Arc;

use bstr::ByteSlice;
use chrono::Datelike;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_expression::types::date::string_to_date;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::*;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::timestamp::string_to_timestamp;
use common_expression::types::variant::cast_scalar_to_variant;
use common_expression::types::variant::cast_scalars_to_variants;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::GenericType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::VariantType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionEval;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;
use jsonb::array_length;
use jsonb::as_bool;
use jsonb::as_f64;
use jsonb::as_i64;
use jsonb::as_str;
use jsonb::build_array;
use jsonb::build_object;
use jsonb::get_by_index;
use jsonb::get_by_name;
use jsonb::get_by_path;
use jsonb::get_by_path_array;
use jsonb::get_by_path_first;
use jsonb::is_array;
use jsonb::is_object;
use jsonb::jsonpath::parse_json_path;
use jsonb::object_keys;
use jsonb::parse_value;
use jsonb::path_exists;
use jsonb::strip_nulls;
use jsonb::to_bool;
use jsonb::to_f64;
use jsonb::to_i64;
use jsonb::to_pretty_string;
use jsonb::to_str;
use jsonb::to_string;
use jsonb::to_u64;
use jsonb::type_of;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("json_object_keys", &["object_keys"]);

    registry.register_passthrough_nullable_1_arg::<VariantType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
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

    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
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

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then check.
            let val = to_string(s);
            match parse_value(val.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(e.to_string().as_bytes()),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(e.to_string().as_bytes()),
            }
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<UInt32Type>>(|val, _| {
            val.and_then(|v| array_length(v).map(|v| v as u32))
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "json_object_keys",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(object_keys)
        }),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match std::str::from_utf8(name) {
                    Ok(name) => match get_by_name(val, name, false) {
                        Some(v) => {
                            output.push(&v);
                        }
                        None => {
                            output.push_null();
                        }
                    },
                    Err(err) => {
                        ctx.set_error(
                            output.len(),
                            format!(
                                "Unable convert name '{}' to string: {}",
                                &String::from_utf8_lossy(name),
                                err
                            ),
                        );
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<VariantType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX.into() {
                    output.push_null();
                } else {
                    match get_by_index(val, idx as usize) {
                        Some(v) => {
                            output.push(&v);
                        }
                        None => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_ignore_case",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match std::str::from_utf8(name) {
                    Ok(name) => match get_by_name(val, name, true) {
                        Some(v) => output.push(&v),
                        None => output.push_null(),
                    },
                    Err(err) => {
                        ctx.set_error(
                            output.len(),
                            format!(
                                "Unable convert name '{}' to string: {}",
                                &String::from_utf8_lossy(name),
                                err
                            ),
                        );
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "json_path_query_array",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path) {
                    Ok(json_path) => {
                        get_by_path_array(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        );
                        if output.builder.offsets.len() == output.len() + 1 {
                            output.push_null();
                        } else {
                            output.validity.push(true);
                        }
                    }
                    Err(_) => {
                        ctx.set_error(
                            output.len(),
                            format!("Invalid JSON Path '{}'", &String::from_utf8_lossy(path),),
                        );
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "json_path_query_first",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path) {
                    Ok(json_path) => {
                        get_by_path_first(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        );
                        if output.builder.offsets.len() == output.len() + 1 {
                            output.push_null();
                        } else {
                            output.validity.push(true);
                        }
                    }
                    Err(_) => {
                        ctx.set_error(
                            output.len(),
                            format!("Invalid JSON Path '{}'", &String::from_utf8_lossy(path),),
                        );
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, StringType, BooleanType, _, _>(
        "json_path_exists",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, BooleanType>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                match parse_json_path(path) {
                    Ok(json_path) => {
                        let res = path_exists(val, json_path);
                        output.push(res);
                    }
                    Err(_) => {
                        ctx.set_error(
                            output.len(),
                            format!("Invalid JSON Path '{}'", &String::from_utf8_lossy(path),),
                        );
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_path",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path) {
                    Ok(json_path) => {
                        get_by_path(
                            val,
                            json_path,
                            &mut output.builder.data,
                            &mut output.builder.offsets,
                        );
                        if output.builder.offsets.len() == output.len() + 1 {
                            output.push_null();
                        } else {
                            output.validity.push(true);
                        }
                    }
                    Err(_) => {
                        ctx.set_error(
                            output.len(),
                            format!("Invalid JSON Path '{}'", &String::from_utf8_lossy(path),),
                        );
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "json_extract_path_text",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<StringType>>(
            |s, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_value(s) {
                    Ok(val) => {
                        let mut buf = Vec::new();
                        val.write_to_vec(&mut buf);
                        match parse_json_path(path) {
                            Ok(json_path) => {
                                let mut out_buf = Vec::new();
                                let mut out_offsets = Vec::new();
                                get_by_path(&buf, json_path, &mut out_buf, &mut out_offsets);
                                if out_offsets.is_empty() {
                                    output.push_null();
                                } else {
                                    let json_str = to_string(&out_buf);
                                    output.builder.put(json_str.as_bytes());
                                    output.builder.commit_row();
                                    output.validity.push(true);
                                }
                            }
                            Err(_) => {
                                ctx.set_error(
                                    output.len(),
                                    format!(
                                        "Invalid JSON Path '{}'",
                                        &String::from_utf8_lossy(path),
                                    ),
                                );
                                output.push_null();
                            }
                        }
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "as_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_bool(v) {
                Some(val) => output.push(val),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Int64Type, _, _>(
        "as_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Int64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_i64(v) {
                Some(val) => output.push(val),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Float64Type, _, _>(
        "as_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Float64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_f64(v) {
                Some(val) => output.push(val.into()),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "as_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_str(v) {
                Some(val) => output.push(val.as_bytes()),
                None => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            if is_array(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            if is_object(v) {
                output.push(v.as_bytes());
            } else {
                output.push_null()
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "to_variant",
        |_, _| FunctionDomain::Full,
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => {
                let mut buf = Vec::new();
                cast_scalar_to_variant(scalar, ctx.func_ctx.tz, &mut buf);
                Value::Scalar(buf)
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                Value::Column(new_col)
            }
        },
    );

    registry.register_combine_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "try_to_variant",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(())),
            })
        },
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => {
                let mut buf = Vec::new();
                cast_scalar_to_variant(scalar, ctx.func_ctx.tz, &mut buf);
                Value::Scalar(Some(buf))
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                Value::Column(NullableColumn {
                    validity: Bitmap::new_constant(true, new_col.len()),
                    column: new_col,
                })
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "to_boolean",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match to_bool(val) {
                Ok(value) => output.push(value),
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(false);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match to_bool(val) {
                    Ok(value) => {
                        output.push(value);
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
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
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match to_str(val) {
                    Ok(value) => {
                        output.validity.push(true);
                        output.builder.put_slice(value.as_bytes());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, DateType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            match as_str(val).and_then(|val| string_to_date(val.as_bytes(), ctx.func_ctx.tz.tz)) {
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
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match as_str(val)
                .and_then(|str_value| string_to_date(str_value.as_bytes(), ctx.func_ctx.tz.tz))
            {
                Some(date) => output.push(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                None => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, TimestampType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            match as_str(val)
                .and_then(|val| string_to_timestamp(val.as_bytes(), ctx.func_ctx.tz.tz))
            {
                Some(ts) => output.push(ts.timestamp_micros()),
                None => {
                    ctx.set_error(output.len(), "unable to cast to type `TIMESTAMP`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match as_str(val) {
                    Some(str_val) => {
                        let timestamp = string_to_timestamp(str_val.as_bytes(), ctx.func_ctx.tz.tz)
                            .map(|ts| ts.timestamp_micros());
                        match timestamp {
                            Some(timestamp) => output.push(timestamp),
                            None => output.push_null(),
                        }
                    }
                    None => output.push_null(),
                }
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
                        |_, _| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<VariantType, NumberType<NUM_TYPE>>(
                            move |val, output, ctx| {
                                if let Some(validity) = &ctx.validity {
                                    if !validity.get_bit(output.len()) {
                                        output.push(NUM_TYPE::default());
                                        return;
                                    }
                                }
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
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            VariantType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(move |val, output, ctx| {
                            if let Some(validity) = &ctx.validity {
                                if !validity.get_bit(output.len()) {
                                    output.push_null();
                                    return;
                                }
                            }
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

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "json_to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let s = to_string(val);
            output.put_slice(s.as_bytes());
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "json_pretty",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let s = to_pretty_string(val);
            output.put_slice(s.as_bytes());
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg(
        "json_strip_nulls",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            if let Err(err) = strip_nulls(val, &mut output.data) {
                ctx.set_error(output.len(), err.to_string());
            };
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg(
        "json_typeof",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match type_of(val) {
                Ok(result) => output.put_str(result),
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            };
            output.commit_row();
        }),
    );

    registry.register_function_factory("json_object", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_object_fn),
            },
        }))
    });

    registry.register_function_factory("try_json_object", |_, args_type| {
        let f = Function {
            signature: FunctionSignature {
                name: "try_json_object".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(json_object_fn),
            },
        };
        Some(Arc::new(f.error_to_null()))
    });

    registry.register_function_factory("json_object_keep_null", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_keep_null".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_object_keep_null_fn),
            },
        }))
    });

    registry.register_function_factory("try_json_object_keep_null", |_, args_type| {
        let f = Function {
            signature: FunctionSignature {
                name: "try_json_object_keep_null".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(json_object_keep_null_fn),
            },
        };
        Some(Arc::new(f.error_to_null()))
    });

    registry.register_function_factory("json_array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_array".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(json_array_fn),
            },
        }))
    });
}

fn json_array_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(cap, cap * 50);
    let mut items = Vec::with_capacity(columns.len());

    for idx in 0..cap {
        items.clear();
        for column in &columns {
            let v = unsafe { column.index_unchecked(idx) };
            let mut val = vec![];
            cast_scalar_to_variant(v, ctx.func_ctx.tz, &mut val);
            items.push(val);
        }
        if let Err(err) = build_array(items.iter().map(|b| &b[..]), &mut builder.data) {
            ctx.set_error(builder.len(), err.to_string());
        };
        builder.commit_row();
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}

fn json_object_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, false)
}

fn json_object_keep_null_fn(args: &[ValueRef<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, true)
}

fn json_object_impl_fn(
    args: &[ValueRef<AnyType>],
    ctx: &mut EvalContext,
    keep_null: bool,
) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(cap, cap * 50);
    if columns.len() % 2 != 0 {
        for i in 0..cap {
            ctx.set_error(i, "The number of keys and values must be equal");
            builder.commit_row();
        }
    } else {
        let mut set = HashSet::new();
        let mut kvs = Vec::with_capacity(columns.len() / 2);
        for idx in 0..cap {
            set.clear();
            kvs.clear();
            let mut has_err = false;
            for i in (0..columns.len()).step_by(2) {
                let k = unsafe { columns[i].index_unchecked(idx) };
                if k == ScalarRef::Null {
                    continue;
                }
                let v = unsafe { columns[i + 1].index_unchecked(idx) };
                if v == ScalarRef::Null && !keep_null {
                    continue;
                }
                let key = match k {
                    ScalarRef::String(v) => unsafe { String::from_utf8_unchecked(v.to_vec()) },
                    _ => {
                        has_err = true;
                        ctx.set_error(builder.len(), "Key must be a string value");
                        break;
                    }
                };
                if set.contains(&key) {
                    has_err = true;
                    ctx.set_error(builder.len(), "Keys have to be unique");
                    break;
                }
                set.insert(key.clone());
                let mut val = vec![];
                cast_scalar_to_variant(v, ctx.func_ctx.tz, &mut val);
                kvs.push((key, val));
            }
            if !has_err {
                if let Err(err) =
                    build_object(kvs.iter().map(|(k, v)| (k, &v[..])), &mut builder.data)
                {
                    ctx.set_error(builder.len(), err.to_string());
                }
            }
            builder.commit_row();
        }
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}

fn prepare_args_columns(
    args: &[ValueRef<AnyType>],
    ctx: &EvalContext,
) -> (Vec<Column>, Option<usize>) {
    let len_opt = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut columns = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let column = match arg {
            ValueRef::Column(column) => column.clone(),
            ValueRef::Scalar(s) => {
                let column_builder = ColumnBuilder::repeat(s, len, &ctx.generics[i]);
                column_builder.build()
            }
        };
        columns.push(column);
    }
    (columns, len_opt)
}
