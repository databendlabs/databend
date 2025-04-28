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

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::iter::once;
use std::sync::Arc;

use bstr::ByteSlice;
use databend_common_column::types::months_days_micros;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::*;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::variant::cast_scalars_to_variants;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use jiff::civil::date;
use jiff::Unit;
use jsonb::jsonpath::parse_json_path;
use jsonb::keypath::parse_key_paths;
use jsonb::parse_value;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("json_object_keys", &["object_keys"]);
    registry.register_aliases("to_string", &["json_to_string"]);

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
            let val = RawJsonb::new(s).to_string();
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
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
            match parse_value(s.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
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
            let val = RawJsonb::new(s).to_string();
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
            match parse_value(s.as_bytes()) {
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
            let val = RawJsonb::new(s).to_string();
            match parse_value(val.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
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
            match parse_value(s.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
            }
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<UInt32Type>>(|val, _| {
            val.and_then(|v| match RawJsonb::new(v).array_length() {
                Ok(len) => len.map(|len| len as u32),
                Err(_) => parse_value(v)
                    .ok()
                    .and_then(|v| v.array_length().map(|len| len as u32)),
            })
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "json_object_keys",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(|v| match RawJsonb::new(v).object_keys() {
                Ok(obj_keys) => obj_keys.map(|v| v.to_vec()),
                Err(_) => parse_value(v).ok().and_then(|v| {
                    v.object_keys().map(|obj_keys| {
                        let mut buf = Vec::new();
                        obj_keys.write_to_vec(&mut buf);
                        buf
                    })
                }),
            })
        }),
    );

    registry.register_function_factory("get_by_keypath", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_by_keypath".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| get_by_keypath_fn(args, ctx, false)),
            },
        }))
    });

    registry.register_function_factory("get_by_keypath_string", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_by_keypath_string".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::String)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| get_by_keypath_fn(args, ctx, true)),
            },
        }))
    });

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb::new(val).get_by_name(name, false) {
                    Ok(Some(v)) => {
                        output.push(v.as_ref());
                    }
                    Ok(None) => {
                        output.push_null();
                    }
                    Err(_) => {
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
                if idx < 0 || idx > i32::MAX as i64 {
                    output.push_null();
                } else {
                    match RawJsonb::new(val).get_by_index(idx as usize) {
                        Ok(Some(v)) => {
                            output.push(v.as_ref());
                        }
                        Ok(None) => {
                            output.push_null();
                        }
                        Err(_) => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_ignore_case",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb::new(val).get_by_name(name, true) {
                    Ok(Some(v)) => output.push(v.as_ref()),
                    Ok(None) => output.push_null(),
                    Err(_) => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<StringType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb::new(val).get_by_name(name, false) {
                    Ok(Some(v)) => {
                        let raw_jsonb = v.as_raw();
                        if let Ok(Some(s)) = raw_jsonb.as_str() {
                            output.push(&s);
                        } else if raw_jsonb.is_null().unwrap_or_default() {
                            output.push_null();
                        } else {
                            let json_str = raw_jsonb.to_string();
                            output.push(&json_str);
                        }
                    }
                    _ => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<StringType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX as i64 {
                    output.push_null();
                } else {
                    match RawJsonb::new(val).get_by_index(idx as usize) {
                        Ok(Some(v)) => {
                            let raw_jsonb = v.as_raw();
                            if let Ok(Some(s)) = raw_jsonb.as_str() {
                                output.push(&s);
                            } else if raw_jsonb.is_null().unwrap_or_default() {
                                output.push_null();
                            } else {
                                let json_str = raw_jsonb.to_string();
                                output.push(&json_str);
                            }
                        }
                        _ => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "json_path_query_array",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |v, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => match RawJsonb::new(v).select_array_by_path(&json_path) {
                        Ok(owned_jsonb) => {
                            output.push(owned_jsonb.as_ref());
                        }
                        Err(err) => {
                            ctx.set_error(
                                output.len(),
                                format!("Select json path array failed err: {}", err),
                            );
                            output.push_null();
                        }
                    },
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
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
            |v, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => match RawJsonb::new(v).select_first_by_path(&json_path) {
                        Ok(owned_jsonb_opt) => match owned_jsonb_opt {
                            Some(owned_jsonb) => {
                                output.push(owned_jsonb.as_ref());
                            }
                            None => {
                                output.push_null();
                            }
                        },
                        Err(err) => {
                            ctx.set_error(
                                output.len(),
                                format!("Select json path first failed err: {}", err),
                            );
                            output.push_null();
                        }
                    },
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_function_factory("json_path_match", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_match".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Boolean)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(|args, ctx| path_predicate_fn(args, ctx, true)),
            },
        }))
    });

    registry.register_function_factory("json_path_exists", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_exists".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Boolean)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, ctx| path_predicate_fn(args, ctx, false)),
            },
        }))
    });

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_path",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |v, path, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match parse_json_path(path.as_bytes()) {
                    Ok(json_path) => match RawJsonb::new(v).select_value_by_path(&json_path) {
                        Ok(owned_jsonb_opt) => match owned_jsonb_opt {
                            Some(owned_jsonb) => {
                                output.push(owned_jsonb.as_ref());
                            }
                            None => {
                                output.push_null();
                            }
                        },
                        Err(err) => {
                            ctx.set_error(
                                output.len(),
                                format!("Select json path failed err: {}", err),
                            );
                            output.push_null();
                        }
                    },
                    Err(_) => {
                        ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
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
                match parse_value(s.as_bytes()) {
                    Ok(val) => {
                        let mut buf = Vec::new();
                        val.write_to_vec(&mut buf);
                        match parse_json_path(path.as_bytes()) {
                            Ok(json_path) => {
                                match RawJsonb::new(&buf).select_value_by_path(&json_path) {
                                    Ok(owned_jsonb_opt) => match owned_jsonb_opt {
                                        Some(v) => {
                                            let raw_jsonb = v.as_raw();
                                            if let Ok(Some(s)) = raw_jsonb.as_str() {
                                                output.push(&s);
                                            } else if raw_jsonb.is_null().unwrap_or_default() {
                                                output.push_null();
                                            } else {
                                                let json_str = raw_jsonb.to_string();
                                                output.push(&json_str);
                                            }
                                        }
                                        None => {
                                            output.push_null();
                                        }
                                    },
                                    Err(err) => {
                                        ctx.set_error(
                                            output.len(),
                                            format!("Select json path text failed err: {}", err),
                                        );
                                        output.push_null();
                                    }
                                }
                            }
                            Err(_) => {
                                ctx.set_error(output.len(), format!("Invalid JSON Path '{path}'"));
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
            match RawJsonb::new(v).as_bool() {
                Ok(Some(res)) => output.push(res),
                Ok(None) => output.push_null(),
                Err(_) => match parse_value(v).ok().and_then(|v| v.as_bool()) {
                    Some(res) => output.push(res),
                    None => output.push_null(),
                },
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
            match RawJsonb::new(v).as_i64() {
                Ok(Some(res)) => output.push(res),
                Ok(None) => output.push_null(),
                Err(_) => match parse_value(v).ok().and_then(|v| v.as_i64()) {
                    Some(res) => output.push(res),
                    None => output.push_null(),
                },
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
            match RawJsonb::new(v).as_f64() {
                Ok(Some(res)) => output.push(res.into()),
                Ok(None) => output.push_null(),
                Err(_) => match parse_value(v).ok().and_then(|v| v.as_f64()) {
                    Some(res) => output.push(res.into()),
                    None => output.push_null(),
                },
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
            match RawJsonb::new(v).as_str() {
                Ok(Some(res)) => output.push(&res),
                Ok(None) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if let Some(res) = val.as_str() {
                            output.push(res);
                            return;
                        }
                    }
                    output.push_null();
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_binary",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_binary() {
                Ok(res) => output.push(res),
                Err(_) => output.push(false),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BinaryType, _, _>(
        "as_binary",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BinaryType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb::new(v).as_binary() {
                Ok(Some(res)) => output.push(&res),
                _ => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_date",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_date() {
                Ok(res) => output.push(res),
                Err(_) => output.push(false),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "as_date",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb::new(v).as_date() {
                Ok(Some(res)) => output.push(res.value),
                _ => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_timestamp() {
                Ok(res) => output.push(res),
                Err(_) => output.push(false),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "as_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |v, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb::new(v).as_timestamp() {
                    Ok(Some(res)) => output.push(res.value),
                    _ => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_interval",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_interval() {
                Ok(res) => output.push(res),
                Err(_) => output.push(false),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, IntervalType, _, _>(
        "as_interval",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<IntervalType>>(
            |v, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb::new(v).as_interval() {
                    Ok(Some(res)) => {
                        output.push(months_days_micros::new(res.months, res.days, res.micros))
                    }
                    _ => output.push_null(),
                }
            },
        ),
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
            match RawJsonb::new(v).is_array() {
                Ok(true) => output.push(v.as_bytes()),
                Ok(false) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if val.is_array() {
                            let mut buf = Vec::new();
                            val.write_to_vec(&mut buf);
                            output.push(&buf);
                            return;
                        }
                    }
                    output.push_null();
                }
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
            match RawJsonb::new(v).is_object() {
                Ok(true) => output.push(v.as_bytes()),
                Ok(false) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if val.is_object() {
                            let mut buf = Vec::new();
                            val.write_to_vec(&mut buf);
                            output.push(&buf);
                            return;
                        }
                    }
                    output.push_null();
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_null_value",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_null() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_null()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_boolean() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_boolean()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_i64() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_i64()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_f64() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_f64()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_string() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_string()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_array() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_array()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb::new(v).is_object() {
                Ok(res) => output.push(res),
                Err(_) => match parse_value(v).ok().map(|v| v.is_object()) {
                    Some(res) => output.push(res),
                    None => output.push(false),
                },
            }
        }),
    );

    registry.register_function_factory("to_variant", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        let return_type = if args_type[0].is_nullable_or_null() {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_variant".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, args_domain| match &args_domain[0] {
                    Domain::Nullable(nullable_domain) => {
                        FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                            has_null: nullable_domain.has_null,
                            value: Some(Box::new(Domain::Undefined)),
                        }))
                    }
                    _ => FunctionDomain::Domain(Domain::Undefined),
                }),
                eval: Box::new(|args, ctx| match &args[0] {
                    Value::Scalar(scalar) => match scalar {
                        Scalar::Null => Value::Scalar(Scalar::Null),
                        _ => {
                            let mut buf = Vec::new();
                            cast_scalar_to_variant(scalar.as_ref(), &ctx.func_ctx.tz, &mut buf);
                            Value::Scalar(Scalar::Variant(buf))
                        }
                    },
                    Value::Column(col) => {
                        let validity = match col {
                            Column::Null { len } => Some(Bitmap::new_constant(false, *len)),
                            Column::Nullable(box ref nullable_column) => {
                                Some(nullable_column.validity.clone())
                            }
                            _ => None,
                        };
                        let new_col = cast_scalars_to_variants(col.iter(), &ctx.func_ctx.tz);
                        if let Some(validity) = validity {
                            Value::Column(NullableColumn::new_column(
                                Column::Variant(new_col),
                                validity,
                            ))
                        } else {
                            Value::Column(Column::Variant(new_col))
                        }
                    }
                }),
            },
        }))
    });

    registry.register_combine_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "try_to_variant",
        |_, domain| {
            let has_null = match domain {
                Domain::Nullable(nullable_domain) => nullable_domain.has_null,
                _ => false,
            };
            FunctionDomain::Domain(NullableDomain {
                has_null,
                value: Some(Box::new(())),
            })
        },
        |val, ctx| match val {
            Value::Scalar(scalar) => match scalar {
                Scalar::Null => Value::Scalar(None),
                _ => {
                    let mut buf = Vec::new();
                    cast_scalar_to_variant(scalar.as_ref(), &ctx.func_ctx.tz, &mut buf);
                    Value::Scalar(Some(buf))
                }
            },
            Value::Column(col) => {
                let validity = match col {
                    Column::Null { len } => Bitmap::new_constant(false, len),
                    Column::Nullable(box ref nullable_column) => nullable_column.validity.clone(),
                    _ => Bitmap::new_constant(true, col.len()),
                };
                let new_col = cast_scalars_to_variants(col.iter(), &ctx.func_ctx.tz);
                Value::Column(NullableColumn::new(new_col, validity))
            }
        },
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "to_boolean",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if RawJsonb::new(val).is_null().unwrap_or_default() {
                    output.push_null();
                } else {
                    match cast_to_bool(val) {
                        Ok(value) => output.push(value),
                        Err(err) => {
                            ctx.set_error(output.len(), err.to_string());
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match cast_to_bool(v) {
                Ok(res) => output.push(res),
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let raw_jsonb = RawJsonb::new(val);
                if let Ok(Some(s)) = raw_jsonb.as_str() {
                    output.push(&s);
                } else if raw_jsonb.is_null().unwrap_or_default() {
                    output.push_null();
                } else {
                    let json_str = raw_jsonb.to_string();
                    output.push(&json_str);
                }
            },
        ),
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
                let raw_jsonb = RawJsonb::new(val);
                if let Ok(Some(s)) = raw_jsonb.as_str() {
                    output.push(&s);
                } else if raw_jsonb.is_null().unwrap_or_default() {
                    output.push_null();
                } else {
                    let json_str = raw_jsonb.to_string();
                    output.push(&json_str);
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            let raw_jsonb = RawJsonb::new(val);
            if raw_jsonb.is_null().unwrap_or_default() {
                output.push_null();
                return;
            }
            if let Ok(Some(date)) = raw_jsonb.as_date() {
                output.push(date.value);
                return;
            }
            match raw_jsonb
                .as_str()
                .map_err(|e| format!("{e}"))
                .and_then(|r| r.ok_or(format!("invalid json type")))
                .and_then(|s| {
                    string_to_date(s.as_bytes(), &ctx.func_ctx.tz).map_err(|e| e.message())
                })
                .and_then(|d| {
                    d.since((Unit::Day, date(1970, 1, 1)))
                        .map_err(|e| format!("{}", e))
                }) {
                Ok(s) => output.push(s.get_days()),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("unable to cast to type `DATE` {}.", e),
                    );
                    output.push_null();
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
            let raw_jsonb = RawJsonb::new(val);
            if let Ok(Some(date)) = raw_jsonb.as_date() {
                output.push(date.value);
                return;
            }
            match raw_jsonb
                .as_str()
                .map_err(|e| format!("{e}"))
                .and_then(|r| r.ok_or(format!("invalid json type")))
                .and_then(|s| {
                    string_to_date(s.as_bytes(), &ctx.func_ctx.tz).map_err(|e| e.message())
                })
                .and_then(|d| {
                    d.since((Unit::Day, date(1970, 1, 1)))
                        .map_err(|e| format!("{}", e))
                }) {
                Ok(s) => output.push(s.get_days()),
                Err(_) => {
                    output.push_null();
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let raw_jsonb = RawJsonb::new(val);
                if raw_jsonb.is_null().unwrap_or_default() {
                    output.push_null();
                    return;
                }
                if let Ok(Some(ts)) = raw_jsonb.as_timestamp() {
                    output.push(ts.value);
                    return;
                }
                match raw_jsonb
                    .as_str()
                    .map_err(|e| format!("{e}"))
                    .and_then(|r| r.ok_or(format!("invalid json type")))
                    .and_then(|s| {
                        string_to_timestamp(s.as_bytes(), &ctx.func_ctx.tz).map_err(|e| e.message())
                    }) {
                    Ok(ts) => output.push(ts.timestamp().as_microsecond()),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("unable to cast to type `TIMESTAMP` {}.", e),
                        );
                        output.push_null();
                    }
                }
            },
        ),
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

                let raw_jsonb = RawJsonb::new(val);
                if let Ok(Some(ts)) = raw_jsonb.as_timestamp() {
                    output.push(ts.value);
                    return;
                }
                match raw_jsonb
                    .as_str()
                    .map_err(|e| format!("{e}"))
                    .and_then(|r| r.ok_or(format!("invalid json type")))
                    .and_then(|s| {
                        string_to_timestamp(s.as_bytes(), &ctx.func_ctx.tz).map_err(|e| e.message())
                    }) {
                    Ok(ts) => output.push(ts.timestamp().as_microsecond()),
                    Err(_) => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match dest_type {
            NumberDataType::NUM_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::MayThrow,
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
                            if RawJsonb::new(val).is_null().unwrap_or_default() {
                                output.push_null();
                                return;
                            }
                            type Native = <NUM_TYPE as Number>::Native;
                            let value: Option<Native> = if dest_type.is_float() {
                                cast_to_f64(val).ok().and_then(num_traits::cast::cast)
                            } else if dest_type.is_signed() {
                                cast_to_i64(val).ok().and_then(num_traits::cast::cast)
                            } else {
                                cast_to_u64(val).ok().and_then(num_traits::cast::cast)
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
                        }),
                    );

                let name = format!("try_to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            VariantType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(move |v, output, ctx| {
                            if let Some(validity) = &ctx.validity {
                                if !validity.get_bit(output.len()) {
                                    output.push_null();
                                    return;
                                }
                            }
                            if dest_type.is_float() {
                                if let Ok(value) = cast_to_f64(v) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else if dest_type.is_signed() {
                                if let Ok(value) = cast_to_i64(v) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else {
                                if let Ok(value) = cast_to_u64(v) {
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
        "json_pretty",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let s = RawJsonb::new(val).to_pretty_string();
            output.put_str(&s);
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
            match RawJsonb::new(val).strip_nulls() {
                Ok(owned_jsonb) => {
                    output.put_slice(owned_jsonb.as_ref());
                }
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            }
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg(
        "concat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, VariantType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match left_val.concat(&right_val) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, VariantType>(
            |val, index, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                match RawJsonb::new(val).delete_by_index(index) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, VariantType>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                match RawJsonb::new(val).delete_by_name(name) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<VariantType, Int32Type, VariantType, VariantType, _, _>(
        "json_array_insert",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<VariantType, Int32Type, VariantType, VariantType>(
            |val, pos, new_val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                let new_value = RawJsonb::new(new_val);
                match RawJsonb::new(val).array_insert(pos, &new_value) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, VariantType, _, _>(
        "json_array_distinct",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match RawJsonb::new(val).array_distinct() {
                Ok(owned_jsonb) => {
                    output.put_slice(owned_jsonb.as_ref());
                }
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                }
            }
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, VariantType, VariantType, _, _>(
        "json_array_intersection",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, VariantType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match left_val.array_intersection(&right_val) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, VariantType, VariantType, _, _>(
        "json_array_except",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, VariantType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match left_val.array_except(&right_val) {
                    Ok(owned_jsonb) => {
                        output.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "json_array_overlap",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, BooleanType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match left_val.array_overlap(&right_val) {
                    Ok(res) => {
                        output.push(res);
                    }
                    Err(err) => {
                        output.push(false);
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
            },
        ),
    );

    registry.register_function_factory("delete_by_keypath", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "delete_by_keypath".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(delete_by_keypath_fn),
            },
        }))
    });

    registry.register_passthrough_nullable_1_arg(
        "json_typeof",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match type_of(v) {
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

    registry.register_passthrough_nullable_2_arg(
        "json_contains_in_left",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, BooleanType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match left_val.contains(&right_val) {
                    Ok(res) => output.push(res),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_contains_in_right",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, BooleanType>(
            |left, right, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                let left_val = RawJsonb::new(left);
                let right_val = RawJsonb::new(right);
                match right_val.contains(&left_val) {
                    Ok(res) => output.push(res),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_any_keys",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, ArrayType<StringType>, BooleanType>(
            |v, keys, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                match RawJsonb::new(v).exists_any_keys(keys.iter()) {
                    Ok(res) => output.push(res),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_all_keys",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, ArrayType<StringType>, BooleanType>(
            |v, keys, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                match RawJsonb::new(v).exists_all_keys(keys.iter()) {
                    Ok(res) => output.push(res),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg(
        "json_exists_key",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, BooleanType>(
            |v, key, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(false);
                        return;
                    }
                }
                match RawJsonb::new(v).exists_all_keys(once(key)) {
                    Ok(res) => output.push(res),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            },
        ),
    );

    registry.register_function_factory("json_object_insert", |_, args_type| {
        if args_type.len() != 3 && args_type.len() != 4 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }
        if args_type.len() == 4
            && args_type[3].remove_nullable() != DataType::Boolean
            && args_type[3] != DataType::Null
        {
            return None;
        }
        let is_nullable = args_type[0].is_nullable_or_null();
        let return_type = if is_nullable {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_insert".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(move |args, ctx| json_object_insert_fn(args, ctx, is_nullable)),
            },
        }))
    });

    registry.register_function_factory("json_object_pick", |_, args_type| {
        if args_type.len() < 2 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        for arg_type in args_type.iter().skip(1) {
            if arg_type.remove_nullable() != DataType::String && *arg_type != DataType::Null {
                return None;
            }
        }
        let is_nullable = args_type[0].is_nullable_or_null();
        let return_type = if is_nullable {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_pick".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(move |args, ctx| {
                    json_object_pick_or_delete_fn(args, ctx, true, is_nullable)
                }),
            },
        }))
    });

    registry.register_function_factory("json_object_delete", |_, args_type| {
        if args_type.len() < 2 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        for arg_type in args_type.iter().skip(1) {
            if arg_type.remove_nullable() != DataType::String && *arg_type != DataType::Null {
                return None;
            }
        }
        let is_nullable = args_type[0].is_nullable_or_null();
        let return_type = if is_nullable {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_delete".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::MayThrow),
                eval: Box::new(move |args, ctx| {
                    json_object_pick_or_delete_fn(args, ctx, false, is_nullable)
                }),
            },
        }))
    });

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "strip_null_value",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(|v| {
                if matches!(RawJsonb::new(v).is_null(), Ok(true)) {
                    return None;
                }
                Some(v.to_vec())
            })
        }),
    );
}

fn json_array_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = BinaryColumnBuilder::with_capacity(cap, cap * 50);
    let mut items = Vec::with_capacity(columns.len());

    for idx in 0..cap {
        items.clear();
        for column in &columns {
            let v = unsafe { column.index_unchecked(idx) };
            let mut val = vec![];
            cast_scalar_to_variant(v, &ctx.func_ctx.tz, &mut val);
            items.push(val);
        }
        match OwnedJsonb::build_array(items.iter().map(|v| RawJsonb::new(v))) {
            Ok(owned_jsonb) => {
                builder.put_slice(owned_jsonb.as_ref());
            }
            Err(err) => {
                ctx.set_error(builder.len(), err.to_string());
            }
        }
        builder.commit_row();
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}

fn json_object_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, false)
}

fn json_object_keep_null_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    json_object_impl_fn(args, ctx, true)
}

fn json_object_impl_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    keep_null: bool,
) -> Value<AnyType> {
    let (columns, len) = prepare_args_columns(args, ctx);
    let cap = len.unwrap_or(1);
    let mut builder = BinaryColumnBuilder::with_capacity(cap, cap * 50);
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
                    ScalarRef::String(v) => v,
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
                set.insert(key);
                let mut val = vec![];
                cast_scalar_to_variant(v, &ctx.func_ctx.tz, &mut val);
                kvs.push((key, val));
            }
            if !has_err {
                match OwnedJsonb::build_object(kvs.iter().map(|(k, v)| (k, RawJsonb::new(&v[..]))))
                {
                    Ok(owned_jsonb) => {
                        builder.put_slice(owned_jsonb.as_ref());
                    }
                    Err(err) => {
                        ctx.set_error(builder.len(), err.to_string());
                    }
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
    args: &[Value<AnyType>],
    ctx: &EvalContext,
) -> (Vec<Column>, Option<usize>) {
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut columns = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let column = match arg {
            Value::Column(column) => column.clone(),
            Value::Scalar(s) => {
                let column_builder = ColumnBuilder::repeat(&s.as_ref(), len, &ctx.generics[i]);
                column_builder.build()
            }
        };
        columns.push(column);
    }
    (columns, len_opt)
}

fn delete_by_keypath_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let scalar_keypath = match &args[1] {
        Value::Scalar(Scalar::String(v)) => Some(parse_key_paths(v.as_bytes())),
        _ => None,
    };
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let keypath = match &args[1] {
            Value::Scalar(_) => Cow::Borrowed(&scalar_keypath),
            Value::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                let path = match scalar {
                    ScalarRef::String(buf) => Some(parse_key_paths(buf.as_bytes())),
                    _ => None,
                };
                Cow::Owned(path)
            }
        };
        match keypath.as_ref() {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        Value::Scalar(scalar) => scalar.as_ref(),
                        Value::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(v) => {
                            match RawJsonb::new(v).delete_by_keypath(path.paths.iter()) {
                                Ok(owned_jsonb) => {
                                    validity.push(true);
                                    builder.put_slice(owned_jsonb.as_ref());
                                }
                                Err(err) => {
                                    validity.push(false);
                                    ctx.set_error(builder.len(), err.to_string());
                                }
                            }
                        }
                        _ => validity.push(false),
                    }
                }
                Err(err) => {
                    validity.push(false);
                    ctx.set_error(builder.len(), err.to_string());
                }
            },
            None => validity.push(false),
        }
        builder.commit_row();
    }

    let validity: Bitmap = validity.into();

    match len_opt {
        Some(_) => Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Variant(builder.build_scalar()))
            }
        }
    }
}

fn get_by_keypath_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    string_res: bool,
) -> Value<AnyType> {
    let scalar_keypath = match &args[1] {
        Value::Scalar(Scalar::String(v)) => Some(parse_key_paths(v.as_bytes())),
        _ => None,
    };
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut builder = if string_res {
        ColumnBuilder::String(StringColumnBuilder::with_capacity(len))
    } else {
        ColumnBuilder::Variant(BinaryColumnBuilder::with_capacity(len, len * 50))
    };

    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let keypath = match &args[1] {
            Value::Scalar(_) => Cow::Borrowed(&scalar_keypath),
            Value::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                let path = match scalar {
                    ScalarRef::String(buf) => Some(parse_key_paths(buf.as_bytes())),
                    _ => None,
                };
                Cow::Owned(path)
            }
        };

        match keypath.as_ref() {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        Value::Scalar(scalar) => scalar.as_ref(),
                        Value::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(v) => {
                            match RawJsonb::new(v).get_by_keypath(path.paths.iter()) {
                                Ok(Some(res)) => {
                                    match &mut builder {
                                        ColumnBuilder::String(builder) => {
                                            let raw_jsonb = res.as_raw();
                                            if let Ok(Some(s)) = raw_jsonb.as_str() {
                                                builder.put_str(&s);
                                            } else {
                                                let json_str = raw_jsonb.to_string();
                                                builder.put_str(&json_str);
                                            }
                                        }
                                        ColumnBuilder::Variant(builder) => {
                                            builder.put_slice(res.as_ref());
                                        }
                                        _ => unreachable!(),
                                    }
                                    validity.push(true);
                                }
                                _ => validity.push(false),
                            }
                        }
                        _ => validity.push(false),
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    validity.push(false);
                }
            },
            None => validity.push(false),
        }

        match &mut builder {
            ColumnBuilder::String(builder) => {
                builder.commit_row();
            }
            ColumnBuilder::Variant(builder) => {
                builder.commit_row();
            }
            _ => unreachable!(),
        }
    }

    let builder = ColumnBuilder::Nullable(Box::new(NullableColumnBuilder { builder, validity }));

    match len_opt {
        Some(_) => Value::Column(builder.build()),
        None => Value::Scalar(builder.build_scalar()),
    }
}

fn path_predicate_fn<'a>(
    args: &'a [Value<AnyType>],
    ctx: &'a mut EvalContext,
    is_match: bool,
) -> Value<AnyType> {
    let scalar_jsonpath = match &args[1] {
        Value::Scalar(Scalar::String(v)) => {
            let res = parse_json_path(v.as_bytes()).map_err(|_| format!("Invalid JSON Path '{v}'"));
            Some(res)
        }
        _ => None,
    };

    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);

    let mut output = MutableBitmap::with_capacity(len);
    let mut validity = MutableBitmap::with_capacity(len);

    for idx in 0..len {
        let jsonpath = match &args[1] {
            Value::Scalar(_) => scalar_jsonpath.clone(),
            Value::Column(col) => {
                let scalar = unsafe { col.index_unchecked(idx) };
                match scalar {
                    ScalarRef::String(buf) => {
                        let res = parse_json_path(buf.as_bytes())
                            .map_err(|_| format!("Invalid JSON Path '{buf}'"));
                        Some(res)
                    }
                    _ => None,
                }
            }
        };
        match jsonpath {
            Some(result) => match result {
                Ok(path) => {
                    let json_row = match &args[0] {
                        Value::Scalar(scalar) => scalar.as_ref(),
                        Value::Column(col) => unsafe { col.index_unchecked(idx) },
                    };
                    match json_row {
                        ScalarRef::Variant(v) => {
                            let jsonb = RawJsonb::new(v);
                            if is_match {
                                let res = jsonb.path_match(&path);
                                match res {
                                    Ok(Some(r)) => {
                                        output.push(r);
                                        validity.push(true);
                                    }
                                    Ok(None) => {
                                        output.push(false);
                                        validity.push(false);
                                    }
                                    Err(err) => {
                                        ctx.set_error(output.len(), err.to_string());
                                        output.push(false);
                                        validity.push(false);
                                    }
                                }
                            } else {
                                let res = jsonb.path_exists(&path);
                                match res {
                                    Ok(r) => {
                                        output.push(r);
                                        validity.push(true);
                                    }
                                    Err(err) => {
                                        ctx.set_error(output.len(), err.to_string());
                                        output.push(false);
                                        validity.push(false);
                                    }
                                }
                            }
                        }
                        _ => {
                            output.push(false);
                            validity.push(false);
                        }
                    }
                }
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(false);
                    validity.push(false);
                }
            },
            None => {
                output.push(false);
                validity.push(false);
            }
        }
    }

    let validity: Bitmap = validity.into();

    match len_opt {
        Some(_) => Value::Column(Column::Boolean(output.into())).wrap_nullable(Some(validity)),
        None => {
            if !validity.get_bit(0) {
                Value::Scalar(Scalar::Null)
            } else {
                Value::Scalar(Scalar::Boolean(output.get(0)))
            }
        }
    }
}

fn json_object_insert_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    is_nullable: bool,
) -> Value<AnyType> {
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut validity = MutableBitmap::with_capacity(len);
    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    for idx in 0..len {
        let value = match &args[0] {
            Value::Scalar(scalar) => scalar.as_ref(),
            Value::Column(col) => unsafe { col.index_unchecked(idx) },
        };
        if value == ScalarRef::Null {
            builder.commit_row();
            validity.push(false);
            continue;
        }
        let v = value.as_variant().unwrap();
        let value = RawJsonb::new(v);
        if !value.is_object().unwrap_or_default() {
            ctx.set_error(builder.len(), "Invalid json object");
            builder.commit_row();
            validity.push(false);
            continue;
        }
        let new_key = match &args[1] {
            Value::Scalar(scalar) => scalar.as_ref(),
            Value::Column(col) => unsafe { col.index_unchecked(idx) },
        };
        let new_val = match &args[2] {
            Value::Scalar(scalar) => scalar.as_ref(),
            Value::Column(col) => unsafe { col.index_unchecked(idx) },
        };
        if new_key == ScalarRef::Null || new_val == ScalarRef::Null {
            builder.put(value.as_ref());
            builder.commit_row();
            validity.push(true);
            continue;
        }
        let update_flag = if args.len() == 4 {
            let v = match &args[3] {
                Value::Scalar(scalar) => scalar.as_ref(),
                Value::Column(col) => unsafe { col.index_unchecked(idx) },
            };
            match v {
                ScalarRef::Boolean(v) => v,
                _ => false,
            }
        } else {
            false
        };
        let new_key = new_key.as_string().unwrap();
        let res = match new_val {
            ScalarRef::Variant(new_val) => {
                let new_val = RawJsonb::new(new_val);
                value.object_insert(new_key, &new_val, update_flag)
            }
            _ => {
                // if the new value is not a json value, cast it to json.
                let mut new_val_buf = vec![];
                cast_scalar_to_variant(new_val.clone(), &ctx.func_ctx.tz, &mut new_val_buf);
                let new_val = RawJsonb::new(new_val_buf.as_bytes());
                value.object_insert(new_key, &new_val, update_flag)
            }
        };
        match res {
            Ok(owned_jsonb) => {
                validity.push(true);
                builder.put_slice(owned_jsonb.as_ref());
            }
            Err(err) => {
                validity.push(false);
                ctx.set_error(builder.len(), err.to_string());
            }
        }
        builder.commit_row();
    }
    if is_nullable {
        let validity: Bitmap = validity.into();
        match len_opt {
            Some(_) => {
                Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity))
            }
            None => {
                if !validity.get_bit(0) {
                    Value::Scalar(Scalar::Null)
                } else {
                    Value::Scalar(Scalar::Variant(builder.build_scalar()))
                }
            }
        }
    } else {
        match len_opt {
            Some(_) => Value::Column(Column::Variant(builder.build())),
            None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
        }
    }
}

fn json_object_pick_or_delete_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    is_pick: bool,
    is_nullable: bool,
) -> Value<AnyType> {
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut keys = BTreeSet::new();
    let mut validity = MutableBitmap::with_capacity(len);
    let mut builder = BinaryColumnBuilder::with_capacity(len, len * 50);
    for idx in 0..len {
        let value = match &args[0] {
            Value::Scalar(scalar) => scalar.as_ref(),
            Value::Column(col) => unsafe { col.index_unchecked(idx) },
        };
        if value == ScalarRef::Null {
            builder.commit_row();
            validity.push(false);
            continue;
        }
        let v = value.as_variant().unwrap();
        let value = RawJsonb::new(v);
        if !value.is_object().unwrap_or_default() {
            ctx.set_error(builder.len(), "Invalid json object");
            builder.commit_row();
            validity.push(false);
            continue;
        }
        keys.clear();
        for arg in args.iter().skip(1) {
            let key = match &arg {
                Value::Scalar(scalar) => scalar.as_ref(),
                Value::Column(col) => unsafe { col.index_unchecked(idx) },
            };
            if key == ScalarRef::Null {
                continue;
            }
            let key = key.as_string().unwrap();
            keys.insert(*key);
        }
        let res = if is_pick {
            value.object_pick(&keys)
        } else {
            value.object_delete(&keys)
        };
        match res {
            Ok(owned_jsonb) => {
                validity.push(true);
                builder.put_slice(owned_jsonb.as_ref());
            }
            Err(err) => {
                validity.push(false);
                ctx.set_error(builder.len(), err.to_string());
            }
        }
        builder.commit_row();
    }
    if is_nullable {
        let validity: Bitmap = validity.into();
        match len_opt {
            Some(_) => {
                Value::Column(Column::Variant(builder.build())).wrap_nullable(Some(validity))
            }
            None => {
                if !validity.get_bit(0) {
                    Value::Scalar(Scalar::Null)
                } else {
                    Value::Scalar(Scalar::Variant(builder.build_scalar()))
                }
            }
        }
    } else {
        match len_opt {
            Some(_) => Value::Column(Column::Variant(builder.build())),
            None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
        }
    }
}

fn cast_to_bool(v: &[u8]) -> Result<bool, jsonb::Error> {
    match RawJsonb::new(v).to_bool() {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                let owned_jsonb = s.parse::<OwnedJsonb>()?;
                let raw_jsonb = owned_jsonb.as_raw();
                return raw_jsonb.to_bool();
            }
            Err(err)
        }
    }
}

fn cast_to_i64(v: &[u8]) -> Result<i64, jsonb::Error> {
    match RawJsonb::new(v).to_i64() {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                let owned_jsonb = s.parse::<OwnedJsonb>()?;
                let raw_jsonb = owned_jsonb.as_raw();
                return raw_jsonb.to_i64();
            }
            Err(err)
        }
    }
}

fn cast_to_u64(v: &[u8]) -> Result<u64, jsonb::Error> {
    match RawJsonb::new(v).to_u64() {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                let owned_jsonb = s.parse::<OwnedJsonb>()?;
                let raw_jsonb = owned_jsonb.as_raw();
                return raw_jsonb.to_u64();
            }
            Err(err)
        }
    }
}

fn cast_to_f64(v: &[u8]) -> Result<f64, jsonb::Error> {
    match RawJsonb::new(v).to_f64() {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                let owned_jsonb = s.parse::<OwnedJsonb>()?;
                let raw_jsonb = owned_jsonb.as_raw();
                return raw_jsonb.to_f64();
            }
            Err(err)
        }
    }
}

fn type_of(v: &[u8]) -> Result<&'static str, jsonb::Error> {
    match RawJsonb::new(v).type_of() {
        Ok(val) => Ok(val),
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                let owned_jsonb = s.parse::<OwnedJsonb>()?;
                let raw_jsonb = owned_jsonb.as_raw();
                return raw_jsonb.type_of();
            }
            Err(err)
        }
    }
}
