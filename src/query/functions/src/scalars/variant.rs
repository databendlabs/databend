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
use std::collections::HashSet;
use std::sync::Arc;

use bstr::ByteSlice;
use chrono::Datelike;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_expression::types::date::string_to_date;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::*;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::timestamp::string_to_timestamp;
use common_expression::types::variant::cast_scalar_to_variant;
use common_expression::types::variant::cast_scalars_to_variants;
use common_expression::types::variant::JSONB_NULL;
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
use common_expression::utils::arrow::constant_bitmap;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_2_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
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
use jsonb::build_object;
use jsonb::get_by_name_ignore_case;
use jsonb::get_by_path;
use jsonb::is_array;
use jsonb::is_object;
use jsonb::object_keys;
use jsonb::parse_json_path;
use jsonb::parse_value;
use jsonb::to_bool;
use jsonb::to_f64;
use jsonb::to_i64;
use jsonb::to_str;
use jsonb::to_u64;
use jsonb::JsonPathRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("json_object_keys", &["object_keys"]);

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
        |_| FunctionDomain::Full,
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
        "json_object_keys",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(object_keys)
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
                    let value = jsonb::parse_value(s);
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
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|val, output, ctx| {
            if val.is_empty() {
                output.push(false);
            } else {
                match to_bool(val) {
                    Ok(value) => output.push(value),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(false);
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|val, output, _| {
            if val.is_empty() {
                output.push_null();
            } else {
                match to_bool(val) {
                    Ok(value) => {
                        output.validity.push(true);
                        output.builder.push(value);
                    }
                    Err(_) => output.push_null(),
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if val.is_empty() {
                output.commit_row();
            } else {
                match to_str(val) {
                    Ok(value) => output.put_slice(value.as_bytes()),
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
                output.commit_row();
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "try_to_string",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|val, output, _| {
            if val.is_empty() {
                output.push_null();
            } else {
                match to_str(val) {
                    Ok(value) => {
                        output.validity.push(true);
                        output.builder.put_slice(value.as_bytes());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, DateType>(|val, output, ctx| {
            if val.is_empty() {
                output.push(0);
            } else {
                match as_str(val).and_then(|val| string_to_date(val.as_bytes(), ctx.tz.tz)) {
                    Some(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    None => {
                        ctx.set_error(output.len(), "unable to cast to type `DATE`");
                        output.push(0);
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "try_to_date",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|val, output, ctx| {
            if val.is_empty() {
                output.push_null();
            } else {
                match as_str(val)
                    .and_then(|str_value| string_to_date(str_value.as_bytes(), ctx.tz.tz))
                {
                    Some(date) => output.push(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    None => output.push_null(),
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, TimestampType>(|val, output, ctx| {
            if val.is_empty() {
                output.push(0);
            } else {
                match as_str(val).and_then(|val| string_to_timestamp(val.as_bytes(), ctx.tz.tz)) {
                    Some(ts) => output.push(ts.timestamp_micros()),
                    None => {
                        ctx.set_error(output.len(), "unable to cast to type `TIMESTAMP`");
                        output.push(0);
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "try_to_timestamp",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |val, output, ctx| {
                if val.is_empty() {
                    output.push_null();
                } else {
                    match as_str(val) {
                        Some(str_val) => {
                            let timestamp = string_to_timestamp(str_val.as_bytes(), ctx.tz.tz)
                                .map(|ts| ts.timestamp_micros());
                            match timestamp {
                                Some(timestamp) => output.push(timestamp),
                                None => output.push_null(),
                            }
                        }
                        None => output.push_null(),
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
                    .register_passthrough_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        FunctionProperty::default(),
                        |_| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<VariantType, NumberType<NUM_TYPE>>(
                            move |val, output, ctx| {
                                if val.is_empty() {
                                    output.push(NUM_TYPE::default());
                                } else {
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
                            if val.is_empty() {
                                output.push_null();
                            } else if dest_type.is_float() {
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

    registry.register_function_factory("json_object", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| FunctionDomain::MayThrow),
            eval: Box::new(move |args, ctx| json_object_fn(args, ctx, false)),
        }))
    });

    registry.register_function_factory("json_object_keep_null", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_object_keep_null".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Variant,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| FunctionDomain::MayThrow),
            eval: Box::new(move |args, ctx| json_object_fn(args, ctx, true)),
        }))
    });
}

fn json_object_fn(
    args: &[ValueRef<AnyType>],
    ctx: &mut EvalContext,
    keep_null: bool,
) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let mut columns = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let column = match arg {
            ValueRef::Column(column) => column.clone(),
            ValueRef::Scalar(s) => {
                let column_builder = ColumnBuilder::repeat(s, 1, &ctx.generics[i]);
                column_builder.build()
            }
        };
        columns.push(column);
    }

    let cap = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(cap, cap * 50);
    if columns.len() % 2 != 0 {
        ctx.set_error(0, "The number of keys and values must be equal");
        for _ in 0..(len.unwrap_or(1)) {
            builder.commit_row();
        }
    } else {
        let mut set = HashSet::new();
        for idx in 0..(len.unwrap_or(1)) {
            set.clear();
            let mut kvs = Vec::with_capacity(columns.len() / 2);
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
                        ctx.set_error(builder.len(), "Key must be a string value");
                        break;
                    }
                };
                if set.contains(&key) {
                    ctx.set_error(builder.len(), "Keys have to be unique");
                    break;
                }
                set.insert(key.clone());
                let mut val = vec![];
                cast_scalar_to_variant(v, ctx.tz, &mut val);
                kvs.push((key, val));
            }
            if let Err(err) = build_object(kvs.iter().map(|(k, v)| (k, &v[..])), &mut builder.data)
            {
                ctx.set_error(builder.len(), err.to_string());
            }
            builder.commit_row();
        }
    }
    match len {
        Some(_) => Value::Column(Column::Variant(builder.build())),
        None => Value::Scalar(Scalar::Variant(builder.build_scalar())),
    }
}
