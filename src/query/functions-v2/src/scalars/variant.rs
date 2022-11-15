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
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::Float64Type;
use common_expression::types::number::Int64Type;
use common_expression::types::number::UInt32Type;
use common_expression::types::number::UInt64Type;
use common_expression::types::variant::cast_scalar_to_variant;
use common_expression::types::variant::cast_scalars_to_variants;
use common_expression::types::variant::JSONB_NULL;
use common_expression::types::BooleanType;
use common_expression::types::GenericType;
use common_expression::types::NullableType;
use common_expression::types::StringType;
use common_expression::types::VariantType;
use common_expression::utils::arrow::constant_bitmap;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
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
use common_jsonb::JsonPath;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        FunctionProperty::default(),
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, _| {
            if s.trim().is_empty() {
                output.put_slice(JSONB_NULL);
                output.commit_row();
                return Ok(());
            }
            let value = parse_value(s).map_err(|err| {
                format!("unable to parse '{}': {}", &String::from_utf8_lossy(s), err)
            })?;
            value.to_vec(&mut output.data);
            output.commit_row();

            Ok(())
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
                        value.to_vec(&mut output.builder.data);
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            }
            Ok(())
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
            Ok(())
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, UInt32Type, _, _>(
        "length",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<UInt32Type>>(|v, output, _| {
            if v.is_empty() {
                output.push_null();
            } else {
                match array_length(v) {
                    Some(len) => output.push(len as u32),
                    None => output.push_null(),
                }
            }
            Ok(())
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "object_keys",
        FunctionProperty::default(),
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, _| {
            if v.is_empty() {
                output.push_null()
            } else {
                match object_keys(v) {
                    Some(keys) => output.push(&keys),
                    None => output.push_null(),
                }
            }
            Ok(())
        }),
    );

    registry.register_combine_nullable_2_arg::<VariantType, UInt64Type, VariantType, _, _>(
        "get",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, UInt64Type, NullableType<VariantType>>(
            |s, idx, output, _| {
                if s.is_empty() {
                    output.push_null()
                } else {
                    let json_path = JsonPath::UInt64(idx);
                    match get_by_path(s, vec![json_path]) {
                        Some(val) => output.push(val.as_slice()),
                        None => output.push_null(),
                    }
                }
                Ok(())
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |s, name, output, _| {
                if s.is_empty() || name.trim().is_empty() {
                    output.push_null()
                } else {
                    let name = String::from_utf8(name.to_vec()).map_err(|err| {
                        format!(
                            "Unable convert name '{}' to string: {}",
                            &String::from_utf8_lossy(name),
                            err
                        )
                    })?;
                    let json_path = JsonPath::String(Cow::Borrowed(&name));
                    match get_by_path(s, vec![json_path]) {
                        Some(val) => output.push(val.as_slice()),
                        None => output.push_null(),
                    }
                }
                Ok(())
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_ignore_case",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |s, name, output, _| {
                if s.is_empty() || name.trim().is_empty() {
                    output.push_null()
                } else {
                    let name = String::from_utf8(name.to_vec()).map_err(|err| {
                        format!(
                            "Unable convert name '{}' to string: {}",
                            &String::from_utf8_lossy(name),
                            err
                        )
                    })?;
                    match get_by_name_ignore_case(s, &name) {
                        Some(val) => output.push(val.as_slice()),
                        None => output.push_null(),
                    }
                }
                Ok(())
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_path",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |s, path, output, _| {
                if s.is_empty() || path.is_empty() {
                    output.push_null()
                } else {
                    let json_paths = parse_json_path(path).map_err(|err| {
                        format!(
                            "Invalid extraction path '{}': {}",
                            &String::from_utf8_lossy(path),
                            err
                        )
                    })?;
                    match get_by_path(s, json_paths) {
                        Some(val) => output.push(val.as_slice()),
                        None => output.push_null(),
                    }
                }
                Ok(())
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "json_extract_path_text",
        FunctionProperty::default(),
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<StringType>>(
            |s, path, output, _| {
                if s.is_empty() || path.is_empty() {
                    output.push_null();
                } else {
                    let value = common_jsonb::parse_value(s).map_err(|err| {
                        format!("unable to parse '{}': {}", &String::from_utf8_lossy(s), err)
                    })?;

                    let json_paths = parse_json_path(path).map_err(|err| {
                        format!(
                            "Invalid extraction path '{}': {}",
                            &String::from_utf8_lossy(path),
                            err
                        )
                    })?;
                    match value.get_by_path(&json_paths) {
                        Some(val) => {
                            let json_val = format!("{val}");
                            output.push(json_val.as_bytes());
                        }
                        None => output.push_null(),
                    }
                }
                Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
                Ok(Value::Scalar(buf))
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.tz);
                Ok(Value::Column(new_col))
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
                Ok(Value::Scalar(Some(buf)))
            }
            ValueRef::Column(col) => {
                let new_col = cast_scalars_to_variants(col.iter(), ctx.tz);
                Ok(Value::Column(NullableColumn {
                    validity: constant_bitmap(true, new_col.len()).into(),
                    column: new_col,
                }))
            }
        },
    );
}
