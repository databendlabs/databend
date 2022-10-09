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
use std::sync::Arc;

use bstr::ByteSlice;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Float64Type;
use common_expression::types::number::Int64Type;
use common_expression::types::number::Number;
use common_expression::types::number::NumberColumnBuilder;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::UInt32Type;
use common_expression::types::number::UInt64Type;
use common_expression::types::number::F64;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::variant::JSONB_NULL;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::VariantType;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::wrap_nullable;
use common_expression::Column;
use common_expression::Function;
use common_expression::FunctionContext;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
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
        |_| None,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, _| {
            if s.trim().is_empty() {
                output.put_slice(JSONB_NULL);
                output.commit_row();
                return Ok(());
            }
            let value = common_jsonb::parse_value(s).map_err(|err| {
                format!("unable to parse '{}': {}", &String::from_utf8_lossy(s), err)
            })?;
            value.to_vec(&mut output.data);
            output.commit_row();

            Ok(())
        }),
    );

    registry.register_1_arg_core::<StringType, NullableType<VariantType>, _, _>(
        "try_parse_json",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    if v.trim().is_empty() {
                        bitmap.push(true);
                        builder.put_slice(JSONB_NULL);
                        builder.commit_row();
                        continue;
                    }
                    match parse_value(v) {
                        Ok(value) => {
                            value.to_vec(&mut builder.data);
                            bitmap.push(true);
                        }
                        Err(_) => bitmap.push(false),
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(s) => {
                let val = if s.trim().is_empty() {
                    Value::Scalar(Some(JSONB_NULL.to_vec()))
                } else {
                    match parse_value(s) {
                        Ok(val) => {
                            let mut buf = Vec::new();
                            val.to_vec(&mut buf);
                            Value::Scalar(Some(buf))
                        }
                        Err(_) => Value::Scalar(None),
                    }
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<StringType>, NullableType<VariantType>, _, _>(
        "try_parse_json",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    if v.trim().is_empty() {
                        bitmap.push(true);
                        builder.put_slice(JSONB_NULL);
                        builder.commit_row();
                        continue;
                    }
                    match parse_value(v) {
                        Ok(value) => {
                            value.to_vec(&mut builder.data);
                            bitmap.push(true);
                        }
                        Err(_) => {
                            bitmap.push(false);
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(s)) => {
                let val = if s.trim().is_empty() {
                    Value::Scalar(Some(JSONB_NULL.to_vec()))
                } else {
                    match parse_value(s) {
                        Ok(val) => {
                            let mut buf = Vec::new();
                            val.to_vec(&mut buf);
                            Value::Scalar(Some(buf))
                        }
                        Err(_) => Value::Scalar(None),
                    }
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<StringType, NullableType<StringType>, _, _>(
        "check_json",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    if v.trim().is_empty() {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    match parse_value(v) {
                        Ok(_) => bitmap.push(false),
                        Err(e) => {
                            bitmap.push(true);
                            builder.put_slice(e.to_string().as_bytes());
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(s) => {
                let val = if s.trim().is_empty() {
                    Value::Scalar(None)
                } else {
                    match parse_value(s) {
                        Ok(_) => Value::Scalar(None),
                        Err(e) => Value::Scalar(Some(e.to_string().as_bytes().to_vec())),
                    }
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<StringType>, NullableType<StringType>, _, _>(
        "check_json",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    if v.trim().is_empty() {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    match parse_value(v) {
                        Ok(_) => bitmap.push(false),
                        Err(e) => {
                            bitmap.push(true);
                            builder.put_slice(e.to_string().as_bytes());
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(s)) => {
                let val = if s.trim().is_empty() {
                    Value::Scalar(None)
                } else {
                    match parse_value(s) {
                        Ok(_) => Value::Scalar(None),
                        Err(e) => Value::Scalar(Some(e.to_string().as_bytes().to_vec())),
                    }
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<UInt32Type>, _, _>(
        "length",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::UInt32, size);
                for v in column.iter() {
                    match array_length(v) {
                        Some(len) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::UInt32(len as u32));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: u32::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match array_length(v) {
                    Some(len) => Value::Scalar(Some(len as u32)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::UInt32, size);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.push_default();
                        continue;
                    }
                    match array_length(v) {
                        Some(len) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::UInt32(len as u32));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: u32::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match array_length(v) {
                    Some(len) => Value::Scalar(Some(len as u32)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<VariantType>, _, _>(
        "object_keys",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    match object_keys(v) {
                        Some(keys) => {
                            bitmap.push(true);
                            builder.put_slice(&keys);
                        }
                        None => {
                            bitmap.push(false);
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match object_keys(v) {
                    Some(keys) => Value::Scalar(Some(keys)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "object_keys",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    match object_keys(v) {
                        Some(keys) => {
                            bitmap.push(true);
                            builder.put_slice(&keys);
                        }
                        None => {
                            bitmap.push(false);
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match object_keys(v) {
                    Some(keys) => Value::Scalar(Some(keys)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_function_factory("get", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get",
                args_type: vec![DataType::Variant, DataType::Number(NumberDataType::UInt64)],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(get_by_index_fn),
        }))
    });

    registry.register_function_factory("get", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Variant)),
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                ],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(wrap_nullable(get_by_index_fn)),
        }))
    });

    registry.register_function_factory("get", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get",
                args_type: vec![DataType::Variant, DataType::String],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(get_by_name_fn),
        }))
    });

    registry.register_function_factory("get", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Variant)),
                    DataType::Nullable(Box::new(DataType::String)),
                ],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(wrap_nullable(get_by_name_fn)),
        }))
    });

    registry.register_function_factory("get_ignore_case", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_ignore_case",
                args_type: vec![DataType::Variant, DataType::String],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(get_ignore_case_fn),
        }))
    });

    registry.register_function_factory("get_ignore_case", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_ignore_case",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Variant)),
                    DataType::Nullable(Box::new(DataType::String)),
                ],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(wrap_nullable(get_ignore_case_fn)),
        }))
    });

    registry.register_function_factory("get_path", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_path",
                args_type: vec![DataType::Variant, DataType::String],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(get_path_fn),
        }))
    });

    registry.register_function_factory("get_path", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_path",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Variant)),
                    DataType::Nullable(Box::new(DataType::String)),
                ],
                return_type: DataType::Nullable(Box::new(DataType::Variant)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(wrap_nullable(get_path_fn)),
        }))
    });

    registry.register_function_factory("json_extract_path_text", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_extract_path_text",
                args_type: vec![DataType::String, DataType::String],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(json_extract_path_text_fn),
        }))
    });

    registry.register_function_factory("json_extract_path_text", |_, _| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_extract_path_text",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::String)),
                    DataType::Nullable(Box::new(DataType::String)),
                ],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_| None),
            eval: Box::new(wrap_nullable(json_extract_path_text_fn)),
        }))
    });

    registry.register_1_arg_core::<VariantType, NullableType<BooleanType>, _, _>(
        "as_boolean",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = MutableBitmap::with_capacity(size);
                for v in column.iter() {
                    match as_bool(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(val);
                        }
                        None => {
                            bitmap.push(false);
                            builder.push(false);
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.into(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match as_bool(v) {
                    Some(val) => Value::Scalar(Some(val)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<BooleanType>, _, _>(
        "as_boolean",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = MutableBitmap::with_capacity(size);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.push(false);
                        continue;
                    }
                    match as_bool(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(val);
                        }
                        None => {
                            bitmap.push(false);
                            builder.push(false);
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.into(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match as_bool(v) {
                    Some(val) => Value::Scalar(Some(val)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<Int64Type>, _, _>(
        "as_integer",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::Int64, size);
                for v in column.iter() {
                    match as_i64(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::Int64(val));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: i64::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match as_i64(v) {
                    Some(val) => Value::Scalar(Some(val)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<Int64Type>, _, _>(
        "as_integer",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::Int64, size);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.push_default();
                        continue;
                    }
                    match as_i64(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::Int64(val));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: i64::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match as_i64(v) {
                    Some(val) => Value::Scalar(Some(val)),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<Float64Type>, _, _>(
        "as_float",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder =
                    NumberColumnBuilder::with_capacity(&NumberDataType::Float64, size);
                for v in column.iter() {
                    match as_f64(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::Float64(val.into()));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: F64::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match as_f64(v) {
                    Some(val) => Value::Scalar(Some(val.into())),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<Float64Type>, _, _>(
        "as_float",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder =
                    NumberColumnBuilder::with_capacity(&NumberDataType::Float64, size);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.push_default();
                        continue;
                    }
                    match as_f64(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.push(NumberScalar::Float64(val.into()));
                        }
                        None => {
                            bitmap.push(false);
                            builder.push_default();
                        }
                    }
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: F64::try_downcast_column(&builder.build()).unwrap(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match as_f64(v) {
                    Some(val) => Value::Scalar(Some(val.into())),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<StringType>, _, _>(
        "as_string",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    match as_str(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.put_slice(val.as_bytes());
                        }
                        None => {
                            bitmap.push(false);
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = match as_str(v) {
                    Some(val) => Value::Scalar(Some(val.as_bytes().to_vec())),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<StringType>, _, _>(
        "as_string",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    match as_str(v) {
                        Some(val) => {
                            bitmap.push(true);
                            builder.put_slice(val.as_bytes());
                        }
                        None => {
                            bitmap.push(false);
                        }
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = match as_str(v) {
                    Some(val) => Value::Scalar(Some(val.as_bytes().to_vec())),
                    None => Value::Scalar(None),
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<VariantType>, _, _>(
        "as_array",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    if is_array(v) {
                        bitmap.push(true);
                        builder.put_slice(v.as_bytes());
                    } else {
                        bitmap.push(false);
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = if is_array(v) {
                    Value::Scalar(Some(v.as_bytes().to_vec()))
                } else {
                    Value::Scalar(None)
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "as_array",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    if is_array(v) {
                        bitmap.push(true);
                        builder.put_slice(v.as_bytes());
                    } else {
                        bitmap.push(false);
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = if is_array(v) {
                    Value::Scalar(Some(v.as_bytes().to_vec()))
                } else {
                    Value::Scalar(None)
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<VariantType, NullableType<VariantType>, _, _>(
        "as_object",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(column) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for v in column.iter() {
                    if is_object(v) {
                        bitmap.push(true);
                        builder.put_slice(v.as_bytes());
                    } else {
                        bitmap.push(false);
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(v) => {
                let val = if is_object(v) {
                    Value::Scalar(Some(v.as_bytes().to_vec()))
                } else {
                    Value::Scalar(None)
                };
                Ok(val)
            }
        },
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "as_object",
        FunctionProperty::default(),
        |_| None,
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, column }) => {
                let size = column.len();
                let mut bitmap = MutableBitmap::with_capacity(size);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);
                for (valid, v) in validity.iter().zip(column.iter()) {
                    if !valid {
                        bitmap.push(false);
                        builder.commit_row();
                        continue;
                    }
                    if is_object(v) {
                        bitmap.push(true);
                        builder.put_slice(v.as_bytes());
                    } else {
                        bitmap.push(false);
                    }
                    builder.commit_row();
                }
                let col = NullableColumn {
                    validity: bitmap.into(),
                    column: builder.build(),
                };
                Ok(Value::Column(col))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(None)),
            ValueRef::Scalar(Some(v)) => {
                let val = if is_object(v) {
                    Value::Scalar(Some(v.as_bytes().to_vec()))
                } else {
                    Value::Scalar(None)
                };
                Ok(val)
            }
        },
    );
}

fn get_by_index_fn(
    args: &[ValueRef<AnyType>],
    _: FunctionContext,
) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<VariantType>().unwrap();
    let index_arg = args[1].try_downcast::<UInt64Type>().unwrap();
    let size = len.unwrap_or(1);
    let mut bitmap = MutableBitmap::with_capacity(size);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        let s = unsafe { source_arg.index_unchecked(idx) };
        if s.is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
        let index = unsafe { index_arg.index_unchecked(idx) };
        let json_path = JsonPath::UInt64(index);
        match get_by_path(s, vec![json_path]) {
            Some(val) => {
                builder.put_slice(val.as_slice());
                bitmap.push(true);
            }
            None => bitmap.push(false),
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: bitmap.into(),
                column: Column::Variant(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => match bitmap.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Ok(Value::Scalar(Scalar::Variant(builder.build_scalar())))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            None => Ok(Value::Scalar(Scalar::Null)),
        },
    }
}

fn get_by_name_fn(
    args: &[ValueRef<AnyType>],
    _: FunctionContext,
) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<VariantType>().unwrap();
    let name_arg = args[1].try_downcast::<StringType>().unwrap();
    let size = len.unwrap_or(1);
    let mut bitmap = MutableBitmap::with_capacity(size);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        let s = unsafe { source_arg.index_unchecked(idx) };
        let name = unsafe { name_arg.index_unchecked(idx) };
        if s.is_empty() || name.is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
        let name = String::from_utf8(name.to_vec()).map_err(|err| {
            format!(
                "Unable convert name '{}' to string: {}",
                &String::from_utf8_lossy(name),
                err
            )
        })?;

        let json_path = JsonPath::String(Cow::Borrowed(&name));
        match get_by_path(s, vec![json_path]) {
            Some(val) => {
                builder.put_slice(val.as_slice());
                bitmap.push(true);
            }
            None => bitmap.push(false),
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: bitmap.into(),
                column: Column::Variant(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => match bitmap.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Ok(Value::Scalar(Scalar::Variant(builder.build_scalar())))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            None => Ok(Value::Scalar(Scalar::Null)),
        },
    }
}

fn get_ignore_case_fn(
    args: &[ValueRef<AnyType>],
    _: FunctionContext,
) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<VariantType>().unwrap();
    let name_arg = args[1].try_downcast::<StringType>().unwrap();
    let size = len.unwrap_or(1);
    let mut bitmap = MutableBitmap::with_capacity(size);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        let s = unsafe { source_arg.index_unchecked(idx) };
        let name = unsafe { name_arg.index_unchecked(idx) };
        if s.is_empty() || name.is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
        let name = String::from_utf8(name.to_vec()).map_err(|err| {
            format!(
                "Unable convert name '{}' to string: {}",
                &String::from_utf8_lossy(name),
                err
            )
        })?;

        match get_by_name_ignore_case(s, &name) {
            Some(val) => {
                builder.put_slice(val.as_slice());
                bitmap.push(true);
            }
            None => bitmap.push(false),
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: bitmap.into(),
                column: Column::Variant(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => match bitmap.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Ok(Value::Scalar(Scalar::Variant(builder.build_scalar())))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            None => Ok(Value::Scalar(Scalar::Null)),
        },
    }
}

fn get_path_fn(args: &[ValueRef<AnyType>], _: FunctionContext) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<VariantType>().unwrap();
    let path_arg = args[1].try_downcast::<StringType>().unwrap();
    let size = len.unwrap_or(1);
    let mut bitmap = MutableBitmap::with_capacity(size);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        let s = unsafe { source_arg.index_unchecked(idx) };
        let path = unsafe { path_arg.index_unchecked(idx) };
        if s.is_empty() || path.is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
        let json_paths = parse_json_path(path).map_err(|err| {
            format!(
                "Invalid extraction path '{}': {}",
                &String::from_utf8_lossy(path),
                err
            )
        })?;
        match get_by_path(s, json_paths) {
            Some(val) => {
                builder.put_slice(val.as_slice());
                bitmap.push(true);
            }
            None => bitmap.push(false),
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: bitmap.into(),
                column: Column::Variant(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => match bitmap.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Ok(Value::Scalar(Scalar::Variant(builder.build_scalar())))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            None => Ok(Value::Scalar(Scalar::Null)),
        },
    }
}

fn json_extract_path_text_fn(
    args: &[ValueRef<AnyType>],
    _: FunctionContext,
) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let path_arg = args[1].try_downcast::<StringType>().unwrap();
    let size = len.unwrap_or(1);
    let mut bitmap = MutableBitmap::with_capacity(size);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        let s = unsafe { source_arg.index_unchecked(idx) };
        if s.trim().is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
        let value = common_jsonb::parse_value(s)
            .map_err(|err| format!("unable to parse '{}': {}", &String::from_utf8_lossy(s), err))?;
        let path = unsafe { path_arg.index_unchecked(idx) };
        if path.is_empty() {
            bitmap.push(false);
            builder.commit_row();
            continue;
        }
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
                builder.put_str(&json_val);
                bitmap.push(true);
            }
            None => bitmap.push(false),
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: bitmap.into(),
                column: Column::Variant(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => match bitmap.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Ok(Value::Scalar(Scalar::Variant(builder.build_scalar())))
                } else {
                    Ok(Value::Scalar(Scalar::Null))
                }
            }
            None => Ok(Value::Scalar(Scalar::Null)),
        },
    }
}
