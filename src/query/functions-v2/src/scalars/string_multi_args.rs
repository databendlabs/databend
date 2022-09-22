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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Int64Type;
use common_expression::types::number::NumberColumnBuilder;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::UInt8Type;
use common_expression::types::string::StringColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::string::StringDomain;
use common_expression::types::AnyType;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::GenericMap;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::wrap_nullable;
use common_expression::Column;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;
use regex::bytes::Regex;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("concat", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat",
                args_type: vec![DataType::String; args_type.len()],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
                let domain = args_domain[0].as_string().unwrap();
                Some(Domain::String(StringDomain {
                    min: domain.min.clone(),
                    max: None,
                }))
            }),
            eval: Box::new(concat_fn),
        }))
    });

    // nullable concat
    registry.register_function_factory("concat", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat",
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(concat_fn)),
        }))
    });

    registry.register_function_factory("concat_ws", |_, args_type| {
        if args_type.len() < 2 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws",
                args_type: vec![DataType::String; args_type.len()],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
                let domain = args_domain[1].as_string().unwrap();
                Some(Domain::String(StringDomain {
                    min: domain.min.clone(),
                    max: None,
                }))
            }),
            eval: Box::new(|args, _generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                let args = args
                    .iter()
                    .map(|arg| arg.try_downcast::<StringType>().unwrap())
                    .collect::<Vec<_>>();

                let size = len.unwrap_or(1);
                let mut builder = StringColumnBuilder::with_capacity(size, 0);

                match &args[0] {
                    ValueRef::Scalar(v) => {
                        for idx in 0..size {
                            for (arg_index, arg) in args.iter().skip(1).enumerate() {
                                if arg_index != 0 {
                                    builder.put_slice(v);
                                }
                                unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                            }
                            builder.commit_row();
                        }
                    }
                    ValueRef::Column(c) => {
                        for idx in 0..size {
                            for (arg_index, arg) in args.iter().skip(1).enumerate() {
                                if arg_index != 0 {
                                    unsafe {
                                        builder.put_slice(c.index_unchecked(idx));
                                    }
                                }
                                unsafe { builder.put_slice(arg.index_unchecked(idx)) }
                            }
                            builder.commit_row();
                        }
                    }
                }

                match len {
                    Some(_) => Ok(Value::Column(Column::String(builder.build()))),
                    _ => Ok(Value::Scalar(Scalar::String(builder.build_scalar()))),
                }
            }),
        }))
    });

    // nullable concat ws
    registry.register_function_factory("concat_ws", |_, args_type| {
        if args_type.len() < 2 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws",
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(|args, _generics| {
                type T = NullableType<StringType>;
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let size = len.unwrap_or(1);
                let new_args = args
                    .iter()
                    .map(|arg| arg.try_downcast::<T>().unwrap())
                    .collect::<Vec<_>>();

                let mut nullable_builder = T::create_builder(size, &[]);
                match &new_args[0] {
                    ValueRef::Scalar(None) => {
                        return Ok(Value::Scalar(T::upcast_scalar(None)));
                    }
                    ValueRef::Scalar(Some(v)) => {
                        let builder = &mut nullable_builder.builder;
                        nullable_builder.validity.extend_constant(size, true);

                        for idx in 0..size {
                            for (i, s) in new_args
                                .iter()
                                .skip(1)
                                .filter_map(|arg| unsafe { arg.index_unchecked(idx) })
                                .enumerate()
                            {
                                if i != 0 {
                                    builder.put_slice(v);
                                }
                                builder.put_slice(s);
                            }
                            builder.commit_row();
                        }
                    }
                    ValueRef::Column(_) => {
                        let mut nullable_builder = T::create_builder(size, &[]);
                        let builder = &mut nullable_builder.builder;
                        let validity = &mut nullable_builder.validity;

                        for idx in 0..size {
                            unsafe {
                                match new_args[0].index_unchecked(idx) {
                                    Some(v) => {
                                        for idx in 0..size {
                                            for (i, s) in new_args
                                                .iter()
                                                .skip(1)
                                                .filter_map(|arg| arg.index_unchecked(idx))
                                                .enumerate()
                                            {
                                                if i != 0 {
                                                    builder.put_slice(v);
                                                }
                                                builder.put_slice(s);
                                            }
                                            builder.commit_row();
                                        }
                                        builder.commit_row();
                                        validity.push(true);
                                    }
                                    None => {
                                        builder.commit_row();
                                        validity.push(false);
                                    }
                                }
                            }
                        }
                    }
                }
                match len {
                    Some(_) => {
                        let n = nullable_builder.build();
                        let c = T::upcast_column(n);
                        Ok(Value::Column(c))
                    }
                    _ => Ok(Value::Scalar(T::upcast_scalar(
                        nullable_builder.build_scalar(),
                    ))),
                }
            }),
        }))
    });

    registry.register_function_factory("char", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "char",
                args_type: vec![DataType::Number(NumberDataType::UInt8); args_type.len()],
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(char_fn),
        }))
    });

    // nullable char
    registry.register_function_factory("char", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "char",
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Number(
                        NumberDataType::UInt8
                    )));
                    args_type.len()
                ],
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(char_fn)),
        }))
    });

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr
    registry.register_function_factory("regexp_instr", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::String; 2],
            3 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
            ],
            4 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
            ],
            5 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
            ],
            6 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
                DataType::String,
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_instr",
                args_type,
                return_type: DataType::Number(NumberDataType::UInt64),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(regexp_instr_fn),
        }))
    });

    // nullable regexp_instr
    registry.register_function_factory("regexp_instr", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::Nullable(Box::new(DataType::String)); 2],
            3 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            4 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            5 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            6 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::String)),
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_instr",
                args_type,
                return_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(regexp_instr_fn)),
        }))
    });

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
    registry.register_function_factory("regexp_like", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::String; 2],
            3 => vec![DataType::String; 3],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_like",
                args_type,
                return_type: DataType::Boolean,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(regexp_like_fn),
        }))
    });

    // nullable regexp_replace
    registry.register_function_factory("regexp_like", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::Nullable(Box::new(DataType::String)); 2],
            3 => vec![DataType::Nullable(Box::new(DataType::String)); 3],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_like",
                args_type,
                return_type: DataType::Nullable(Box::new(DataType::Boolean)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(regexp_like_fn)),
        }))
    });

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
    registry.register_function_factory("regexp_replace", |_, args_type| {
        let args_type = match args_type.len() {
            3 => vec![DataType::String; 3],
            4 => vec![
                DataType::String,
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
            ],
            5 => vec![
                DataType::String,
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
            ],
            6 => vec![
                DataType::String,
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
                DataType::String,
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_replace",
                args_type,
                return_type: DataType::String,
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(regexp_replace_fn),
        }))
    });

    // nullable regexp_replace
    registry.register_function_factory("regexp_replace", |_, args_type| {
        let args_type = match args_type.len() {
            3 => vec![DataType::Nullable(Box::new(DataType::String)); 3],
            4 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            5 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            6 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::String)),
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_replace",
                args_type,
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(regexp_replace_fn)),
        }))
    });

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr
    registry.register_function_factory("regexp_substr", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::String; 2],
            3 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
            ],
            4 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
            ],
            5 => vec![
                DataType::String,
                DataType::String,
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
                DataType::String,
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_substr",
                args_type,
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(regexp_substr_fn),
        }))
    });

    // nullable regexp_substr
    registry.register_function_factory("regexp_substr", |_, args_type| {
        let args_type = match args_type.len() {
            2 => vec![DataType::Nullable(Box::new(DataType::String)); 2],
            3 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            4 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            ],
            5 => vec![
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::String)),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                DataType::Nullable(Box::new(DataType::String)),
            ],
            _ => return None,
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "regexp_substr",
                args_type,
                return_type: DataType::Nullable(Box::new(DataType::String)),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_, _| None),
            eval: Box::new(wrap_nullable(regexp_substr_fn)),
        }))
    });
}

fn concat_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let args = args
        .iter()
        .map(|arg| arg.try_downcast::<StringType>().unwrap())
        .collect::<Vec<_>>();

    let size = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        for arg in &args {
            unsafe { builder.put_slice(arg.index_unchecked(idx)) }
        }
        builder.commit_row();
    }

    match len {
        Some(_) => Ok(Value::Column(Column::String(builder.build()))),
        _ => Ok(Value::Scalar(Scalar::String(builder.build_scalar()))),
    }
}

fn char_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let args = args
        .iter()
        .map(|arg| arg.try_downcast::<UInt8Type>().unwrap())
        .collect::<Vec<_>>();

    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let input_rows = len.unwrap_or(1);

    let mut values: Vec<u8> = vec![0; input_rows * args.len()];
    let values_ptr = values.as_mut_ptr();

    for (i, arg) in args.iter().enumerate() {
        match arg {
            ValueRef::Scalar(v) => {
                for j in 0..input_rows {
                    unsafe {
                        *values_ptr.add(args.len() * j + i) = *v;
                    }
                }
            }
            ValueRef::Column(c) => {
                for (j, ch) in UInt8Type::iter_column(c).enumerate() {
                    unsafe {
                        *values_ptr.add(args.len() * j + i) = ch;
                    }
                }
            }
        }
    }
    let offsets = (0..(input_rows + 1) as u64 * args.len() as u64)
        .step_by(args.len())
        .collect::<Vec<_>>();
    let result = StringColumn {
        data: values.into(),
        offsets: offsets.into(),
    };
    Ok(Value::Column(Column::String(result)))
}

fn regexp_instr_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let pat_arg = args[1].try_downcast::<StringType>().unwrap();
    let pos_arg = if args.len() >= 3 {
        Some(args[2].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let occur_arg = if args.len() >= 4 {
        Some(args[3].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let ro_arg = if args.len() >= 5 {
        Some(args[4].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let mt_arg = if args.len() >= 6 {
        Some(args[5].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let size = len.unwrap_or(1);
    let mut key: Vec<Vec<u8>> = Vec::new();
    let mut map: HashMap<Vec<Vec<u8>>, Regex> = HashMap::new();
    let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, size);
    for idx in 0..size {
        unsafe {
            let source = source_arg.index_unchecked(idx);
            let pat = pat_arg.index_unchecked(idx);
            let pos = pos_arg.as_ref().map(|pos_arg| pos_arg.index_unchecked(idx));
            let occur = occur_arg
                .as_ref()
                .map(|occur_arg| occur_arg.index_unchecked(idx));
            let ro = ro_arg.as_ref().map(|ro_arg| ro_arg.index_unchecked(idx));
            let mt = mt_arg.as_ref().map(|mt_arg| mt_arg.index_unchecked(idx));

            regexp::validate_regexp_arguments("regexp_instr", pos, occur, ro)?;
            if source.is_empty() || pat.is_empty() {
                builder.push(NumberScalar::UInt64(0u64));
                continue;
            }
            key.push(pat.to_vec());
            if let Some(mt) = mt {
                key.push(mt.to_vec());
            }
            let re = if let Some(re) = map.get(&key) {
                re
            } else {
                let re = regexp::build_regexp_from_pattern("regexp_instr", pat, mt)?;
                map.insert(key.clone(), re);
                map.get(&key).unwrap()
            };
            key.clear();

            let pos = pos.unwrap_or(1);
            let occur = occur.unwrap_or(1);
            let ro = ro.unwrap_or(0);

            let instr = regexp::regexp_instr(source, re, pos, occur, ro);
            builder.push(NumberScalar::UInt64(instr));
        }
    }
    match len {
        Some(_) => Ok(Value::Column(Column::Number(builder.build()))),
        _ => Ok(Value::Scalar(Scalar::Number(builder.build_scalar()))),
    }
}

fn regexp_like_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let pat_arg = args[1].try_downcast::<StringType>().unwrap();
    let mt_arg = if args.len() >= 3 {
        Some(args[2].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let size = len.unwrap_or(1);
    let mut key: Vec<Vec<u8>> = Vec::new();
    let mut map: HashMap<Vec<Vec<u8>>, Regex> = HashMap::new();
    let mut builder = MutableBitmap::with_capacity(size);
    for idx in 0..size {
        unsafe {
            let source = source_arg.index_unchecked(idx);
            let pat = pat_arg.index_unchecked(idx);
            let mt = mt_arg.as_ref().map(|mt_arg| mt_arg.index_unchecked(idx));

            key.push(pat.to_vec());
            if let Some(mt) = mt {
                key.push(mt.to_vec());
            }
            let re = if let Some(re) = map.get(&key) {
                re
            } else {
                let re = regexp::build_regexp_from_pattern("regexp_like", pat, mt)?;
                map.insert(key.clone(), re);
                map.get(&key).unwrap()
            };
            key.clear();

            builder.push(re.is_match(source));
        }
    }
    match len {
        Some(_) => Ok(Value::Column(Column::Boolean(builder.into()))),
        _ => Ok(Value::Scalar(Scalar::Boolean(builder.pop().unwrap()))),
    }
}

fn regexp_replace_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let pat_arg = args[1].try_downcast::<StringType>().unwrap();
    let repl_arg = args[2].try_downcast::<StringType>().unwrap();
    let pos_arg = if args.len() >= 4 {
        Some(args[3].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let occur_arg = if args.len() >= 5 {
        Some(args[4].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let mt_arg = if args.len() >= 6 {
        Some(args[5].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let size = len.unwrap_or(1);
    let mut key: Vec<Vec<u8>> = Vec::new();
    let mut map: HashMap<Vec<Vec<u8>>, Regex> = HashMap::new();
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    for idx in 0..size {
        unsafe {
            let source = source_arg.index_unchecked(idx);
            let pat = pat_arg.index_unchecked(idx);
            let repl = repl_arg.index_unchecked(idx);
            let pos = pos_arg.as_ref().map(|pos_arg| pos_arg.index_unchecked(idx));
            let occur = occur_arg
                .as_ref()
                .map(|occur_arg| occur_arg.index_unchecked(idx));
            let mt = mt_arg.as_ref().map(|mt_arg| mt_arg.index_unchecked(idx));

            if let Some(occur) = occur {
                if occur < 0 {
                    // the occurrence argument for regexp_replace is different with other regexp_* function
                    // the value of '0' is valid, so check the value here separately
                    return Err(format!(
                        "Incorrect arguments to regexp_replace: occurrence must not be negative, but got {}",
                        occur
                    ));
                }
            }
            regexp::validate_regexp_arguments("regexp_replace", pos, None, None)?;
            if source.is_empty() || pat.is_empty() {
                builder.data.extend_from_slice(source);
                builder.commit_row();
                continue;
            }
            key.push(pat.to_vec());
            if let Some(mt) = mt {
                key.push(mt.to_vec());
            }
            let re = if let Some(re) = map.get(&key) {
                re
            } else {
                let re = regexp::build_regexp_from_pattern("regexp_replace", pat, mt)?;
                map.insert(key.clone(), re);
                map.get(&key).unwrap()
            };
            key.clear();

            let pos = pos.unwrap_or(1);
            let occur = occur.unwrap_or(0);

            regexp::regexp_replace(source, re, repl, pos, occur, &mut builder.data);
            builder.commit_row();
        }
    }
    match len {
        Some(_) => Ok(Value::Column(Column::String(builder.build()))),
        _ => Ok(Value::Scalar(Scalar::String(builder.build_scalar()))),
    }
}

fn regexp_substr_fn(args: &[ValueRef<AnyType>], _: &GenericMap) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });

    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let pat_arg = args[1].try_downcast::<StringType>().unwrap();
    let pos_arg = if args.len() >= 3 {
        Some(args[2].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let occur_arg = if args.len() >= 4 {
        Some(args[3].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };
    let mt_arg = if args.len() >= 5 {
        Some(args[4].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let size = len.unwrap_or(1);
    let mut key: Vec<Vec<u8>> = Vec::new();
    let mut map: HashMap<Vec<Vec<u8>>, Regex> = HashMap::new();
    let mut builder = StringColumnBuilder::with_capacity(size, 0);
    let mut validity = MutableBitmap::with_capacity(size);
    for idx in 0..size {
        unsafe {
            let source = source_arg.index_unchecked(idx);
            let pat = pat_arg.index_unchecked(idx);
            let pos = pos_arg.as_ref().map(|pos_arg| pos_arg.index_unchecked(idx));
            let occur = occur_arg
                .as_ref()
                .map(|occur_arg| occur_arg.index_unchecked(idx));
            let mt = mt_arg.as_ref().map(|mt_arg| mt_arg.index_unchecked(idx));

            regexp::validate_regexp_arguments("regexp_substr", pos, occur, None)?;
            if source.is_empty() || pat.is_empty() {
                validity.push(false);
                builder.commit_row();
                continue;
            }
            key.push(pat.to_vec());
            if let Some(mt) = mt {
                key.push(mt.to_vec());
            }
            let re = if let Some(re) = map.get(&key) {
                re
            } else {
                let re = regexp::build_regexp_from_pattern("regexp_substr", pat, mt)?;
                map.insert(key.clone(), re);
                map.get(&key).unwrap()
            };
            key.clear();

            let pos = pos.unwrap_or(1);
            let occur = occur.unwrap_or(1);

            let substr = regexp::regexp_substr(source, re, pos, occur);
            match substr {
                Some(substr) => {
                    builder.put_slice(substr);
                    validity.push(true);
                }
                None => {
                    validity.push(false);
                }
            }
            builder.commit_row();
        }
    }
    match len {
        Some(_) => {
            let col = Column::Nullable(Box::new(NullableColumn {
                validity: validity.into(),
                column: Column::String(builder.build()),
            }));
            Ok(Value::Column(col))
        }
        _ => {
            let is_not_null = validity.pop();
            match is_not_null {
                Some(is_not_null) => {
                    if is_not_null {
                        Ok(Value::Column(Column::String(builder.build())))
                    } else {
                        Ok(Value::Scalar(Scalar::Null))
                    }
                }
                None => Ok(Value::Scalar(Scalar::Null)),
            }
        }
    }
}

mod regexp {
    use bstr::ByteSlice;
    use regex::bytes::Match;
    use regex::bytes::Regex;
    use regex::bytes::RegexBuilder;

    #[inline]
    pub fn build_regexp_from_pattern(
        fn_name: &str,
        pat: &[u8],
        mt: Option<&[u8]>,
    ) -> Result<Regex, String> {
        let pattern = match pat.is_empty() {
            true => "^$",
            false => simdutf8::basic::from_utf8(pat).map_err(|e| {
                format!("Unable to convert the {} pattern to string: {}", fn_name, e)
            })?,
        };
        // the default match type value is 'i', if it is empty
        let mt = match mt {
            Some(mt) => {
                if mt.is_empty() {
                    "i".as_bytes()
                } else {
                    mt
                }
            }
            None => "i".as_bytes(),
        };

        let mut builder = RegexBuilder::new(pattern);

        for c in mt.chars() {
            let r = match c {
                'c' => Ok(builder.case_insensitive(false)),
                'i' => Ok(builder.case_insensitive(true)),
                'm' => Ok(builder.multi_line(true)),
                'n' => Ok(builder.dot_matches_new_line(true)),
                // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
                // Notes: https://docs.rs/regex/1.5.4/regex/bytes/struct.RegexBuilder.html
                // Notes: https://github.com/rust-lang/regex/issues/244
                // It seems that the regexp crate doesn't support the 'u' match type.
                'u' => Err(format!(
                    "Unsupported arguments to {} match type: {}",
                    fn_name, c,
                )),
                _ => Err(format!(
                    "Incorrect arguments to {} match type: {}",
                    fn_name, c,
                )),
            };
            #[allow(clippy::question_mark)]
            if let Err(e) = r {
                return Err(e);
            }
        }
        builder
            .build()
            .map_err(|e| format!("Unable to build regex from {} pattern: {}", fn_name, e))
    }

    /// Validates the arguments of 'regexp_*' functions, returns error if any of arguments is invalid
    /// and make the error logic the same as snowflake, since it is more reasonable and consistent
    #[inline]
    pub fn validate_regexp_arguments(
        fn_name: &str,
        pos: Option<i64>,
        occur: Option<i64>,
        ro: Option<i64>,
    ) -> Result<(), String> {
        if let Some(pos) = pos {
            if pos < 1 {
                return Err(format!(
                    "Incorrect arguments to {}: position must be positive, but got {}",
                    fn_name, pos
                ));
            }
        }
        if let Some(occur) = occur {
            if occur < 1 {
                return Err(format!(
                    "Incorrect arguments to {}: occurrence must be positive, but got {}",
                    fn_name, occur
                ));
            }
        }
        if let Some(ro) = ro {
            if ro != 0 && ro != 1 {
                return Err(format!(
                    "Incorrect arguments to {}: return_option must be 1 or 0, but got {}",
                    fn_name, ro
                ));
            }
        }

        Ok(())
    }

    #[inline]
    pub fn regexp_instr(s: &[u8], re: &Regex, pos: i64, occur: i64, ro: i64) -> u64 {
        let pos = (pos - 1) as usize; // set the index start from 0

        // the 'pos' postion is the character index,
        // so we should iterate the character to find the byte index.
        let mut pos = match s.char_indices().nth(pos) {
            Some((start, _, _)) => start,
            None => return 0,
        };

        let m = regexp_match_result(s, re, &mut pos, &occur);
        if m.is_none() {
            return 0;
        }

        // the matched result is the byte index, but the 'regexp_instr' function returns the character index,
        // so we should iterate the character to find the character index.
        let mut instr = 0_usize;
        for (p, (start, end, _)) in s.char_indices().enumerate() {
            if ro == 0 {
                if start == m.unwrap().start() {
                    instr = p + 1;
                    break;
                }
            } else if end == m.unwrap().end() {
                instr = p + 2;
                break;
            }
        }

        instr as u64
    }

    #[inline]
    pub fn regexp_replace(
        s: &[u8],
        re: &Regex,
        repl: &[u8],
        pos: i64,
        occur: i64,
        buf: &mut Vec<u8>,
    ) {
        let pos = (pos - 1) as usize; // set the index start from 0

        // the 'pos' postion is the character index,
        // so we should iterate the character to find the byte index.
        let mut pos = match s.char_indices().nth(pos) {
            Some((start, _, _)) => start,
            None => {
                buf.extend_from_slice(s);
                return;
            }
        };

        let m = regexp_match_result(s, re, &mut pos, &occur);
        if m.is_none() {
            buf.extend_from_slice(s);
            return;
        }

        buf.extend_from_slice(&s[..m.unwrap().start()]);

        if occur == 0 {
            let s = &s[m.unwrap().start()..];
            buf.extend_from_slice(&re.replace_all(s, repl));
        } else {
            buf.extend_from_slice(repl);
            buf.extend_from_slice(&s[m.unwrap().end()..])
        }
    }

    #[inline]
    pub fn regexp_substr<'a>(s: &'a [u8], re: &Regex, pos: i64, occur: i64) -> Option<&'a [u8]> {
        let occur = if occur < 1 { 1 } else { occur };
        let pos = if pos < 1 { 0 } else { (pos - 1) as usize };

        // the 'pos' postion is the character index,
        // so we should iterate the character to find the byte index.
        let mut pos = match s.char_indices().nth(pos) {
            Some((start, _, _)) => start,
            None => return None,
        };

        let m = regexp_match_result(s, re, &mut pos, &occur);

        m.map(|m| m.as_bytes())
    }

    #[inline]
    fn regexp_match_result<'a>(
        s: &'a [u8],
        re: &Regex,
        pos: &mut usize,
        occur: &i64,
    ) -> Option<Match<'a>> {
        let mut i = 1_i64;
        let m = loop {
            let m = re.find_at(s, *pos);
            if i >= *occur || m.is_none() {
                break m;
            }

            i += 1;
            if let Some(m) = m {
                // set the start postion of 'find_at' function to the position following the matched substring
                *pos = m.end();
            }
        };

        m
    }
}
