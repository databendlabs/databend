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

use std::sync::Arc;

use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::PassthroughNullable;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::*;
use regex::Match;
use string::StringColumnBuilder;

pub fn register(registry: &mut FunctionRegistry) {
    let concat = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());

        let signature = FunctionSignature {
            name: "concat".to_string(),
            args_type: vec![DataType::String; args_type.len()],
            return_type: DataType::String,
        };

        fn concat_domain(_: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
            let domain = domains[0].as_string().unwrap();
            FunctionDomain::Domain(Domain::String(StringDomain {
                min: domain.min.clone(),
                max: None,
            }))
        }

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            concat_domain,
            concat_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("concat", concat);

    // nullable concat
    let nullable_concat = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat".to_string(),
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(FunctionDomain::Full),
                eval: Box::new(PassthroughNullable(concat_fn)),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("concat", nullable_concat);

    let concat_ws = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.len() < 2 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws".to_string(),
                args_type: vec![DataType::String; args_type.len()],
                return_type: DataType::String,
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, args_domain| {
                    let domain = args_domain[1].as_string().unwrap();
                    FunctionDomain::Domain(Domain::String(StringDomain {
                        min: domain.min.clone(),
                        max: None,
                    }))
                }),
                eval: scalar_evaluator(|args, _| {
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });
                    let args = args
                        .iter()
                        .map(|arg| arg.try_downcast::<StringType>().unwrap())
                        .collect::<Vec<_>>();

                    let size = len.unwrap_or(1);
                    let mut builder = StringColumnBuilder::with_capacity(size);

                    match &args[0] {
                        Value::Scalar(sep) => {
                            for idx in 0..size {
                                for (arg_index, arg) in args.iter().skip(1).enumerate() {
                                    if arg_index != 0 {
                                        builder.put_str(sep);
                                    }
                                    builder.put_str(unsafe { arg.index_unchecked(idx) });
                                }
                                builder.commit_row();
                            }
                        }
                        Value::Column(c) => {
                            for idx in 0..size {
                                for (arg_index, arg) in args.iter().skip(1).enumerate() {
                                    if arg_index != 0 {
                                        builder.put_str(unsafe { c.index_unchecked(idx) });
                                    }
                                    builder.put_str(unsafe { arg.index_unchecked(idx) });
                                }
                                builder.commit_row();
                            }
                        }
                    }

                    match len {
                        Some(_) => Value::Column(Column::String(builder.build())),
                        _ => Value::Scalar(Scalar::String(builder.build_scalar())),
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("concat_ws", concat_ws);

    // nullable concat ws
    let concat_ws = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.len() < 2 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "concat_ws".to_string(),
                args_type: vec![DataType::Nullable(Box::new(DataType::String)); args_type.len()],
                return_type: DataType::Nullable(Box::new(DataType::String)),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(|args, _| {
                    type T = NullableType<StringType>;
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });

                    let size = len.unwrap_or(1);
                    let new_args = args
                        .iter()
                        .map(|arg| arg.try_downcast::<T>().unwrap())
                        .collect::<Vec<_>>();

                    let mut nullable_builder = T::create_builder(size, &[]);
                    match &new_args[0] {
                        Value::Scalar(None) => {
                            return Value::Scalar(T::upcast_scalar(None));
                        }
                        Value::Scalar(Some(v)) => {
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
                                        builder.put_str(v);
                                    }
                                    builder.put_str(s);
                                }
                                builder.commit_row();
                            }
                        }
                        Value::Column(_) => {
                            let builder = &mut nullable_builder.builder;
                            let validity = &mut nullable_builder.validity;

                            for idx in 0..size {
                                unsafe {
                                    match new_args[0].index_unchecked(idx) {
                                        Some(sep) => {
                                            for (i, s) in new_args
                                                .iter()
                                                .skip(1)
                                                .filter_map(|arg| arg.index_unchecked(idx))
                                                .enumerate()
                                            {
                                                if i != 0 {
                                                    builder.put_str(sep);
                                                }
                                                builder.put_str(s);
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
                            let col = T::upcast_column(nullable_builder.build());
                            Value::Column(col)
                        }
                        _ => Value::Scalar(T::upcast_scalar(nullable_builder.build_scalar())),
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("concat_ws", concat_ws);

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr
    let regexp_instr = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());

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

        let signature = FunctionSignature {
            name: "regexp_instr".to_string(),
            args_type,
            return_type: DataType::Number(NumberDataType::UInt64),
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            regexp_instr_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("regexp_instr", regexp_instr);

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
    let regexp_like = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());
        let args_type = match args_type.len() {
            2 => vec![DataType::String; 2],
            3 => vec![DataType::String; 3],
            _ => return None,
        };

        let signature = FunctionSignature {
            name: "regexp_like".to_string(),
            args_type,
            return_type: DataType::Boolean,
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            regexp_like_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("regexp_like", regexp_like);

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, StringType, _, _>(
        "regexp_extract",
        |_, _, _| FunctionDomain::MayThrow,
        |source_arg, pat_arg, ctx| {
            inner_regexp_extract(&source_arg, &pat_arg, &Value::Scalar(0), ctx)
        },
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, UInt32Type, StringType, _, _>(
        "regexp_extract",
        |_, _, _, _| FunctionDomain::MayThrow,
        |source_arg, pat_arg, group_arg, ctx| {
            inner_regexp_extract(&source_arg, &pat_arg, &group_arg, ctx)
        }
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, ArrayType<StringType>, MapType<StringType, StringType>, _, _>(
        "regexp_extract",
        |_, _, _, _| FunctionDomain::MayThrow,
        |source_arg, pat_arg, name_list_arg, ctx| {
            let len = [&source_arg, &pat_arg].iter().find_map(|arg| match arg {
                Value::Column(col) => Some(col.len()),
                _ => None,
            }).or_else(|| match &name_list_arg {
                Value::Column(col) => Some(col.len()),
                _ => None,
            });

            let cached_reg = match &pat_arg {
                Value::Scalar(pat) => {
                    regexp::build_regexp_from_pattern("regexp_extract", pat, None).ok()
                }
                _ => None,
            };

            let size = len.unwrap_or(1);
            let mut builder =
                MapType::<StringType, StringType>::create_builder(size, ctx.generics);

            for idx in 0..size {
                let source = unsafe { source_arg.index_unchecked(idx) };
                let pat = unsafe { pat_arg.index_unchecked(idx) };
                let name_list = unsafe { name_list_arg.index_unchecked(idx) };
                let mut local_re = None;
                if cached_reg.is_none() {
                    match regexp::build_regexp_from_pattern("regexp_extract", pat, None) {
                        Ok(re) => {
                            local_re = Some(re);
                        }
                        Err(err) => {
                            ctx.set_error(builder.len(), err);
                            builder.push_default();
                            continue;
                        }
                    }
                };
                let re = cached_reg
                    .as_ref()
                    .unwrap_or_else(|| local_re.as_ref().unwrap());
                let captures = re.captures_iter(source).last();
                if let Some(captures) = &captures
                    && name_list.len() + 1 > captures.len() {
                        ctx.set_error(builder.len(), "Not enough group names in regexp_extract");
                        builder.push_default();
                        continue;
                    }
                for (i, name) in name_list.iter().enumerate() {
                    let value = captures
                        .as_ref()
                        .and_then(|caps| caps.get(i + 1).as_ref().map(Match::as_str))
                        .unwrap_or("");
                    builder.put_item((name, value))
                }
                builder.commit_row();
            }
            if len.is_some() {
                Value::Column(builder.build())
            } else {
                Value::Scalar(builder.build_scalar())
            }
        }
    );

    registry
        .register_passthrough_nullable_2_arg::<StringType, StringType, ArrayType<StringType>, _, _>(
            "regexp_extract_all",
            |_, _, _| FunctionDomain::MayThrow,
            |source_arg, pat_arg, ctx| {
                regexp_extract_all(&source_arg, &pat_arg, &Value::Scalar(0), ctx)
            },
        );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, UInt32Type, ArrayType<StringType>, _, _>(
        "regexp_extract_all",
        |_, _, _, _| FunctionDomain::MayThrow,
        |source_arg, pat_arg, group_arg, ctx| {
            regexp_extract_all(&source_arg, &pat_arg, &group_arg, ctx)
        }
    );

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
    let regexp_replace = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());

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

        let signature = FunctionSignature {
            name: "regexp_replace".to_string(),
            args_type,
            return_type: DataType::String,
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            regexp_replace_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("regexp_replace", regexp_replace);

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr
    let regexp_substr = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());
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

        let signature = FunctionSignature {
            name: "regexp_substr".to_string(),
            args_type,
            return_type: DataType::Nullable(Box::new(DataType::String)),
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            regexp_substr_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("regexp_substr", regexp_substr);

    // char function
    let char = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());
        let signature = FunctionSignature {
            name: "char".to_string(),
            args_type: vec![DataType::Number(NumberDataType::Int64); args_type.len()],
            return_type: DataType::String,
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::Full,
            char_fn,
            None,
            has_null,
        )))
    }));
    registry.register_function_factory("char", char);
    registry.register_aliases("char", &["chr"]);
}

fn regexp_extract_all(
    source_arg: &Value<StringType>,
    pat_arg: &Value<StringType>,
    group_arg: &Value<UInt32Type>,
    ctx: &mut EvalContext,
) -> Value<ArrayType<StringType>> {
    let len = [&source_arg, &pat_arg]
        .iter()
        .find_map(|arg| match arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        })
        .or_else(|| match &group_arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        });
    let cached_reg = match &pat_arg {
        Value::Scalar(pat) => regexp::build_regexp_from_pattern("regexp_extract", pat, None).ok(),
        _ => None,
    };

    let size = len.unwrap_or(1);
    let mut builder = ArrayColumnBuilder::<StringType>::with_capacity(size, 0, ctx.generics);
    for idx in 0..size {
        let source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let group = unsafe { group_arg.index_unchecked(idx) as usize };

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_extract", pat, None) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push_default();
                    continue;
                }
            }
        };

        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());
        let mut row = StringColumnBuilder::with_capacity(0);
        if group > 9 {
            ctx.set_error(builder.len(), "Group index must be between 0 and 9!");
        }
        for caps in re.captures_iter(source) {
            if group >= caps.len() {
                ctx.set_error(
                    builder.len(),
                    format!(
                        "Pattern has {} groups. Cannot access group {}",
                        caps.len(),
                        group
                    ),
                );
                row.put_str("");
                row.commit_row();
                continue;
            }
            if let Some(v) = caps.get(group).map(|ma| ma.as_str()) {
                row.put_str(v);
            } else {
                row.put_str("");
            }
            row.commit_row();
        }
        builder.push(row.build());
    }
    if len.is_some() {
        Value::Column(builder.build())
    } else {
        Value::Scalar(builder.build_scalar())
    }
}

fn inner_regexp_extract(
    source_arg: &Value<StringType>,
    pat_arg: &Value<StringType>,
    group_arg: &Value<UInt32Type>,
    ctx: &mut EvalContext,
) -> Value<StringType> {
    let len = [&source_arg, &pat_arg]
        .iter()
        .find_map(|arg| match arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        })
        .or_else(|| match &group_arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        });

    let cached_reg = match &pat_arg {
        Value::Scalar(pat) => regexp::build_regexp_from_pattern("regexp_extract", pat, None).ok(),
        _ => None,
    };

    let size = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(size);
    for idx in 0..size {
        let source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let group = unsafe { group_arg.index_unchecked(idx) as usize };

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_extract", pat, None) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.put_str("");
                    continue;
                }
            }
        };
        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());
        if let Some(caps) = re.captures(source) {
            if group > 9 {
                ctx.set_error(builder.len(), "Group index must be between 0 and 9!");
                builder.put_str("");
            } else if let Some(ma) = caps.get(group) {
                builder.put_str(ma.as_str());
            }
        }
        builder.put_str("");
        builder.commit_row();
    }
    if len.is_some() {
        Value::Column(builder.build())
    } else {
        Value::Scalar(builder.build_scalar())
    }
}

fn concat_fn(args: &[Value<AnyType>], _: &mut EvalContext) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let args = args
        .iter()
        .map(|arg| arg.try_downcast::<StringType>().unwrap())
        .collect::<Vec<_>>();

    let size = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(size);
    for idx in 0..size {
        for arg in &args {
            builder.put_str(unsafe { arg.index_unchecked(idx) });
        }
        builder.commit_row();
    }

    match len {
        Some(_) => Value::Column(Column::String(builder.build())),
        _ => Value::Scalar(Scalar::String(builder.build_scalar())),
    }
}

fn regexp_instr_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
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
    let mut builder = Vec::with_capacity(size);

    let cached_reg = match (&pat_arg, &mt_arg) {
        (Value::Scalar(pat), Some(Value::Scalar(mt))) => {
            regexp::build_regexp_from_pattern("regexp_instr", pat, Some(mt)).ok()
        }
        (Value::Scalar(pat), None) => {
            regexp::build_regexp_from_pattern("regexp_instr", pat, None).ok()
        }
        _ => None,
    };

    for idx in 0..size {
        let mut source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let pos = pos_arg
            .as_ref()
            .map(|pos_arg| unsafe { pos_arg.index_unchecked(idx) });
        let occur = occur_arg
            .as_ref()
            .map(|occur_arg| unsafe { occur_arg.index_unchecked(idx) });
        let ro = ro_arg
            .as_ref()
            .map(|ro_arg| unsafe { ro_arg.index_unchecked(idx) });
        let mt = mt_arg
            .as_ref()
            .map(|mt_arg| unsafe { mt_arg.index_unchecked(idx) });

        if let Err(err) = regexp::validate_regexp_arguments("regexp_instr", pos, occur, ro) {
            ctx.set_error(builder.len(), err);
            builder.push(0);
            continue;
        }

        if source.is_empty() || pat.is_empty() {
            builder.push(0);
            continue;
        }

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_instr", pat, mt) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                    continue;
                }
            }
        };
        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());

        let pos = pos.unwrap_or(1);
        let occur = occur.unwrap_or(1);
        let ro = ro.unwrap_or(0);

        if let Some((idx, _)) = source.char_indices().nth((pos - 1) as usize) {
            source = &source[idx..];
        }
        let instr = regexp::regexp_instr(source, re, pos, occur, ro);
        builder.push(instr);
    }

    match len {
        Some(_) => Value::Column(Column::Number(NumberColumn::UInt64(builder.into()))),
        _ => Value::Scalar(Scalar::Number(NumberScalar::UInt64(builder.pop().unwrap()))),
    }
}

fn regexp_like_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let source_arg = args[0].try_downcast::<StringType>().unwrap();
    let pat_arg = args[1].try_downcast::<StringType>().unwrap();
    let mt_arg = if args.len() >= 3 {
        Some(args[2].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let cached_reg = match (&pat_arg, &mt_arg) {
        (Value::Scalar(pat), Some(Value::Scalar(mt))) => {
            regexp::build_regexp_from_pattern("regexp_like", pat, Some(mt)).ok()
        }
        (Value::Scalar(pat), None) => {
            regexp::build_regexp_from_pattern("regexp_like", pat, None).ok()
        }
        _ => None,
    };

    let size = len.unwrap_or(1);
    let mut builder = MutableBitmap::with_capacity(size);
    for idx in 0..size {
        let source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let mt = mt_arg
            .as_ref()
            .map(|mt_arg| unsafe { mt_arg.index_unchecked(idx) });

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_like", pat, mt) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(false);
                    continue;
                }
            }
        };
        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());
        builder.push(re.is_match(source));
    }
    match len {
        Some(_) => Value::Column(Column::Boolean(builder.into())),
        _ => Value::Scalar(Scalar::Boolean(builder.pop().unwrap())),
    }
}

fn regexp_replace_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
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
    let mut builder = StringColumnBuilder::with_capacity(size);

    let cached_reg = match (&pat_arg, &mt_arg) {
        (Value::Scalar(pat), Some(Value::Scalar(mt))) => {
            regexp::build_regexp_from_pattern("regexp_replace", pat, Some(mt)).ok()
        }
        (Value::Scalar(pat), None) => {
            regexp::build_regexp_from_pattern("regexp_replace", pat, None).ok()
        }
        _ => None,
    };

    for idx in 0..size {
        let source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let repl = unsafe { repl_arg.index_unchecked(idx) };
        let pos = pos_arg
            .as_ref()
            .map(|pos_arg| unsafe { pos_arg.index_unchecked(idx) });
        let occur = occur_arg
            .as_ref()
            .map(|occur_arg| unsafe { occur_arg.index_unchecked(idx) });
        let mt = mt_arg
            .as_ref()
            .map(|mt_arg| unsafe { mt_arg.index_unchecked(idx) });

        if let Some(occur) = occur
            && occur < 0
        {
            ctx.set_error(builder.len(), format!(
                    "Incorrect arguments to regexp_replace: occurrence must not be negative, but got {}",
                    occur
                ));
            StringType::push_default(&mut builder);
            continue;
        }

        if let Err(err) = regexp::validate_regexp_arguments("regexp_replace", pos, None, None) {
            ctx.set_error(builder.len(), err);
            StringType::push_default(&mut builder);
            continue;
        }

        if source.is_empty() || pat.is_empty() {
            builder.put_and_commit(source);
            continue;
        }

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_replace", pat, mt) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    StringType::push_default(&mut builder);
                    continue;
                }
            }
        };
        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());

        let pos = pos.unwrap_or(1);
        let occur = occur.unwrap_or(0);

        regexp::regexp_replace(source, re, repl, pos, occur, &mut builder);
        builder.commit_row();
    }
    match len {
        Some(_) => Value::Column(Column::String(builder.build())),
        _ => Value::Scalar(Scalar::String(builder.build_scalar())),
    }
}

fn regexp_substr_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
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

    let cached_reg = match (&pat_arg, &mt_arg) {
        (Value::Scalar(pat), Some(Value::Scalar(mt))) => {
            regexp::build_regexp_from_pattern("regexp_replace", pat, Some(mt)).ok()
        }
        (Value::Scalar(pat), None) => {
            regexp::build_regexp_from_pattern("regexp_replace", pat, None).ok()
        }
        _ => None,
    };

    let size = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(size);
    let mut validity = MutableBitmap::with_capacity(size);
    for idx in 0..size {
        let source = unsafe { source_arg.index_unchecked(idx) };
        let pat = unsafe { pat_arg.index_unchecked(idx) };
        let pos = pos_arg
            .as_ref()
            .map(|pos_arg| unsafe { pos_arg.index_unchecked(idx) });
        let occur = occur_arg
            .as_ref()
            .map(|occur_arg| unsafe { occur_arg.index_unchecked(idx) });
        let mt = mt_arg
            .as_ref()
            .map(|mt_arg| unsafe { mt_arg.index_unchecked(idx) });

        if let Err(err) = regexp::validate_regexp_arguments("regexp_substr", pos, occur, None) {
            ctx.set_error(builder.len(), err);
            StringType::push_default(&mut builder);
            validity.push(false);
            continue;
        }

        if source.is_empty() || pat.is_empty() {
            validity.push(false);
            builder.commit_row();
            continue;
        }

        let pos = pos.unwrap_or(1);
        let occur = occur.unwrap_or(1);

        let mut local_re = None;
        if cached_reg.is_none() {
            match regexp::build_regexp_from_pattern("regexp_substr", pat, mt) {
                Ok(re) => {
                    local_re = Some(re);
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    StringType::push_default(&mut builder);
                    validity.push(false);
                    continue;
                }
            }
        };
        let re = cached_reg
            .as_ref()
            .unwrap_or_else(|| local_re.as_ref().unwrap());

        let substr = regexp::regexp_substr(source, re, pos, occur);
        match substr {
            Some(substr) => {
                builder.put_str(substr);
                validity.push(true);
            }
            None => {
                validity.push(false);
            }
        }
        builder.commit_row();
    }
    match len {
        Some(_) => {
            let col = NullableColumn::new_column(Column::String(builder.build()), validity.into());
            Value::Column(col)
        }
        _ => match validity.pop() {
            Some(is_not_null) => {
                if is_not_null {
                    Value::Scalar(Scalar::String(builder.build_scalar()))
                } else {
                    Value::Scalar(Scalar::Null)
                }
            }
            None => Value::Scalar(Scalar::Null),
        },
    }
}

fn char_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let args = args
        .iter()
        .map(|arg| arg.try_downcast::<Int64Type>().unwrap())
        .collect::<Vec<_>>();

    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let input_rows = len.unwrap_or(1);
    let mut builder = StringColumnBuilder::with_capacity(input_rows);

    'F: for row in 0..input_rows {
        for arg in &args {
            let val = arg.index(row).unwrap();
            // turn val into unicode char
            if val < 0 || val > u32::MAX as i64 {
                builder.put_str("");
                builder.commit_row();
                ctx.set_error(row, "Invalid character code in the CHR input");
                continue 'F;
            }

            if let Some(c) = std::char::from_u32(val as u32) {
                builder.put_char(c);
            } else {
                builder.put_str("");
                builder.commit_row();
                ctx.set_error(row, "Invalid character code in the CHR input");
                continue 'F;
            }
        }
        builder.commit_row();
    }

    match len {
        Some(_) => Value::Column(Column::String(builder.build())),
        _ => Value::Scalar(Scalar::String(builder.build_scalar())),
    }
}

pub mod regexp {
    use databend_common_expression::types::string::StringColumnBuilder;
    use regex::Regex;
    use regex::RegexBuilder;

    #[inline]
    pub fn build_regexp_from_pattern(
        fn_name: &str,
        pat: &str,
        mt: Option<&str>,
    ) -> Result<Regex, String> {
        let pattern = match pat.is_empty() {
            true => "^$",
            false => pat,
        };

        // the default match type value is 'i', if it is empty
        let mt = match mt {
            Some(mt) => {
                if mt.is_empty() {
                    "i"
                } else {
                    mt
                }
            }
            None => "i",
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
        if let Some(pos) = pos
            && pos < 1
        {
            return Err(format!(
                "Incorrect arguments to {}: position must be positive, but got {}",
                fn_name, pos
            ));
        }
        if let Some(occur) = occur
            && occur < 1
        {
            return Err(format!(
                "Incorrect arguments to {}: occurrence must be positive, but got {}",
                fn_name, occur
            ));
        }
        if let Some(ro) = ro
            && ro != 0
            && ro != 1
        {
            return Err(format!(
                "Incorrect arguments to {}: return_option must be 1 or 0, but got {}",
                fn_name, ro
            ));
        }

        Ok(())
    }

    #[inline]
    pub fn regexp_instr(
        expr: &str,
        regex: &Regex,
        pos: i64,
        occurrence: i64,
        return_option: i64,
    ) -> u64 {
        if let Some(m) = regex.find_iter(expr).nth((occurrence - 1) as usize) {
            let find_pos = if return_option == 0 {
                m.start()
            } else {
                m.end()
            };

            let count = expr[..find_pos].chars().count() as i64;
            return (count + pos) as _;
        }
        0
    }

    #[inline]
    pub fn regexp_replace(
        s: &str,
        re: &Regex,
        repl: &str,
        pos: i64,
        occur: i64,
        builder: &mut StringColumnBuilder,
    ) {
        let pos = (pos - 1) as usize;
        // set the index start from 0
        // the 'pos' position is the character index,
        // so we should iterate the character to find the byte index.
        let char_pos = match s.char_indices().nth(pos) {
            Some((start, _)) => start,
            None => {
                builder.put_str(s);
                return;
            }
        };

        let (before_trimmed, trimmed) = (&s[..char_pos], &s[char_pos..]);
        builder.put_str(before_trimmed);

        // means we should replace all matched strings
        if occur == 0 {
            builder.put_str(&re.replace_all(trimmed, repl));
        } else if let Some(capature) = re.captures_iter(trimmed).nth((occur - 1) as _) {
            // unwrap on 0 is OK because captures only reports matches.
            let m = capature.get(0).unwrap();
            builder.put_str(&trimmed[0..m.start()]);
            builder.put_str(repl);
            builder.put_str(&trimmed[m.end()..]);
        } else {
            builder.put_str(trimmed);
        }
    }

    #[inline]
    pub fn regexp_substr<'a>(s: &'a str, re: &Regex, pos: i64, occur: i64) -> Option<&'a str> {
        let occur = if occur < 1 { 1 } else { occur };
        let pos = if pos < 1 { 0 } else { (pos - 1) as usize };

        // the 'pos' position is the character index,
        // so we should iterate the character to find the byte index.
        let char_pos = match s.char_indices().nth(pos) {
            Some((start, _)) => start,
            None => return None,
        };

        let m = re.find_iter(&s[char_pos..]).nth((occur - 1) as _);
        m.map(|m| m.as_str())
    }
}
