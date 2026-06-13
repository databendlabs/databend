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

use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_expression::aggregate::AggrState;
use databend_common_expression::aggregate::AggregateFunctionRef;
use databend_common_expression::aggregate::StateAddr;
use databend_common_expression::aggregate::StatesLayout;
use databend_common_expression::aggregate::get_states_layout;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::KeysRef;
use databend_common_hashtable::StackHashMap;
use databend_common_hashtable::StackHashSet;
use itertools::Itertools;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::AggregateFunctionFactory;

const ARRAY_AGGREGATE_FUNCTIONS: &[(&str, &str); 14] = &[
    ("array_avg", "avg"),
    ("array_count", "count"),
    ("array_max", "max"),
    ("array_min", "min"),
    ("array_sum", "sum"),
    ("array_any", "any"),
    ("array_stddev_samp", "stddev_samp"),
    ("array_stddev_pop", "stddev_pop"),
    ("array_stddev", "stddev"),
    ("array_std", "std"),
    ("array_median", "median"),
    ("array_approx_count_distinct", "approx_count_distinct"),
    ("array_kurtosis", "kurtosis"),
    ("array_skewness", "skewness"),
];

const ARRAY_SORT_FUNCTIONS: &[(&str, (bool, bool)); 4] = &[
    ("array_sort_asc_null_first", (true, true)),
    ("array_sort_desc_null_first", (false, true)),
    ("array_sort_asc_null_last", (true, false)),
    ("array_sort_desc_null_last", (false, false)),
];

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("contains", &["array_contains"]);
    registry.register_aliases("get", &["array_get"]);
    registry.register_aliases("length", &["array_length", "array_size"]);
    registry.register_aliases("slice", &["array_slice"]);
    registry.register_aliases("range", &["array_generate_range"]);

    register_array_aggr(registry);

    registry.register_0_arg_core::<EmptyArrayType, _>(
        "array",
        |_| FunctionDomain::Full,
        |_| Value::Scalar(()),
    );

    let array_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "array".to_string(),
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Array(Box::new(DataType::Generic(0))),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, args_domain| {
                    FunctionDomain::Domain(
                        args_domain.iter().fold(Domain::Array(None), |acc, x| {
                            acc.merge(&Domain::Array(Some(Box::new(x.clone()))))
                        }),
                    )
                }),
                eval: scalar_evaluator(|args, ctx| {
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });

                    let mut builder: ArrayColumnBuilder<GenericType<0>> =
                        ArrayColumnBuilder::with_capacity(len.unwrap_or(1), 0, ctx.generics);

                    for idx in 0..(len.unwrap_or(1)) {
                        for arg in args {
                            match arg {
                                Value::Scalar(scalar) => {
                                    builder.put_item(scalar.as_ref());
                                }
                                Value::Column(col) => unsafe {
                                    builder.put_item(col.index_unchecked(idx));
                                },
                            }
                        }
                        builder.commit_row();
                    }

                    match len {
                        Some(_) => Value::Column(Column::Array(Box::new(
                            builder
                                .build()
                                .upcast(&DataType::Array(Box::new(DataType::Generic(0)))),
                        ))),
                        None => Value::Scalar(Scalar::Array(builder.build_scalar())),
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("array", array_factory);

    // Returns a merged array of tuples in which the nth tuple contains all nth values of input arrays.
    let array_zip_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        let args_type = args_type.to_vec();

        let inner_types: Vec<DataType> = args_type
            .iter()
            .map(|arg_type| {
                let is_nullable = arg_type.is_nullable();
                match arg_type.remove_nullable() {
                    DataType::Array(box inner_type) => {
                        if is_nullable {
                            inner_type.wrap_nullable()
                        } else {
                            inner_type.clone()
                        }
                    }
                    _ => arg_type.clone(),
                }
            })
            .collect();
        let return_type = DataType::Array(Box::new(DataType::Tuple(inner_types.clone())));
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "arrays_zip".to_string(),
                args_type: args_type.clone(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, args_domain| {
                    let inner_domains = args_domain
                        .iter()
                        .map(|arg_domain| match arg_domain {
                            Domain::Nullable(nullable_domain) => match &nullable_domain.value {
                                Some(box Domain::Array(Some(inner_domain))) => {
                                    Domain::Nullable(NullableDomain {
                                        has_null: nullable_domain.has_null,
                                        value: Some(Box::new(*inner_domain.clone())),
                                    })
                                }
                                _ => Domain::Nullable(nullable_domain.clone()),
                            },
                            Domain::Array(Some(box inner_domain)) => inner_domain.clone(),
                            _ => arg_domain.clone(),
                        })
                        .collect();
                    FunctionDomain::Domain(Domain::Array(Some(Box::new(Domain::Tuple(
                        inner_domains,
                    )))))
                }),
                eval: scalar_evaluator(move |args, ctx| {
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });

                    let mut offset = 0;
                    let mut offsets = Vec::new();
                    offsets.push(0);
                    let tuple_type = DataType::Tuple(inner_types.clone());
                    let mut builder = ColumnBuilder::with_capacity(&tuple_type, 0);
                    for i in 0..len.unwrap_or(1) {
                        let mut is_diff_len = false;
                        let mut array_len = None;
                        for arg in args {
                            let value = unsafe { arg.index_unchecked(i) };
                            if let ScalarRef::Array(col) = value {
                                if let Some(array_len) = array_len {
                                    if array_len != col.len() {
                                        is_diff_len = true;
                                        let err = format!(
                                            "array length must be equal, but got {} and {}",
                                            array_len,
                                            col.len()
                                        );
                                        ctx.set_error(builder.len(), err);
                                        offsets.push(offset);
                                        break;
                                    }
                                } else {
                                    array_len = Some(col.len());
                                }
                            }
                        }
                        if is_diff_len {
                            continue;
                        }
                        let array_len = array_len.unwrap_or(1);
                        for j in 0..array_len {
                            let mut tuple_values = Vec::with_capacity(args.len());
                            for arg in args {
                                let value = unsafe { arg.index_unchecked(i) };
                                match value {
                                    ScalarRef::Array(col) => {
                                        let tuple_value = unsafe { col.index_unchecked(j) };
                                        tuple_values.push(tuple_value.to_owned());
                                    }
                                    _ => {
                                        tuple_values.push(value.to_owned());
                                    }
                                }
                            }
                            let tuple_value = Scalar::Tuple(tuple_values);
                            builder.push(tuple_value.as_ref());
                        }
                        offset += array_len as u64;
                        offsets.push(offset);
                    }

                    match len {
                        Some(_) => {
                            let array_column = ArrayColumn::new(builder.build(), offsets.into());
                            Value::Column(Column::Array(Box::new(array_column)))
                        }
                        _ => Value::Scalar(Scalar::Array(builder.build())),
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("arrays_zip", array_zip_factory);

    registry.register_1_arg::<EmptyArrayType, NumberType<u8>, _>(
        "length",
        |_, _| FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 }),
        |_, _| 0u8,
    );

    registry.register_2_arg::<NumberType<i64>, NumberType<i64>, ArrayType<NumberType<i64>>, _>(
        "range",
        |_, _, _| FunctionDomain::Full,
        |start, end, ctx| {
            const MAX: i64 = 500000000;
            if end - start > MAX {
                // the same behavior as Clickhouse
                ctx.set_error(
                    0,
                    format!(
                        "the allowed maximum values of range function is {}, but got {}",
                        MAX,
                        end - start
                    ),
                );
                return vec![].into();
            }
            (start..end).collect()
        },
    );

    registry.register_3_arg::<NumberType<i64>, NumberType<i64>, NumberType<i64>, ArrayType<NumberType<i64>>, _, _>(
        "range",
        |_, _, _, _| FunctionDomain::Full,
        |start, end, step, ctx| {
            const MAX: i64 = 500000000;
            if step == 0 {
                ctx.set_error(
                    0,
                    "step cannot be zero".to_string()
                );
                return vec![].into();
            }
            let len = ((end - start) / step).abs();
            if len > MAX {
                // the same behavior as Clickhouse
                ctx.set_error(
                    0,
                    format!(
                        "the allowed maximum values of range function is {}, but got {}",
                        MAX,
                        len
                    ),
                );
                return vec![].into();
            }
            let mut vals = Vec::with_capacity(len as usize);
            let mut num = start;
            if step > 0 {
                while num < end {
                    vals.push(num);
                    num += step;
                }
            } else {
                while num > end {
                    vals.push(num);
                    num += step;
                }
            }
            vals.into()
        },
    );

    registry.register_1_arg::<ArrayType<GenericType<0>>, NumberType<u64>, _>(
        "length",
        |_, _| FunctionDomain::Full,
        |arr, _| arr.len() as u64,
    );

    registry.register_2_arg_core::<NullableType<EmptyArrayType>, NullableType<UInt64Type>, NullType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_2_arg_core::<NullableType<ArrayType<NullType>>, NullableType<UInt64Type>, NullType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    registry.register_combine_nullable_2_arg::<ArrayType<NullableType<GenericType<0>>>, UInt64Type, GenericType<0>, _, _>(
        "get",
        |_, domain, _| FunctionDomain::Domain(NullableDomain {
            has_null: true,
            value: domain.as_ref().and_then(|domain| domain.value.clone()),
        }),
        vectorize_with_builder_2_arg::<ArrayType<NullableType<GenericType<0>>>, UInt64Type, NullableType<GenericType<0>>>(
            |arr, idx, output, _| {
                if idx == 0 {
                    output.push_null();
                } else {
                    match arr.index(idx as usize - 1) {
                        Some(Some(item)) => output.push(item),
                        _ => output.push_null(),
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, UInt64Type, _, _>(
        "array_indexof",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, UInt64Type>(
            |arr, val, _| {
                arr.iter().position(|item| item == val).map(|pos| pos+1).unwrap_or(0) as u64
            },
        ),
    );

    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, EmptyArrayType, _>(
        "array_concat",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| (),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_concat",
        |_, domain1, domain2| {
            FunctionDomain::Domain(
                match (domain1, domain2) {
                    (Some(domain1), Some(domain2)) => Some(domain1.merge(domain2)),
                    (Some(domain1), None) => Some(domain1).cloned(),
                    (None, Some(domain2)) => Some(domain2).cloned(),
                    (None, None) => None,
                }
            )
        },
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |lhs, rhs, output, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                output.builder.append_column(&lhs);
                output.builder.append_column(&rhs);
                output.commit_row()
            }
        ),
    );

    registry
        .scalar_builder("array_flatten")
        .function()
        .typed_1_arg::<ArrayType<ArrayType<GenericType<0>>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone().flatten()))
        .vectorized(vectorize_1_arg::<
            ArrayType<ArrayType<GenericType<0>>>,
            ArrayType<GenericType<0>>,
        >(|arr, ctx| {
            let mut builder = ColumnBuilder::with_capacity(&ctx.generics[0], arr.len());
            for v in arr.iter() {
                builder.append_column(&v);
            }
            builder.build()
        }))
        .register();

    registry
        .register_passthrough_nullable_2_arg::<ArrayType<StringType>, StringType, StringType, _, _>(
            "array_to_string",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<ArrayType<StringType>, StringType, StringType>(
                |lhs, rhs, output, ctx| {
                    if let Some(validity) = &ctx.validity
                        && !validity.get_bit(output.len())
                    {
                        output.commit_row();
                        return;
                    }
                    for (i, d) in lhs.iter().enumerate() {
                        if i != 0 {
                            output.put_str(rhs);
                        }
                        output.put_str(d);
                    }
                    output.commit_row();
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<ArrayType<NullableType<StringType>>, StringType, StringType, _, _>(
        "array_to_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<NullableType<StringType>>, StringType, StringType>(
            |lhs, rhs, output, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                for (i, d) in lhs.iter().filter(|x| x.is_some()).enumerate() {
                    if i != 0  {
                        output.put_str(rhs);
                    }
                    output.put_str(d.unwrap());
                }
                output.commit_row();
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<EmptyArrayType, Int64Type, EmptyArrayType, _, _>(
            "slice",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<EmptyArrayType, Int64Type, EmptyArrayType>(
                |_, _, output, _| {
                    *output += 1;
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, Int64Type, ArrayType<GenericType<0>>, _, _>(
        "slice",
        |_, domain, _| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, Int64Type, ArrayType<GenericType<0>>>(
            |arr, start, output, _| {
                let start = if start == 0 {
                    0
                } else if start > 0 {
                    start as usize - 1
                } else {
                    let start = arr.len() as i64 + start;
                    if start >= 0 {
                        start as usize
                    } else {
                        0
                    }
                };
                if arr.len() == 0 || start >= arr.len() {
                    output.push_default();
                } else {
                    let range = Range { start, end: arr.len() };
                    let arr_slice = arr.slice(range);
                    output.push(arr_slice);
                }
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<EmptyArrayType, Int64Type, Int64Type, EmptyArrayType, _, _>(
        "slice",
        |_, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_3_arg::<EmptyArrayType, Int64Type, Int64Type, EmptyArrayType>(
            |_, _, _, output, _| {
                *output += 1;
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<ArrayType<GenericType<0>>, Int64Type, Int64Type, ArrayType<GenericType<0>>, _, _>(
        "slice",
        |_, domain, _, _| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_3_arg::<ArrayType<GenericType<0>>, Int64Type, Int64Type, ArrayType<GenericType<0>>>(
            |arr, start, end, output, _| {
                let start = if start == 0 {
                    0
                } else if start > 0 {
                    start as usize - 1
                } else {
                    let start = arr.len() as i64 + start;
                    if start >= 0 {
                        start as usize
                    } else {
                        0
                    }
                };
                let end = if end >= 0 {
                    end as usize
                } else {
                    let end = arr.len() as i64 + end + 1;
                    if end >= 0 {
                        end as usize
                    } else {
                        0
                    }
                };

                if arr.len() == 0 || start >= arr.len() || start >= end {
                    output.push_default();
                } else {
                    let range = if end < arr.len() {
                        Range { start, end }
                    } else {
                        Range { start, end: arr.len() }
                    };
                    let arr_slice = arr.slice(range);
                    output.push(arr_slice);
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, ArrayType<GenericType<0>>, _, _>(
        "array_remove",
        |_, domain, _| FunctionDomain::Domain(domain.clone()),
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, GenericType<0>, ArrayType<GenericType<0>>>(
            |arr, item, output, _| {
                let mut builder = ColumnBuilder::with_capacity(&arr.data_type(), arr.len());
                for val in arr.iter() {
                    if !val.eq(&item) {
                        builder.push(val);
                    }
                }
                output.push(builder.build());
            }
        ),
    );

    registry
        .scalar_builder("array_remove_first")
        .function()
        .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(
            vectorize_with_builder_1_arg::<EmptyArrayType, EmptyArrayType>(|_, output, _| {
                *output += 1;
            }),
        )
        .register();

    registry
        .scalar_builder("array_remove_first")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone()))
        .vectorized(vectorize_with_builder_1_arg::<
            ArrayType<GenericType<0>>,
            ArrayType<GenericType<0>>,
        >(|arr, output, _| {
            if arr.len() <= 1 {
                output.push_default();
            } else {
                let range = Range {
                    start: 1,
                    end: arr.len(),
                };
                let arr_slice = arr.slice(range);
                output.push(arr_slice);
            }
        }))
        .register();

    registry
        .scalar_builder("array_remove_last")
        .function()
        .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(
            vectorize_with_builder_1_arg::<EmptyArrayType, EmptyArrayType>(|_, output, _| {
                *output += 1;
            }),
        )
        .register();

    registry
        .scalar_builder("array_remove_last")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone()))
        .vectorized(vectorize_with_builder_1_arg::<
            ArrayType<GenericType<0>>,
            ArrayType<GenericType<0>>,
        >(|arr, output, _| {
            if arr.len() <= 1 {
                output.push_default();
            } else {
                let range = Range {
                    start: 0,
                    end: arr.len() - 1,
                };
                let arr_slice = arr.slice(range);
                output.push(arr_slice);
            }
        }))
        .register();

    registry
        .scalar_builder("array_reverse")
        .function()
        .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_1_arg::<EmptyArrayType, EmptyArrayType>(
            |arr, _| arr,
        ))
        .register();

    registry
        .scalar_builder("array_reverse")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone()))
        .vectorized(vectorize_with_builder_1_arg::<
            ArrayType<GenericType<0>>,
            ArrayType<GenericType<0>>,
        >(|arr, output, _| {
            let mut vals = Vec::with_capacity(arr.len());
            let mut builder = ColumnBuilder::with_capacity(&arr.data_type(), arr.len());
            for val in arr.iter() {
                vals.push(val);
            }
            for val in vals.into_iter().rev() {
                builder.push(val);
            }
            output.push(builder.build());
        }))
        .register();

    registry.register_2_arg_core::<GenericType<0>, NullableType<ArrayType<GenericType<0>>>, ArrayType<GenericType<0>>, _, _>(
        "array_prepend",
        |_, item_domain, array_domain| {
            let domain = array_domain
                .value
                .as_ref()
                .map(|box inner_domain| {
                    inner_domain
                        .as_ref()
                        .map(|inner_domain| inner_domain.merge(item_domain))
                        .unwrap_or(item_domain.clone())
                });
            FunctionDomain::Domain(domain)
        },
        vectorize_with_builder_2_arg::<GenericType<0>, NullableType<ArrayType<GenericType<0>>>, ArrayType<GenericType<0>>>(
            |val, arr, output, _| {
                output.put_item(val);
                if let Some(arr) = arr {
                    for item in arr.iter() {
                        output.put_item(item);
                    }
                }
                output.commit_row()
        })
    );

    registry.register_2_arg_core::<NullableType<ArrayType<GenericType<0>>>, GenericType<0>, ArrayType<GenericType<0>>, _, _>(
        "array_append",
        |_, array_domain, item_domain| {
            let domain = array_domain
                .value
                .as_ref()
                .map(|box inner_domain| {
                    inner_domain
                        .as_ref()
                        .map(|inner_domain| inner_domain.merge(item_domain))
                        .unwrap_or(item_domain.clone())
                });
            FunctionDomain::Domain(domain)
        },
        vectorize_with_builder_2_arg::<NullableType<ArrayType<GenericType<0>>>, GenericType<0>, ArrayType<GenericType<0>>>(
            |arr, val, output, _| {
                if let Some(arr) = arr {
                    for item in arr.iter() {
                        output.put_item(item);
                    }
                }
                output.put_item(val);
                output.commit_row()
        })
    );

    fn eval_contains<T: ArgType>(lhs: Value<ArrayType<T>>, rhs: Value<T>) -> Value<BooleanType>
    where T::Scalar: HashtableKeyable {
        match lhs {
            Value::Scalar(array) => {
                let mut set = StackHashSet::<_, 128>::with_capacity(T::column_len(&array));
                for val in T::iter_column(&array) {
                    let _ = set.set_insert(T::to_owned_scalar(val));
                }
                match rhs {
                    Value::Scalar(c) => Value::Scalar(set.contains(&c)),
                    Value::Column(col) => {
                        let result = BooleanType::column_from_iter(
                            T::iter_column(&col).map(|c| set.contains(&T::to_owned_scalar(c))),
                            &[],
                        );
                        Value::Column(result)
                    }
                }
            }
            Value::Column(array_column) => {
                let result = match rhs {
                    Value::Scalar(c) => BooleanType::column_from_iter(
                        array_column
                            .iter()
                            .map(|array| T::iter_column(&array).contains(&T::to_scalar_ref(&c))),
                        &[],
                    ),
                    Value::Column(col) => BooleanType::column_from_iter(
                        array_column
                            .iter()
                            .zip(T::iter_column(&col))
                            .map(|(array, c)| {
                                T::iter_column(&array).any(|v| T::equal(v, c.clone()))
                            }),
                        &[],
                    ),
                };
                Value::Column(result)
            }
        }
    }

    for left in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_2_arg::<ArrayType<NumberType<NUM_TYPE>>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                    "contains",
                    |_, lhs, rhs| {
                        lhs.as_ref().map(|lhs| {
                            lhs.domain_contains(rhs)
                        }).unwrap_or(FunctionDomain::Full)
                    },
                    |lhs, rhs, _| eval_contains::<NumberType<NUM_TYPE>>(lhs, rhs)
                );
            }
        });
    }

    registry.register_passthrough_nullable_2_arg::<ArrayType<StringType>, StringType, BooleanType, _, _>(
        "contains",
        |_, lhs, rhs| {
            lhs.as_ref().map(|lhs| {
                lhs.domain_contains(rhs)
            }).unwrap_or(FunctionDomain::Full)
        },
        |lhs, rhs, _| {
            match lhs {
                Value::Scalar(array) => {
                    let mut set = StackHashSet::<_, 128>::with_capacity(StringType::column_len(&array));
                    for val in array.iter() {
                        let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                        let _ = set.set_insert(key_ref);
                    }
                    match rhs {
                        Value::Scalar(val) =>  {
                            let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                            Value::Scalar(set.contains( &key_ref))
                        },
                        Value::Column(col) => {
                            let result = BooleanType::column_from_iter(StringType::iter_column(&col).map(|val| {
                                let key_ref = KeysRef::create(val.as_ptr() as usize, val.len());
                                set.contains(&key_ref)
                            }), &[]);
                            Value::Column(result)
                        }
                    }
                },
                Value::Column(array_column) => {
                    let result = match rhs {
                        Value::Scalar(c) => BooleanType::column_from_iter(array_column
                            .iter()
                            .map(|array| StringType::iter_column(&array).contains(&c.as_str())), &[]),
                        Value::Column(col) => BooleanType::column_from_iter(array_column
                            .iter()
                            .zip(StringType::iter_column(&col))
                            .map(|(array, c)| StringType::iter_column(&array).contains(&c)), &[]),
                    };
                    Value::Column(result)
                }
            }
        }
    );

    registry
        .register_passthrough_nullable_2_arg::<ArrayType<DateType>, DateType, BooleanType, _, _>(
            "contains",
            |_, lhs, rhs| {
                let has_true = lhs.is_some_and(|lhs| !(lhs.min > rhs.max || lhs.max < rhs.min));
                FunctionDomain::Domain(BooleanDomain {
                    has_false: true,
                    has_true,
                })
            },
            |lhs, rhs, _| eval_contains::<DateType>(lhs, rhs),
        );

    registry.register_passthrough_nullable_2_arg::<ArrayType<TimestampType>, TimestampType, BooleanType, _, _>(
        "contains",
        |_, lhs, rhs| {
            let has_true = lhs.is_some_and(|lhs| !(lhs.min > rhs.max || lhs.max < rhs.min));
            FunctionDomain::Domain(BooleanDomain {
                has_false: true,
                has_true,
            })
        },
        |lhs, rhs, _| eval_contains::<TimestampType>(lhs, rhs)
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<BooleanType>, BooleanType, BooleanType, _, _>(
        "contains",
        |_, lhs, rhs| {
            match lhs {
                Some(lhs) => {
                    FunctionDomain::Domain(BooleanDomain {
                        has_false: (lhs.has_false && rhs.has_true) || (lhs.has_true && rhs.has_false),
                        has_true: (lhs.has_false && rhs.has_false) || (lhs.has_true && rhs.has_true),
                    })
                },
                None => FunctionDomain::Domain(BooleanDomain {
                    has_false: false,
                    has_true: false,
                }),
            }
        },
        |lhs, rhs, _| {
            match lhs {
                Value::Scalar(array) => {
                    let mut set = StackHashSet::<_, 128>::with_capacity(BooleanType::column_len(&array));
                    for val in array.iter() {
                        let _ = set.set_insert(val as u8);
                    }
                    match rhs {
                        Value::Scalar(val) =>  {
                            Value::Scalar(set.contains(&(val as u8)))
                        },
                        Value::Column(col) => {
                            let result = BooleanType::column_from_iter(BooleanType::iter_column(&col).map(|val| {
                                set.contains(&(val as u8))
                            }), &[]);
                            Value::Column(result)
                        }
                    }
                },
                Value::Column(array_column) => {
                    let result = match rhs {
                        Value::Scalar(c) => BooleanType::column_from_iter(array_column
                            .iter()
                            .map(|array| BooleanType::iter_column(&array).contains(&c)), &[]),
                        Value::Column(col) => BooleanType::column_from_iter(array_column
                            .iter()
                            .zip(BooleanType::iter_column(&col))
                            .map(|(array, c)| BooleanType::iter_column(&array).contains(&c)), &[]),
                    };
                    Value::Column(result)
                }
            }
        }
    );

    registry.register_2_arg_core::<NullableType<ArrayType<GenericType<0>>>, GenericType<0>, BooleanType, _, _>(
        "contains",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<NullableType<ArrayType<GenericType<0>>>, GenericType<0>, BooleanType>(
            |lhs, rhs, _| {
                lhs.map(|col| col.iter().contains(&rhs)).unwrap_or(false)
            }
        )
    );

    registry
        .scalar_builder("array_unique")
        .function()
        .typed_1_arg::<EmptyArrayType, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Domain(SimpleDomain { min: 0, max: 0 }))
        .vectorized(vectorize_1_arg::<EmptyArrayType, UInt64Type>(|_, _| 0))
        .register();

    registry
        .scalar_builder("array_unique")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_1_arg::<ArrayType<GenericType<0>>, UInt64Type>(
            |arr, _| {
                if arr.len() > 0 {
                    let mut set: StackHashSet<u128, 16> = StackHashSet::with_capacity(arr.len());
                    for val in arr.iter() {
                        if val == ScalarRef::Null {
                            continue;
                        }
                        let mut hasher = SipHasher24::new();
                        val.hash(&mut hasher);
                        let hash128 = hasher.finish128();
                        let _ = set.set_insert(hash128.into());
                    }
                    set.len() as u64
                } else {
                    0
                }
            },
        ))
        .register();

    registry
        .scalar_builder("array_distinct")
        .function()
        .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_1_arg::<EmptyArrayType, EmptyArrayType>(
            |arr, _| arr,
        ))
        .register();

    registry
        .scalar_builder("array_distinct")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone()))
        .vectorized(vectorize_1_arg::<
            ArrayType<GenericType<0>>,
            ArrayType<GenericType<0>>,
        >(|arr, _| {
            if arr.len() > 0 {
                let data_type = arr.data_type();
                let mut builder = ColumnBuilder::with_capacity(&data_type, arr.len());
                let mut set: StackHashSet<u128, 16> = StackHashSet::with_capacity(arr.len());
                for val in arr.iter() {
                    if val == ScalarRef::Null {
                        continue;
                    }
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();
                    if !set.contains(&key) {
                        let _ = set.set_insert(key);
                        builder.push(val);
                    }
                }
                builder.build()
            } else {
                arr
            }
        }))
        .register();

    registry
        .scalar_builder("array_compact")
        .function()
        .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_1_arg::<EmptyArrayType, EmptyArrayType>(
            |arr, _| arr,
        ))
        .register();

    registry
        .scalar_builder("array_compact")
        .function()
        .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| FunctionDomain::Domain(domain.clone()))
        .vectorized(vectorize_with_builder_1_arg::<
            ArrayType<GenericType<0>>,
            ArrayType<GenericType<0>>,
        >(|arr, output, _| {
            let builder = &mut output.builder;
            for val in arr.iter() {
                if val == ScalarRef::Null {
                    continue;
                }
                builder.push(val);
            }
            output.commit_row()
        }))
        .register();

    registry.register_passthrough_nullable_2_arg::<EmptyArrayType, EmptyArrayType, EmptyArrayType, _, _>(
        "array_intersection",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<EmptyArrayType, EmptyArrayType, EmptyArrayType>(|arr, _, _| arr),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_intersection",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |left, right, output, _| {
                let mut map: StackHashMap<u128, usize, 16> = StackHashMap::with_capacity(right.len());
                let builder = &mut output.builder;
                for val in right.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();
                    unsafe { match map.insert_and_entry(key) {
                        Ok(e) => {
                            let v = e.get_mut();
                            *v += 1;
                        }
                        Err(e) => {
                            let v = e.get_mut();
                            *v += 1;
                        }
                    }
                    }
                }

                for val in left.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();

                    if let Some(v) = map.get_mut(&key)
                        && *v > 0 {
                            *v -= 1;
                            builder.push(val);
                        }
                }
                output.commit_row()
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<EmptyArrayType, EmptyArrayType, EmptyArrayType, _, _>(
        "array_except",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<EmptyArrayType, EmptyArrayType, EmptyArrayType>(|arr, _, _| arr),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
        "array_except",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>(
            |left, right, output, _| {
                let mut map: StackHashMap<u128, usize, 16> = StackHashMap::with_capacity(right.len());
                let builder = &mut output.builder;
                for val in right.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();
                    unsafe { match map.insert_and_entry(key) {
                        Ok(e) => {
                            let v = e.get_mut();
                            *v += 1;
                        }
                        Err(e) => {
                            let v = e.get_mut();
                            *v += 1;
                        }
                    }
                    }
                }

                for val in left.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();

                    if let Some(v) = map.get_mut(&key)
                        && *v > 0 {
                            *v -= 1;
                            continue;
                        }
                    builder.push(val);
                }
                output.commit_row()
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
            "array_overlap",
            |_, _, _| FunctionDomain::Full,
            vectorize_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType>(|_, _, _| false),
        );

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
        "array_overlap",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType>(
            |left, right, output, _| {
                let mut set: StackHashSet<u128, 16> = StackHashSet::with_capacity(right.len());
                for val in right.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();
                    let _ = set.set_insert(key);
                }

                let mut res = false;
                for val in left.iter() {
                    let mut hasher = SipHasher24::new();
                    val.hash(&mut hasher);
                    let hash128 = hasher.finish128();
                    let key = hash128.into();

                    if set.contains(&key) {
                        res = true;
                        break;
                    }
                }
                output.push(res);
            },
        ),
    );
}

struct ArrayAggEvaluator<'a> {
    func: &'a AggregateFunctionRef,
    state_layout: &'a StatesLayout,
    addr: StateAddr,
    need_manual_drop_state: bool,
    _arena: Bump,
}

impl<'a> ArrayAggEvaluator<'a> {
    fn new(func: &'a AggregateFunctionRef, state_layout: &'a StatesLayout) -> Self {
        let arena = Bump::new();
        let addr = arena.alloc_layout(state_layout.layout).into();
        func.init_state(AggrState::new(addr, &state_layout.states_loc[0]));
        Self {
            state_layout,
            addr,
            need_manual_drop_state: func.need_manual_drop_state(),
            func,
            _arena: arena,
        }
    }

    fn state(&self) -> AggrState<'_> {
        AggrState::new(self.addr, &self.state_layout.states_loc[0])
    }

    fn eval(&mut self, entry: BlockEntry, builder: &mut ColumnBuilder) -> Result<()> {
        let state = self.state();
        if self.need_manual_drop_state {
            unsafe {
                self.func.drop_state(state);
            }
        }
        self.func.init_state(state);
        let rows = entry.len();
        let entries = &[entry];
        self.func.accumulate(state, entries.into(), None, rows)?;
        self.func.merge_result(state, false, builder)?;
        Ok(())
    }
}

impl Drop for ArrayAggEvaluator<'_> {
    fn drop(&mut self) {
        if !self.need_manual_drop_state {
            return;
        }
        drop_guard(move || unsafe {
            self.func.drop_state(self.state());
        })
    }
}

struct ArrayAggDesc {
    func: AggregateFunctionRef,
    state_layout: Arc<StatesLayout>,
    return_type: DataType,
}

impl ArrayAggDesc {
    fn new(name: &str, array_type: &DataType) -> Result<Self> {
        let factory = AggregateFunctionFactory::instance();
        let func = factory.get(name, vec![], vec![array_type.clone()], vec![])?;
        let return_type = func.return_type()?;
        let funcs = [func.clone()];
        let state_layout = Arc::new(get_states_layout(&funcs)?);
        Ok(Self {
            func,
            state_layout,
            return_type,
        })
    }

    fn create_evaluator(&self) -> ArrayAggEvaluator<'_> {
        ArrayAggEvaluator::new(&self.func, &self.state_layout)
    }
}

struct ArrayAggFunctionImpl {
    desc: Option<ArrayAggDesc>,
    return_type: DataType,
}

impl ArrayAggFunctionImpl {
    fn new(name: &'static str, arg_type: &DataType) -> Option<Self> {
        let (desc, return_type) = match arg_type {
            DataType::Nullable(box DataType::EmptyArray) | DataType::EmptyArray => (
                None,
                if name == "count" {
                    UInt64Type::data_type()
                } else {
                    DataType::Null
                },
            ),
            DataType::Nullable(box DataType::Array(box array_type))
            | DataType::Array(box array_type)
            | DataType::Nullable(box array_type @ DataType::Variant)
            | array_type @ DataType::Variant => {
                let desc = ArrayAggDesc::new(name, array_type).ok()?;
                let return_type = desc.return_type.clone();
                (Some(desc), return_type)
            }
            _ => return None,
        };
        Some(Self {
            desc,
            return_type: if arg_type.is_nullable() {
                return_type.wrap_nullable()
            } else {
                return_type
            },
        })
    }

    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let Some(desc) = &self.desc else {
            return match args {
                [_] => Value::Scalar(Scalar::default_value(&self.return_type)),
                _ => unreachable!(),
            };
        };

        match args {
            [Value::Scalar(Scalar::Null | Scalar::EmptyArray)] => {
                Value::Scalar(Scalar::default_value(&self.return_type))
            }
            [Value::Scalar(scalar @ Scalar::Array(_) | scalar @ Scalar::Variant(_))] => {
                scalar_to_array_column(scalar.as_ref())
                    .and_then(|col| {
                        let mut evaluator = desc.create_evaluator();
                        let mut builder = ColumnBuilder::with_capacity(&desc.return_type, 1);
                        evaluator.eval(col.into(), &mut builder)?;
                        Ok(Value::Scalar(builder.build_scalar()))
                    })
                    .unwrap_or_else(|err| {
                        ctx.set_error(0, err.to_string());
                        Value::Scalar(Scalar::default_value(&self.return_type))
                    })
            }
            [Value::Scalar(_)] => unreachable!(),
            [Value::Column(Column::Nullable(box column))]
                if desc.return_type != self.return_type =>
            {
                let mut builder = ColumnBuilder::with_capacity(&self.return_type, column.len());
                let mut evaluator = desc.create_evaluator();
                let ColumnBuilder::Nullable(box nullable) = &mut builder else {
                    unreachable!()
                };
                for (row_index, scalar) in column.iter().enumerate() {
                    let Some(scalar) = scalar else {
                        nullable.push_null();
                        continue;
                    };

                    let col = match scalar_to_array_column(scalar) {
                        Ok(col) => col,
                        Err(err) => {
                            ctx.set_error(row_index, err.to_string());
                            nullable.push_null();
                            continue;
                        }
                    };

                    nullable.validity.push(true);
                    if let Err(err) = evaluator.eval(col.into(), &mut nullable.builder) {
                        ctx.set_error(row_index, err.to_string());
                        if nullable.builder.len() == row_index {
                            nullable.builder.push_default();
                        }
                    }
                }
                Value::Column(builder.build())
            }
            [Value::Column(column)] => {
                let mut builder = ColumnBuilder::with_capacity(&self.return_type, column.len());
                let mut evaluator = desc.create_evaluator();
                for (row_index, scalar) in column.iter().enumerate() {
                    if scalar == ScalarRef::Null {
                        builder.push_default();
                        continue;
                    }

                    let col = match scalar_to_array_column(scalar) {
                        Ok(col) => col,
                        Err(err) => {
                            ctx.set_error(row_index, err.to_string());
                            builder.push_default();
                            continue;
                        }
                    };

                    if let Err(err) = evaluator.eval(col.into(), &mut builder) {
                        ctx.set_error(row_index, err.to_string());
                        if builder.len() == row_index {
                            builder.push_default();
                        }
                    }
                }
                Value::Column(builder.build())
            }
            _ => unreachable!(),
        }
    }
}

fn scalar_to_array_column(scalar: ScalarRef) -> Result<Column> {
    match scalar {
        ScalarRef::Array(col) => Ok(col.clone()),
        ScalarRef::Variant(val) => {
            let array_val = RawJsonb::new(val);
            match array_val.array_values() {
                Ok(vals_opt) => {
                    let vals = vals_opt.unwrap_or(vec![array_val.to_owned()]);
                    let variant_col = BinaryColumn::from_iter(vals.iter().map(|v| v.as_raw()));
                    Ok(Column::Variant(variant_col))
                }
                Err(err) => Err(ErrorCode::Internal(err.to_string())),
            }
        }
        _ => unreachable!(),
    }
}

fn register_array_aggr(registry: &mut FunctionRegistry) {
    for (fn_name, name) in ARRAY_AGGREGATE_FUNCTIONS {
        let factory = FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
            let [arg] = args_type else {
                return None;
            };
            let impl_info = ArrayAggFunctionImpl::new(name, arg)?;
            let return_type = impl_info.return_type.clone();
            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: fn_name.to_string(),
                    args_type: vec![arg.clone()],
                    return_type,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: domain_evaluator(move |_, _| FunctionDomain::MayThrow),
                    eval: scalar_evaluator(move |args, ctx| impl_info.eval(args, ctx)),
                    derive_stat: None,
                },
            }))
        }));
        registry.register_function_factory(fn_name, factory);
    }

    for (fn_name, sort_desc) in ARRAY_SORT_FUNCTIONS {
        registry
            .scalar_builder(*fn_name)
            .function()
            .typed_1_arg::<EmptyArrayType, EmptyArrayType>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::Full)
            .vectorized(vectorize_1_arg::<EmptyArrayType, EmptyArrayType>(
                |arr, _| arr,
            ))
            .register();

        registry
            .scalar_builder(*fn_name)
            .function()
            .typed_1_arg::<VariantType, VariantType>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::MayThrow)
            .vectorized(vectorize_with_builder_1_arg::<VariantType, VariantType>(
                |val, output, ctx| {
                    if let Some(validity) = &ctx.validity
                        && !validity.get_bit(output.len())
                    {
                        output.commit_row();
                        return;
                    }
                    let array_val = RawJsonb::new(val);
                    match array_val.array_values() {
                        Ok(vals_opt) => {
                            let vals = vals_opt.unwrap_or(vec![array_val.to_owned()]);
                            let variant_col =
                                BinaryColumn::from_iter(vals.iter().map(|v| v.as_raw()));
                            let len = variant_col.len();
                            let col = BlockEntry::Column(Column::Variant(variant_col));
                            let sort_desc = vec![SortColumnDescription {
                                offset: 0,
                                asc: sort_desc.0,
                                nulls_first: sort_desc.1,
                            }];
                            match DataBlock::sort(&DataBlock::new(vec![col], len), &sort_desc, None)
                            {
                                Ok(block) => {
                                    let sorted_arr =
                                        block.columns()[0].value().into_column().unwrap();
                                    let mut sorted_vals = Vec::with_capacity(sorted_arr.len());
                                    for scalar in sorted_arr.iter() {
                                        let val = scalar.as_variant().unwrap();
                                        sorted_vals.push(RawJsonb::new(val));
                                    }
                                    match OwnedJsonb::build_array(sorted_vals.into_iter()) {
                                        Ok(owned_jsonb) => {
                                            output.put_slice(owned_jsonb.as_ref());
                                        }
                                        Err(err) => {
                                            ctx.set_error(output.len(), err.to_string());
                                        }
                                    }
                                }
                                Err(err) => {
                                    ctx.set_error(output.len(), err.to_string());
                                }
                            }
                        }
                        Err(err) => {
                            ctx.set_error(output.len(), err.to_string());
                        }
                    }
                    output.commit_row();
                },
            ))
            .register();

        registry
            .scalar_builder(*fn_name)
            .function()
            .typed_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::MayThrow)
            .vectorized(vectorize_with_builder_1_arg::<
                ArrayType<GenericType<0>>,
                ArrayType<GenericType<0>>,
            >(|arr, output, ctx| {
                let len = arr.len();
                let sort_desc = vec![SortColumnDescription {
                    offset: 0,
                    asc: sort_desc.0,
                    nulls_first: sort_desc.1,
                }];
                match DataBlock::sort(&DataBlock::new(vec![arr.into()], len), &sort_desc, None) {
                    Ok(block) => {
                        let sorted_arr = block.columns()[0].value().into_column().unwrap();
                        output.push(sorted_arr);
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push_default();
                    }
                }
            }))
            .register();
    }
}
