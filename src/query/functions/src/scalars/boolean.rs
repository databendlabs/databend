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

#![allow(unused_comparisons)]
#![allow(clippy::absurd_extreme_comparisons)]

use std::sync::Arc;

use databend_common_base::base::OrderedFloat;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::error_to_null;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::boolean;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::with_float_mapped_type;
use databend_common_expression::with_integer_mapped_type;

pub fn register(registry: &mut FunctionRegistry) {
    let fn_filters_factory = |func_name: &'static str| {
        FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
            if args_type.len() < 2 {
                return None;
            }
            if args_type.iter().any(|arg_type| {
                !arg_type.is_null() && arg_type.remove_nullable() != DataType::Boolean
            }) {
                return None;
            }

            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: func_name.to_string(),
                    args_type: args_type.to_vec(),
                    return_type: DataType::Boolean,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: domain_evaluator(move |_, _| {
                        unreachable!(
                            "cal_domain of `{func_name}` should be handled by the `Evaluator`"
                        )
                    }),
                    eval: scalar_evaluator(move |_, _| {
                        unreachable!("`{func_name}` should be handled by the `Evaluator`")
                    }),
                    derive_stat: None,
                },
            }))
        }))
    };
    registry.register_function_factory("and_filters", fn_filters_factory("and_filters"));
    registry.register_function_factory("or_filters", fn_filters_factory("or_filters"));

    registry
        .scalar_builder("not")
        .function()
        .typed_1_arg::<BooleanType, BooleanType>()
        .passthrough_nullable()
        .calc_domain(|_, arg| {
            FunctionDomain::Domain(BooleanDomain {
                has_false: arg.has_true,
                has_true: arg.has_false,
            })
        })
        .vectorized(|val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(!scalar),
            Value::Column(column) => Value::Column(!&column),
        })
        .register();

    registry.register_2_arg_core::<BooleanType, BooleanType, BooleanType, _, _>(
        "and",
        |_, lhs, rhs| {
            FunctionDomain::Domain(BooleanDomain {
                has_false: lhs.has_false || rhs.has_false,
                has_true: lhs.has_true && rhs.has_true,
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (Value::Scalar(true), other) | (other, Value::Scalar(true)) => other.to_owned(),
            (Value::Scalar(false), _) | (_, Value::Scalar(false)) => Value::Scalar(false),
            (Value::Column(a), Value::Column(b)) => Value::Column(&a & &b),
        },
    );

    registry.register_2_arg_core::<BooleanType, BooleanType, BooleanType, _, _>(
        "or",
        |_, lhs, rhs| {
            FunctionDomain::Domain(BooleanDomain {
                has_false: lhs.has_false && rhs.has_false,
                has_true: lhs.has_true || rhs.has_true,
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (Value::Scalar(true), _) | (_, Value::Scalar(true)) => Value::Scalar(true),
            (Value::Scalar(false), other) | (other, Value::Scalar(false)) => other.to_owned(),
            (Value::Column(a), Value::Column(b)) => Value::Column(&a | &b),
        },
    );

    // https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
    registry.register_2_arg_core::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>, _, _>(
        "and",
        |_, lhs, rhs| {
            let lhs_has_null = lhs.has_null;
            let lhs_has_true = lhs.value.as_ref().map(|v| v.has_true).unwrap_or(false);
            let lhs_has_false = lhs.value.as_ref().map(|v| v.has_false).unwrap_or(false);

            let rhs_has_null = rhs.has_null;
            let rhs_has_true = rhs.value.as_ref().map(|v| v.has_true).unwrap_or(false);
            let rhs_has_false = rhs.value.as_ref().map(|v| v.has_false).unwrap_or(false);

            let (has_null, has_true, has_false) = if (!lhs_has_null && !lhs_has_true) || (!rhs_has_null && !rhs_has_true) {
                (false, false, true)
            } else {
                (
                    lhs_has_null || rhs_has_null,
                    lhs_has_true && rhs_has_true,
                    lhs_has_false || rhs_has_false
                )
            };

            let value = if has_true || has_false {
                Some(Box::new(BooleanDomain{
                    has_true,
                    has_false,
                }))
            } else {
                None
            };

             FunctionDomain::Domain(NullableDomain::<BooleanType> {
                    has_null,
                    value,
            })
        },
        // value = lhs & rhs,  valid = (lhs_v & rhs_v) | (!lhs & lhs_v) | (!rhs & rhs_v))
        vectorize_2_arg::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>>(|lhs, rhs, _| {
            match (lhs, rhs) {
                (Some(false), _) => Some(false),
                (_, Some(false))  => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None
             }
        }),
    );

    registry.register_2_arg_core::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>, _, _>(
        "or",
        |_, lhs, rhs| {
            let lhs_has_null = lhs.has_null;
            let lhs_has_true = lhs.value.as_ref().map(|v| v.has_true).unwrap_or(false);
            let lhs_has_false = lhs.value.as_ref().map(|v| v.has_false).unwrap_or(false);

            let rhs_has_null = rhs.has_null;
            let rhs_has_true = rhs.value.as_ref().map(|v| v.has_true).unwrap_or(false);
            let rhs_has_false = rhs.value.as_ref().map(|v| v.has_false).unwrap_or(false);

            let (has_null, has_true, has_false) = if (!lhs_has_null && !lhs_has_false) || (!rhs_has_null && !rhs_has_false) {
                (false, true, false)
            } else {
                (
                    lhs_has_null || rhs_has_null,
                    lhs_has_true || rhs_has_true,
                    lhs_has_false && rhs_has_false
                )
            };

            let value = if has_true || has_false {
                Some(Box::new(BooleanDomain{
                    has_true,
                    has_false,
                }))
            } else {
                None
            };

            FunctionDomain::Domain(NullableDomain::<BooleanType> {
                    has_null,
                    value,
            })
        },
        // value = lhs | rhs,  valid = (lhs_v & rhs_v) | (lhs_v & lhs) | (rhs_v & rhs)
        vectorize_2_arg::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>>(|lhs, rhs, _| {
            match (lhs, rhs) {
                (Some(true), _) => Some(true),
                (_, Some(true))  => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "xor",
        |_, lhs, rhs| {
            FunctionDomain::Domain(BooleanDomain {
                has_false: (lhs.has_false && rhs.has_false) || (lhs.has_true && rhs.has_true),
                has_true: (lhs.has_false && rhs.has_true) || (lhs.has_true && rhs.has_false),
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (Value::Scalar(true), Value::Scalar(other))
            | (Value::Scalar(other), Value::Scalar(true)) => Value::Scalar(!other),
            (Value::Scalar(true), Value::Column(other))
            | (Value::Column(other), Value::Scalar(true)) => Value::Column(!&other),
            (Value::Scalar(false), other) | (other, Value::Scalar(false)) => other.to_owned(),
            (Value::Column(a), Value::Column(b)) => Value::Column(boolean::xor(&a, &b)),
        },
    );

    registry
        .scalar_builder("to_string")
        .function()
        .typed_1_arg::<BooleanType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(eval_boolean_to_string)
        .register();

    registry.register_combine_nullable_1_arg::<BooleanType, StringType, _, _>(
        "try_to_string",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_boolean_to_string),
    );

    registry
        .scalar_builder("to_boolean")
        .function()
        .typed_1_arg::<StringType, BooleanType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_string_to_boolean)
        .register();

    registry.register_combine_nullable_1_arg::<StringType, BooleanType, _, _>(
        "try_to_boolean",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_boolean),
    );

    registry.register_1_arg_core::<BooleanType, BooleanType, _, _>(
        "is_true",
        |_, domain| FunctionDomain::Domain(*domain),
        |val, _| val.to_owned(),
    );

    registry.register_1_arg_core::<NullableType<BooleanType>, BooleanType, _, _>(
        "is_true",
        |_, domain| {
            FunctionDomain::Domain(BooleanDomain {
                has_false: domain.has_null
                    || domain.value.as_ref().map(|v| v.has_false).unwrap_or(false),
                has_true: domain.value.as_ref().map(|v| v.has_true).unwrap_or(false),
            })
        },
        |val, _| match val {
            Value::Scalar(None) => Value::Scalar(false),
            Value::Scalar(Some(scalar)) => Value::Scalar(scalar),
            Value::Column(NullableColumn { column, validity }) => {
                Value::Column((&column) & (&validity))
            }
        },
    );

    for num_type in ALL_INTEGER_TYPES {
        with_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, BooleanType, _>(
                    "to_boolean",
                    |_, domain| {
                        FunctionDomain::Domain(BooleanDomain {
                            has_false: domain.min <= 0 && domain.max >= 0,
                            has_true: !(domain.min == 0 && domain.max == 0),
                        })
                    },
                    |val, _| val != 0,
                );

                registry
                    .register_combine_nullable_1_arg::<NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "try_to_boolean",
                        |_, domain| {
                            FunctionDomain::Domain(NullableDomain {
                                has_null: false,
                                value: Some(Box::new(BooleanDomain {
                                    has_false: domain.min <= 0 && domain.max >= 0,
                                    has_true: !(domain.min == 0 && domain.max == 0),
                                })),
                            })
                        },
                        vectorize_with_builder_1_arg::<
                            NumberType<NUM_TYPE>,
                            NullableType<BooleanType>,
                        >(|val, output, _| {
                            output.builder.push(val != 0);
                            output.validity.push(true);
                        }),
                    );

                let name = format!("to_{num_type}").to_lowercase();
                registry.register_1_arg::<BooleanType, NumberType<NUM_TYPE>, _>(
                    &name,
                    |_, domain| {
                        FunctionDomain::Domain(SimpleDomain {
                            min: if domain.has_false { 0 } else { 1 },
                            max: if domain.has_true { 1 } else { 0 },
                        })
                    },
                    |val, _| NUM_TYPE::from(val),
                );

                let name = format!("try_to_{num_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<BooleanType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, domain| {
                            FunctionDomain::Domain(NullableDomain {
                                has_null: false,
                                value: Some(Box::new(SimpleDomain {
                                    min: if domain.has_false { 0 } else { 1 },
                                    max: if domain.has_true { 1 } else { 0 },
                                })),
                            })
                        },
                        vectorize_with_builder_1_arg::<
                            BooleanType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(|val, output, _| {
                            output.push(NUM_TYPE::from(val));
                        }),
                    );
            }
            _ => unreachable!(),
        })
    }

    for num_type in ALL_FLOAT_TYPES {
        with_float_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, BooleanType, _>(
                    "to_boolean",
                    |_, domain| {
                        FunctionDomain::Domain(BooleanDomain {
                            has_false: domain.min <= OrderedFloat(0.0)
                                && domain.max >= OrderedFloat(0.0),
                            has_true: !(domain.min == OrderedFloat(0.0)
                                && domain.max == OrderedFloat(0.0)),
                        })
                    },
                    |val, _| val != OrderedFloat(0.0),
                );

                registry
                    .register_combine_nullable_1_arg::<NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "try_to_boolean",
                        |_, domain| {
                            FunctionDomain::Domain(NullableDomain {
                                has_null: false,
                                value: Some(Box::new(BooleanDomain {
                                    has_false: domain.min <= OrderedFloat(0.0)
                                        && domain.max >= OrderedFloat(0.0),
                                    has_true: !(domain.min == OrderedFloat(0.0)
                                        && domain.max == OrderedFloat(0.0)),
                                })),
                            })
                        },
                        vectorize_with_builder_1_arg::<
                            NumberType<NUM_TYPE>,
                            NullableType<BooleanType>,
                        >(|val, output, _| {
                            output.builder.push(val != OrderedFloat(0.0));
                            output.validity.push(true);
                        }),
                    );

                let name = format!("to_{num_type}").to_lowercase();
                registry.register_1_arg::<BooleanType, NumberType<NUM_TYPE>, _>(
                    &name,
                    |_, domain| {
                        FunctionDomain::Domain(SimpleDomain {
                            min: if domain.has_false {
                                OrderedFloat(0.0)
                            } else {
                                OrderedFloat(1.0)
                            },
                            max: if domain.has_true {
                                OrderedFloat(1.0)
                            } else {
                                OrderedFloat(0.0)
                            },
                        })
                    },
                    |val, _| {
                        if val {
                            NUM_TYPE::from(OrderedFloat(1.0))
                        } else {
                            NUM_TYPE::from(OrderedFloat(0.0))
                        }
                    },
                );

                let name = format!("try_to_{num_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<BooleanType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, domain| {
                            FunctionDomain::Domain(NullableDomain {
                                has_null: false,
                                value: Some(Box::new(SimpleDomain {
                                    min: if domain.has_false {
                                        OrderedFloat(0.0)
                                    } else {
                                        OrderedFloat(1.0)
                                    },
                                    max: if domain.has_true {
                                        OrderedFloat(1.0)
                                    } else {
                                        OrderedFloat(0.0)
                                    },
                                })),
                            })
                        },
                        vectorize_with_builder_1_arg::<
                            BooleanType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(|val, output, _| {
                            if val {
                                output.push(NUM_TYPE::from(OrderedFloat(1.0)))
                            } else {
                                output.push(NUM_TYPE::from(OrderedFloat(0.0)))
                            }
                        }),
                    );
            }
            _ => unreachable!(),
        })
    }
}

fn eval_boolean_to_string(val: Value<BooleanType>, ctx: &mut EvalContext) -> Value<StringType> {
    vectorize_with_builder_1_arg::<BooleanType, StringType>(|val, output, _| {
        output.put_and_commit(if val { "true" } else { "false" });
    })(val, ctx)
}

fn eval_string_to_boolean(val: Value<StringType>, ctx: &mut EvalContext) -> Value<BooleanType> {
    vectorize_with_builder_1_arg::<StringType, BooleanType>(|val, output, ctx| {
        if val.eq_ignore_ascii_case("true") {
            output.push(true);
        } else if val.eq_ignore_ascii_case("false") {
            output.push(false);
        } else {
            ctx.set_error(output.len(), "cannot parse to type `BOOLEAN`");
            output.push(false);
        }
    })(val, ctx)
}
