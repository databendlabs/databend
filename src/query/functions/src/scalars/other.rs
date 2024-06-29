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

use std::io::Write;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_base::base::uuid::Uuid;
use databend_common_expression::error_to_null;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use ordered_float::OrderedFloat;
use rand::Rng;
use rand::SeedableRng;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("inet_aton", &["ipv4_string_to_num"]);
    registry.register_aliases("try_inet_aton", &["try_ipv4_string_to_num"]);
    registry.register_aliases("inet_ntoa", &["ipv4_num_to_string"]);
    registry.register_aliases("try_inet_ntoa", &["try_ipv4_num_to_string"]);
    registry.register_aliases("assume_not_null", &["remove_nullable"]);
    registry.register_aliases("gen_random_uuid", &["uuid"]);

    register_inet_aton(registry);
    register_inet_ntoa(registry);
    register_run_diff(registry);
    register_grouping(registry);

    registry.properties.insert(
        "rand".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry.properties.insert(
        "gen_random_uuid".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry.register_passthrough_nullable_1_arg::<Float64Type, StringType, _, _>(
        "humanize_size",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Float64Type, StringType>(move |val, output, _| {
            let new_val = convert_byte_size(val.into());
            output.put_str(&new_val);
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Float64Type, StringType, _, _>(
        "humanize_number",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Float64Type, StringType>(move |val, output, _| {
            let new_val = convert_number_size(val.into());
            output.put_str(&new_val);
            output.commit_row();
        }),
    );

    registry.register_1_arg_core::<Float64Type, UInt8Type, _, _>(
        "sleep",
        |_, _| FunctionDomain::MayThrow,
        |a, ctx| {
            if let Some(val) = a.as_scalar() {
                let duration =
                    Duration::try_from_secs_f64((*val).into()).map_err(|x| x.to_string());
                match duration {
                    Ok(duration) => {
                        // Note!!!: Don't increase the sleep time to a large value, it'll block the thread.
                        if duration.gt(&Duration::from_secs(3)) {
                            let err = format!(
                                "The maximum sleep time is 3 seconds. Requested: {:?}",
                                duration
                            );
                            ctx.set_error(0, err);
                        } else {
                            std::thread::sleep(duration);
                        }
                    }
                    Err(e) => {
                        ctx.set_error(0, e);
                    }
                }
            } else {
                ctx.set_error(0, "Must be constant value");
            }
            Value::Scalar(0_u8)
        },
    );

    registry.register_0_arg_core::<NumberType<F64>, _, _>(
        "rand",
        |_| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        },
        |ctx| {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            let rand_nums = (0..ctx.num_rows)
                .map(|_| rng.gen::<F64>())
                .collect::<Vec<_>>();
            Value::Column(rand_nums.into())
        },
    );

    registry.register_1_arg::<NumberType<u64>, NumberType<F64>, _, _>(
        "rand",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        },
        |val, _| {
            let mut rng = rand::rngs::SmallRng::seed_from_u64(val);
            rng.gen::<F64>()
        },
    );

    registry.register_1_arg_core::<GenericType<0>, StringType, _, _>(
        "typeof",
        |_, _| FunctionDomain::Full,
        |_, ctx| Value::Scalar(ctx.generics[0].sql_name()),
    );

    registry.register_function_factory("ignore", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "ignore".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Boolean,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| {
                    FunctionDomain::Domain(Domain::Boolean(BooleanDomain {
                        has_true: false,
                        has_false: true,
                    }))
                }),
                eval: Box::new(|_, _| Value::Scalar(Scalar::Boolean(false))),
            },
        }))
    });

    registry.register_1_arg_core::<NullableType<GenericType<0>>, GenericType<0>, _, _>(
        "assume_not_null",
        |_, domain| {
            domain
                .value
                .as_ref()
                .map(|domain| FunctionDomain::Domain((**domain).clone()))
                .unwrap_or(FunctionDomain::Full)
        },
        |val, ctx| match val {
            ValueRef::Scalar(None) => Value::Scalar(Scalar::default_value(&ctx.generics[0])),
            ValueRef::Scalar(Some(scalar)) => Value::Scalar(scalar.to_owned()),
            ValueRef::Column(NullableColumn { column, .. }) => Value::Column(column),
        },
    );

    registry.register_1_arg_core::<NullType, NullType, _, _>(
        "to_nullable",
        |_, _| FunctionDomain::Domain(()),
        |val, _| val.to_owned(),
    );

    registry
        .register_1_arg_core::<NullableType<GenericType<0>>, NullableType<GenericType<0>>, _, _>(
            "to_nullable",
            |_, domain| FunctionDomain::Domain(domain.clone()),
            |val, _| val.to_owned(),
        );

    registry.register_0_arg_core::<StringType, _, _>(
        "gen_random_uuid",
        |_| FunctionDomain::Full,
        |ctx| {
            let mut values: Vec<u8> = Vec::with_capacity(ctx.num_rows * 36);
            let mut offsets: Vec<u64> = Vec::with_capacity(ctx.num_rows);
            offsets.push(0);

            for _ in 0..ctx.num_rows {
                let value = Uuid::new_v4();
                offsets.push(offsets.last().unwrap() + 36u64);
                write!(&mut values, "{:x}", value).unwrap();
            }
            let col = StringColumn::new(values.into(), offsets.into());
            Value::Column(col)
        },
    );
}

fn register_inet_aton(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, UInt32Type, _, _>(
        "inet_aton",
        |_, _| FunctionDomain::MayThrow,
        eval_inet_aton,
    );

    registry.register_combine_nullable_1_arg::<StringType, UInt32Type, _, _>(
        "try_inet_aton",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_inet_aton),
    );

    fn eval_inet_aton(val: ValueRef<StringType>, ctx: &mut EvalContext) -> Value<UInt32Type> {
        vectorize_with_builder_1_arg::<StringType, UInt32Type>(|addr_str, output, ctx| {
            match addr_str.parse::<Ipv4Addr>() {
                Ok(addr) => {
                    let addr_binary = u32::from(addr);
                    output.push(addr_binary);
                }
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_inet_ntoa(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, StringType, _, _>(
        "inet_ntoa",
        |_, _| FunctionDomain::MayThrow,
        eval_inet_ntoa,
    );

    registry.register_combine_nullable_1_arg::<Int64Type, StringType, _, _>(
        "try_inet_ntoa",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_inet_ntoa),
    );

    fn eval_inet_ntoa(val: ValueRef<Int64Type>, ctx: &mut EvalContext) -> Value<StringType> {
        vectorize_with_builder_1_arg::<Int64Type, StringType>(|val, output, ctx| {
            match num_traits::cast::cast::<i64, u32>(val) {
                Some(val) => {
                    let addr_str = Ipv4Addr::from(val.to_be_bytes()).to_string();
                    output.put_str(&addr_str);
                    output.commit_row();
                }
                None => {
                    ctx.set_error(
                        output.len(),
                        format!("Failed to parse '{}' into a IPV4 address", val),
                    );
                    output.commit_row();
                }
            }
        })(val, ctx)
    }
}

macro_rules! register_simple_domain_type_run_diff {
    ($registry:ident, $T:ty, $O:ty, $source_primitive_type:ty, $zero:expr) => {
        $registry.register_passthrough_nullable_1_arg::<$T, $O, _, _>(
            "running_difference",
            |_, _| FunctionDomain::MayThrow,
            move |arg1, ctx| match arg1 {
                ValueRef::Scalar(_val) => {
                    let mut builder =
                        NumberType::<$source_primitive_type>::create_builder(1, ctx.generics);
                    builder.push($zero);
                    Value::Scalar(NumberType::<$source_primitive_type>::build_scalar(builder))
                }
                ValueRef::Column(col) => {
                    let a_iter = NumberType::<$source_primitive_type>::iter_column(&col);
                    let b_iter = NumberType::<$source_primitive_type>::iter_column(&col);
                    let size = col.len();
                    let mut builder = NumberType::<$source_primitive_type>::create_builder(
                        a_iter.size_hint().0,
                        ctx.generics,
                    );
                    builder.push($zero);
                    for (a, b) in a_iter.skip(1).zip(b_iter.take(size - 1)) {
                        let diff = a - b;
                        builder.push(diff);
                    }
                    Value::Column(NumberType::<$source_primitive_type>::build_column(builder))
                }
            },
        );
    };
}

fn register_run_diff(registry: &mut FunctionRegistry) {
    register_simple_domain_type_run_diff!(registry, NumberType<i64>, NumberType<i64>, i64, 0);
    register_simple_domain_type_run_diff!(registry, DateType, NumberType<i32>, i32, 0);
    register_simple_domain_type_run_diff!(registry, TimestampType, NumberType<i64>, i64, 0);
    register_simple_domain_type_run_diff!(
        registry,
        NumberType<F64>,
        NumberType<F64>,
        F64,
        OrderedFloat(0.0)
    );
}

fn register_grouping(registry: &mut FunctionRegistry) {
    registry.register_function_factory("grouping", |params, arg_type| {
        if arg_type.len() != 1 {
            return None;
        }

        let params: Vec<usize> = params.iter().map(|p| p.get_i64().unwrap() as _).collect();

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "grouping".to_string(),
                args_type: vec![DataType::Number(NumberDataType::UInt32)],
                return_type: DataType::Number(NumberDataType::UInt32),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, _| match &args[0] {
                    ValueRef::Scalar(ScalarRef::Number(NumberScalar::UInt32(v))) => Value::Scalar(
                        Scalar::Number(NumberScalar::UInt32(compute_grouping(&params, *v))),
                    ),
                    ValueRef::Column(Column::Number(NumberColumn::UInt32(col))) => {
                        let output = col
                            .iter()
                            .map(|v| compute_grouping(&params, *v))
                            .collect::<Vec<_>>();
                        Value::Column(Column::Number(NumberColumn::UInt32(output.into())))
                    }
                    _ => unreachable!(),
                }),
            },
        }))
    })
}

/// Compute `grouping` by `grouping_id` and `cols`.
///
/// `cols` are indices of the column represented in `_grouping_id`.
/// The order will influence the result of `grouping`.
#[inline(always)]
pub fn compute_grouping(cols: &[usize], grouping_id: u32) -> u32 {
    let mut grouping = 0;
    for (i, &j) in cols.iter().rev().enumerate() {
        grouping |= ((grouping_id & (1 << j)) >> j) << i;
    }
    grouping
}
