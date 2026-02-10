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

use databend_common_base::base::OrderedFloat;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::error_to_null;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::Float32Type;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_io::number::FmtCacheEntry;
use rand::Rng;
use rand::SeedableRng;
use uuid::Uuid;

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
    register_num_to_char(registry);

    registry.properties.insert(
        "rand".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry.properties.insert(
        "gen_random_uuid".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry
        .scalar_builder("humanize_size")
        .function()
        .typed_1_arg::<Float64Type, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<Float64Type, StringType>(
            move |val, output, _| {
                let new_val = convert_byte_size(val.into());
                output.put_and_commit(new_val);
            },
        ))
        .register();

    registry
        .scalar_builder("humanize_number")
        .function()
        .typed_1_arg::<Float64Type, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<Float64Type, StringType>(
            move |val, output, _| {
                let new_val = convert_number_size(val.into());
                output.put_and_commit(new_val);
            },
        ))
        .register();

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

    registry
        .scalar_builder("rand")
        .function()
        .typed_0_arg::<NumberType<F64>>()
        .calc_domain(|_| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        })
        .vectorized(|ctx| {
            let mut rng = if ctx.func_ctx.random_function_seed {
                rand::rngs::SmallRng::seed_from_u64(1)
            } else {
                rand::rngs::SmallRng::from_entropy()
            };
            let rand_nums = (0..ctx.num_rows)
                .map(|_| rng.r#gen::<F64>())
                .collect::<Vec<_>>();
            Value::Column(rand_nums.into())
        })
        .function()
        .typed_1_arg::<NumberType<u64>, NumberType<F64>>()
        .passthrough_nullable()
        .calc_domain(|_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        })
        .each_row(|val, _| {
            let mut rng = rand::rngs::SmallRng::seed_from_u64(val);
            rng.r#gen::<F64>()
        })
        .register();

    registry
        .scalar_builder("typeof")
        .function()
        .typed_1_arg::<GenericType<0>, StringType>()
        .vectorized(|_, ctx| Value::Scalar(ctx.generics[0].sql_name()))
        .register();

    let ignore = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "ignore".to_string(),
                args_type: (0..args_type.len()).map(DataType::Generic).collect(),
                return_type: DataType::Boolean,
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, _| {
                    FunctionDomain::Domain(Domain::Boolean(BooleanDomain {
                        has_true: false,
                        has_false: true,
                    }))
                }),
                eval: scalar_evaluator(|_, _| Value::Scalar(Scalar::Boolean(false))),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("ignore", ignore);

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
            Value::Scalar(None) => Value::Scalar(Scalar::default_value(&ctx.generics[0])),
            Value::Scalar(Some(scalar)) => Value::Scalar(scalar.to_owned()),
            Value::Column(NullableColumn { column, .. }) => Value::Column(column),
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

    registry.register_0_arg_core::<StringType, _>(
        "gen_random_uuid",
        |_| FunctionDomain::Full,
        |ctx| {
            let mut builder = StringColumnBuilder::with_capacity(ctx.num_rows);

            for _ in 0..ctx.num_rows {
                let value = Uuid::now_v7();
                write!(&mut builder.row_buffer, "{}", value).unwrap();
                builder.commit_row();
            }

            Value::Column(builder.build())
        },
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, Float64Type, _, _>(
        "jaro_winkler",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<StringType, StringType, Float64Type>(|s1, s2, _ctx| {
            jaro_winkler::jaro_winkler(s1, s2).into()
        }),
    );
}

fn register_inet_aton(registry: &mut FunctionRegistry) {
    registry
        .scalar_builder("inet_aton")
        .function()
        .typed_1_arg::<StringType, UInt32Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_inet_aton)
        .register();

    registry.register_combine_nullable_1_arg::<StringType, UInt32Type, _, _>(
        "try_inet_aton",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_inet_aton),
    );

    fn eval_inet_aton(val: Value<StringType>, ctx: &mut EvalContext) -> Value<UInt32Type> {
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
    registry
        .scalar_builder("inet_ntoa")
        .function()
        .typed_1_arg::<Int64Type, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_inet_ntoa)
        .register();

    registry.register_combine_nullable_1_arg::<Int64Type, StringType, _, _>(
        "try_inet_ntoa",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_inet_ntoa),
    );

    fn eval_inet_ntoa(val: Value<Int64Type>, ctx: &mut EvalContext) -> Value<StringType> {
        vectorize_with_builder_1_arg::<Int64Type, StringType>(|val, output, ctx| {
            match num_traits::cast::cast::<i64, u32>(val) {
                Some(val) => {
                    let addr_str = Ipv4Addr::from(val.to_be_bytes()).to_string();
                    output.put_and_commit(addr_str);
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
        $registry
            .scalar_builder("running_difference")
            .function()
            .typed_1_arg::<$T, $O>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::MayThrow)
            .vectorized(move |arg1, ctx| match arg1 {
                Value::Scalar(_val) => {
                    let mut builder =
                        NumberType::<$source_primitive_type>::create_builder(1, ctx.generics);
                    builder.push($zero);
                    Value::Scalar(NumberType::<$source_primitive_type>::build_scalar(builder))
                }
                Value::Column(col) => {
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
            })
            .register();
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
    let grouping = FunctionFactory::Closure(Box::new(|params, arg_type: &[DataType]| {
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
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(move |args, _| match &args[0] {
                    Value::Scalar(Scalar::Number(NumberScalar::UInt32(v))) => Value::Scalar(
                        Scalar::Number(NumberScalar::UInt32(compute_grouping(&params, *v))),
                    ),
                    Value::Column(Column::Number(NumberColumn::UInt32(col))) => {
                        let output = col
                            .iter()
                            .map(|v| compute_grouping(&params, *v))
                            .collect::<Vec<_>>();
                        Value::Column(Column::Number(NumberColumn::UInt32(output.into())))
                    }
                    v => unreachable!("unexpected value type for grouping function: {:?}", v),
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("grouping", grouping);

    // dummy grouping
    // used in type_check before AggregateRewriter
    let dummy_grouping = FunctionFactory::Closure(Box::new(|_, arg_type: &[DataType]| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "grouping".to_string(),
                args_type: vec![DataType::Generic(0); arg_type.len()],
                return_type: DataType::Number(NumberDataType::UInt32),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(move |args, _| {
                    unreachable!(
                        "grouping function must be rewritten in type_checker, but got: {:?}",
                        args
                    )
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("grouping", dummy_grouping);
}

fn register_num_to_char(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["to_char"]);
    registry.register_passthrough_nullable_2_arg::<Int64Type, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<Int64Type, StringType, StringType>(
            |value, fmt, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }

                // TODO: We should cache FmtCacheEntry
                match fmt
                    .parse::<FmtCacheEntry>()
                    .and_then(|entry| entry.process_i64(value))
                {
                    Ok(s) => {
                        builder.put_and_commit(s);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.commit_row()
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<Float32Type, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<Float32Type, StringType, StringType>(
            |value, fmt, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }

                // TODO: We should cache FmtCacheEntry
                match fmt
                    .parse::<FmtCacheEntry>()
                    .and_then(|entry| entry.process_f32(*value))
                {
                    Ok(s) => {
                        builder.put_str(&s);
                        builder.commit_row()
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.commit_row()
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<Float64Type, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<Float64Type, StringType, StringType>(
            |value, fmt, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }

                // TODO: We should cache FmtCacheEntry
                match fmt
                    .parse::<FmtCacheEntry>()
                    .and_then(|entry| entry.process_f64(*value))
                {
                    Ok(s) => {
                        builder.put_str(&s);
                        builder.commit_row()
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.commit_row()
                    }
                }
            },
        ),
    );
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
// this implementation comes from https://github.com/joshuaclayton/jaro_winkler
pub(crate) mod jaro_winkler {
    enum DataWrapper {
        Vec(Vec<bool>),
        Bitwise(u128),
    }

    impl DataWrapper {
        fn build(len: usize) -> Self {
            if len <= 128 {
                DataWrapper::Bitwise(0)
            } else {
                let mut internal = Vec::with_capacity(len);
                internal.extend(std::iter::repeat_n(false, len));
                DataWrapper::Vec(internal)
            }
        }

        fn get(&self, idx: usize) -> bool {
            match self {
                DataWrapper::Vec(v) => v[idx],
                DataWrapper::Bitwise(v1) => (v1 >> idx) & 1 == 1,
            }
        }

        fn set_true(&mut self, idx: usize) {
            match self {
                DataWrapper::Vec(v) => v[idx] = true,
                DataWrapper::Bitwise(v1) => *v1 |= 1 << idx,
            }
        }
    }

    /// Calculates the Jaro-Winkler distance of two strings.
    ///
    /// The return value is between 0.0 and 1.0, where 1.0 means the strings are equal.
    pub fn jaro_winkler(left_: &str, right_: &str) -> f64 {
        let llen = left_.len();
        let rlen = right_.len();

        let (left, right, s1_len, s2_len) = if llen < rlen {
            (right_, left_, rlen, llen)
        } else {
            (left_, right_, llen, rlen)
        };

        match (s1_len, s2_len) {
            (0, 0) => return 1.0,
            (0, _) | (_, 0) => return 0.0,
            (_, _) => (),
        }

        if left == right {
            return 1.0;
        }

        let range = matching_distance(s1_len, s2_len);
        let mut s1m = DataWrapper::build(s1_len);
        let mut s2m = DataWrapper::build(s2_len);
        let mut matching: f64 = 0.0;
        let mut transpositions: f64 = 0.0;
        let left_as_bytes = left.as_bytes();
        let right_as_bytes = right.as_bytes();

        for (i, item) in right_as_bytes.iter().enumerate().take(s2_len) {
            let mut j = (i as isize - range as isize).max(0) as usize;
            let l = (i + range + 1).min(s1_len);
            while j < l {
                if item == &left_as_bytes[j] && !s1m.get(j) {
                    s1m.set_true(j);
                    s2m.set_true(i);
                    matching += 1.0;
                    break;
                }

                j += 1;
            }
        }

        if matching == 0.0 {
            return 0.0;
        }

        let mut l = 0;

        for (i, item) in right_as_bytes.iter().enumerate().take(s2_len - 1) {
            if s2m.get(i) {
                let mut j = l;

                while j < s1_len {
                    if s1m.get(j) {
                        l = j + 1;
                        break;
                    }

                    j += 1;
                }

                if item != &left_as_bytes[j] {
                    transpositions += 1.0;
                }
            }
        }
        transpositions = (transpositions / 2.0).ceil();

        let jaro = (matching / (s1_len as f64)
            + matching / (s2_len as f64)
            + (matching - transpositions) / matching)
            / 3.0;

        let prefix_length = left_as_bytes
            .iter()
            .zip(right_as_bytes)
            .take(4)
            .take_while(|(l, r)| l == r)
            .count() as f64;

        jaro + prefix_length * 0.1 * (1.0 - jaro)
    }

    fn matching_distance(s1_len: usize, s2_len: usize) -> usize {
        let max = s1_len.max(s2_len) as f32;
        ((max / 2.0).floor() - 1.0) as usize
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn different_is_zero() {
            assert_eq!(jaro_winkler("foo", "bar"), 0.0);
        }

        #[test]
        fn same_is_one() {
            assert_eq!(jaro_winkler("foo", "foo"), 1.0);
            assert_eq!(jaro_winkler("", ""), 1.0);
        }

        #[test]
        fn test_hello() {
            assert_eq!(jaro_winkler("hell", "hello"), 0.96);
        }

        macro_rules! assert_within {
            ($x:expr, $y:expr, delta=$d:expr) => {
                assert!(($x - $y).abs() <= $d)
            };
        }

        #[test]
        fn test_boundary() {
            let long_value = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s";
            let longer_value = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s";
            let result = jaro_winkler(long_value, longer_value);
            assert_within!(result, 0.82, delta = 0.01);
        }

        #[test]
        fn test_close_to_boundary() {
            let long_value = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test";
            assert_eq!(long_value.len(), 129);
            let longer_value = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s";
            let result = jaro_winkler(long_value, longer_value);
            assert_within!(result, 0.8, delta = 0.001);
        }
    }
}
