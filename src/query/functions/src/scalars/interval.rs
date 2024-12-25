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

use databend_common_column::types::months_days_micros;
use databend_common_expression::date_helper::EvalMonthsImpl;
use databend_common_expression::error_to_null;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::interval::string_to_interval;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS interval)
    // to_interval(xx)
    register_string_to_interval(registry);
    register_interval_to_string(registry);
    // data/timestamp/interval +/- interval
    register_interval_add_sub(registry);
    register_number_to_interval(registry);
}

fn register_string_to_interval(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, IntervalType, _, _>(
        "to_interval",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_interval,
    );
    registry.register_combine_nullable_1_arg::<StringType, IntervalType, _, _>(
        "try_to_interval",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_interval),
    );

    fn eval_string_to_interval(
        val: Value<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<IntervalType> {
        vectorize_with_builder_1_arg::<StringType, IntervalType>(|val, output, ctx| {
            match string_to_interval(val) {
                Ok(interval) => output.push(months_days_micros::new(
                    interval.months,
                    interval.days,
                    interval.micros,
                )),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("cannot parse to type `INTERVAL`. {}", e),
                    );
                    output.push(months_days_micros::new(0, 0, 0));
                }
            }
        })(val, ctx)
    }
}

fn register_interval_to_string(registry: &mut FunctionRegistry) {
    registry.register_combine_nullable_1_arg::<IntervalType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, NullableType<StringType>>(
            |interval, output, _| {
                let res = interval_to_string(&interval).to_string();
                output.push(&res);
            },
        ),
    );
}

fn register_interval_add_sub(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<IntervalType, IntervalType, IntervalType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, IntervalType, IntervalType>(
            |a, b, output, _| {
                output.push(months_days_micros::new(
                    a.months() + b.months(),
                    a.days() + b.days(),
                    a.microseconds() + b.microseconds(),
                ))
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_add(b.microseconds())
                        .wrapping_add((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.jiff_tz.clone(),
                        b.months(),
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<IntervalType, TimestampType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<IntervalType, TimestampType, TimestampType>(
                |b, a, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_add(b.microseconds())
                        .wrapping_add((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.jiff_tz.clone(),
                        b.months(),
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<IntervalType, IntervalType, IntervalType, _, _>(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, IntervalType, IntervalType>(
            |a, b, output, _| {
                output.push(months_days_micros::new(
                    a.months() - b.months(),
                    a.days() - b.days(),
                    a.microseconds() - b.microseconds(),
                ));
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "minus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_sub(b.microseconds())
                        .wrapping_sub((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.jiff_tz.clone(),
                        -b.months(),
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );
}

fn register_number_to_interval(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "epoch",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_centuries",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 100 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_days",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, val as i32, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_weeks",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, (val * 7) as i32, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_decades",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 10 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_hours",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 3600 * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_microseconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_millennia",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 1000 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_milliseconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 1000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_minutes",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 60 * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_months",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(val as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_seconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_years",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 12) as i32, 0, 0);
            output.push(res);
        }),
    );
}
