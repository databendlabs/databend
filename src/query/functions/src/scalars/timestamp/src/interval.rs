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
use databend_common_column::types::timestamp_tz;
use databend_common_exception::Result;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::date_helper::DateConverter;
use databend_common_expression::date_helper::EvalDaysImpl;
use databend_common_expression::date_helper::EvalMonthsImpl;
use databend_common_expression::date_helper::calc_date_to_timestamp;
use databend_common_expression::date_helper::timestamp_tz_components_via_lut;
use databend_common_expression::date_helper::today_date;
use databend_common_expression::error_to_null;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::interval::string_to_interval;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::fast_components_from_timestamp;
use jiff::Timestamp;
use jiff::Zoned;
use jiff::tz::Offset;
use jiff::tz::TimeZone;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS interval)
    // to_interval(xx)
    register_string_to_interval(registry);
    register_interval_to_string(registry);
    // data/timestamp/interval +/- interval
    register_interval_add_sub_mul(registry);
    register_number_to_interval(registry);
}

fn register_string_to_interval(registry: &mut FunctionRegistry) {
    registry
        .scalar_builder("to_interval")
        .function()
        .typed_1_arg::<StringType, IntervalType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_string_to_interval)
        .register();
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
    registry
        .scalar_builder("to_string")
        .function()
        .typed_1_arg::<IntervalType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .each_row(|interval, _| interval_to_string(&interval).to_string())
        .register();
}

fn register_interval_add_sub_mul(registry: &mut FunctionRegistry) {
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

    registry.register_passthrough_nullable_2_arg::<DateType, IntervalType, DateType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, IntervalType, DateType>(
            |date, interval, output, ctx| {
                eval_date_plus(date, interval, output, ctx);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<IntervalType, DateType, DateType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, DateType, DateType>(
            |interval, date, output, ctx| {
                eval_date_plus(date, interval, output, ctx);
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    eval_timestamp_plus(
                        a,
                        b,
                        output,
                        ctx,
                        |input| input,
                        |result| result,
                        ctx.func_ctx.tz.clone(),
                    );
                },
            ),
        );
    registry.register_passthrough_nullable_2_arg::<TimestampTzType, IntervalType, TimestampTzType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampTzType, IntervalType, TimestampTzType>(
            |a, b, output, ctx| {
                let offset = a.seconds_offset();
                let offset_micros = match timestamp_tz::micros_offset_inner(offset as i64) {
                    Some(v) => v,
                    None => {
                        ctx.set_error(output.len(), "invalid timestamp timezone offset");
                        output.push(timestamp_tz::default());
                        return;
                    }
                };
                let local = a.timestamp().wrapping_add(offset_micros);
                eval_timestamp_plus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.wrapping_sub(offset_micros);
                        timestamp_tz::new(utc, offset)
                    },
                    TimeZone::UTC,
                );
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<IntervalType, TimestampType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<IntervalType, TimestampType, TimestampType>(
                |b, a, output, ctx| {
                    eval_timestamp_plus(
                        a,
                        b,
                        output,
                        ctx,
                        |input| input,
                        |result| result,
                        ctx.func_ctx.tz.clone(),
                    );
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<IntervalType, TimestampTzType, TimestampTzType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, TimestampTzType, TimestampTzType>(
            |b, a, output, ctx| {
                let offset = a.seconds_offset();
                let offset_micros = match timestamp_tz::micros_offset_inner(offset as i64) {
                    Some(v) => v,
                    None => {
                        ctx.set_error(output.len(), "invalid timestamp timezone offset");
                        output.push(timestamp_tz::default());
                        return;
                    }
                };
                let local = a.timestamp().wrapping_add(offset_micros);
                eval_timestamp_plus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.wrapping_sub(offset_micros);
                        timestamp_tz::new(utc, offset)
                    },
                    TimeZone::UTC,
                );
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

    registry.register_passthrough_nullable_2_arg::<DateType, IntervalType, DateType, _, _>(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, IntervalType, DateType>(
            |date, interval, output, ctx| {
                eval_date_minus(date, interval, output, ctx);
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "minus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    eval_timestamp_minus(
                        a,
                        b,
                        output,
                        ctx,
                        |input| input,
                        |result| result,
                        ctx.func_ctx.tz.clone(),
                    );
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<TimestampTzType, IntervalType, TimestampTzType, _, _>(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampTzType, IntervalType, TimestampTzType>(
            |a, b, output, ctx| {
                let offset = a.seconds_offset();
                let offset_micros = match timestamp_tz::micros_offset_inner(offset as i64) {
                    Some(v) => v,
                    None => {
                        ctx.set_error(output.len(), "invalid timestamp timezone offset");
                        output.push(timestamp_tz::default());
                        return;
                    }
                };
                let local = a.timestamp().wrapping_add(offset_micros);
                eval_timestamp_minus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.wrapping_sub(offset_micros);
                        timestamp_tz::new(utc, offset)
                    },
                    TimeZone::UTC,
                );
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, TimestampType, IntervalType, _, _>(
            "age",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, TimestampType, IntervalType>(
                |t1, t2, output, ctx| {
                    let mut is_negative = false;
                    let mut t1 = t1;
                    let mut t2 = t2;
                    if t1 < t2 {
                        std::mem::swap(&mut t1, &mut t2);
                        is_negative = true;
                    }
                    let tz = &ctx.func_ctx.tz;
                    if let (Some(c1), Some(c2)) = (
                        fast_components_from_timestamp(t1, tz),
                        fast_components_from_timestamp(t2, tz),
                    ) {
                        output.push(calc_age_from_components(&c1, &c2, is_negative));
                    } else {
                        let t1 = t1.to_timestamp(tz);
                        let t2 = t2.to_timestamp(tz);
                        output.push(calc_age(t1, t2, is_negative));
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<TimestampTzType, TimestampTzType, IntervalType, _, _>(
            "age",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampTzType, TimestampTzType, IntervalType>(
                |t1, t2, output, ctx| {
                    let mut is_negative = false;
                    let mut t1 = t1;
                    let mut t2 = t2;
                    if t1 < t2 {
                        std::mem::swap(&mut t1, &mut t2);
                        is_negative = true;
                    }

                    if let (Some(c1), Some(c2)) = (
                        timestamp_tz_components_via_lut(t1),
                        timestamp_tz_components_via_lut(t2),
                    ) {
                        output.push(calc_age_from_components(&c1, &c2, is_negative));
                        return;
                    }

                    let zone1 = match Offset::from_seconds(t1.seconds_offset())
                        .map(TimeZone::fixed)
                    {
                        Ok(zone) => zone,
                        Err(err) => {
                            ctx.set_error(output.len(), err.to_string());
                            return;
                        }
                    };
                    let zone2 = match Offset::from_seconds(t2.seconds_offset())
                        .map(TimeZone::fixed)
                    {
                        Ok(zone) => zone,
                        Err(err) => {
                            ctx.set_error(output.len(), err.to_string());
                            return;
                        }
                    };
                    if let (Some(c1), Some(c2)) = (
                        fast_components_from_timestamp(t1.timestamp(), &zone1),
                        fast_components_from_timestamp(t2.timestamp(), &zone2),
                    ) {
                        output.push(calc_age_from_components(&c1, &c2, is_negative));
                        return;
                    }
                    let to_zoned = |ts_tz: timestamp_tz,
                                    zone: &TimeZone|
                     -> std::result::Result<Zoned, String> {
                        let ts =
                            Timestamp::from_microsecond(ts_tz.timestamp()).map_err(|err| err.to_string())?;
                        Ok(ts.to_zoned(zone.clone()))
                    };
                    let (t1, t2) = match (to_zoned(t1, &zone1), to_zoned(t2, &zone2)) {
                        (Ok(t1), Ok(t2)) => (t1, t2),
                        (Err(err), _) | (_, Err(err)) => {
                            ctx.set_error(output.len(), err);
                            return;
                        }
                    };
                    output.push(calc_age(t1, t2, is_negative));
                },
            ),
        );

    // age(ts) == age(now() at midnight, ts);
    registry
        .scalar_builder("age")
        .function()
        .typed_1_arg::<TimestampType, IntervalType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<TimestampType, IntervalType>(
            |t2, output, ctx| {
                let mut is_negative = false;
                let tz = &ctx.func_ctx.tz;

                let today_date = today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz);
                match calc_date_to_timestamp(today_date, tz) {
                    Ok(t) => {
                        let mut t1 = t;
                        let mut t2_val = t2;

                        if t1 < t2_val {
                            std::mem::swap(&mut t1, &mut t2_val);
                            is_negative = true;
                        }
                        if let (Some(c1), Some(c2)) = (
                            fast_components_from_timestamp(t1, tz),
                            fast_components_from_timestamp(t2_val, tz),
                        ) {
                            output.push(calc_age_from_components(&c1, &c2, is_negative));
                        } else {
                            let mut t1 = t1.to_timestamp(tz);
                            let mut t2 = t2_val.to_timestamp(tz);

                            if t1 < t2 {
                                std::mem::swap(&mut t1, &mut t2);
                                is_negative = true;
                            }
                            output.push(calc_age(t1, t2, is_negative));
                        }
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), e);
                        output.push(months_days_micros::new(0, 0, 0));
                    }
                }
            },
        ))
        .register();

    // age(ts) == age(now() at midnight, ts);
    registry
        .scalar_builder("age")
        .function()
        .typed_1_arg::<TimestampTzType, IntervalType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(
            vectorize_with_builder_1_arg::<TimestampTzType, IntervalType>(|t2, output, ctx| {
                let fn_eval_age = |t2: timestamp_tz, ctx: &mut EvalContext| {
                    let mut is_negative = false;

                    let offset = Offset::from_seconds(t2.seconds_offset())?;
                    let zone = TimeZone::fixed(offset);
                    let today_date = today_date(&ctx.func_ctx.now, &zone);
                    let t1_raw = calc_date_to_timestamp(today_date, &zone)?;
                    let t2_raw = t2.timestamp();
                    let today_ts = timestamp_tz::new(t1_raw, t2.seconds_offset());
                    let (t1, t2_micros, later_ts, earlier_ts) = if t1_raw >= t2_raw {
                        (t1_raw, t2_raw, today_ts, t2)
                    } else {
                        is_negative = true;
                        (t2_raw, t1_raw, t2, today_ts)
                    };

                    if let (Some(c1), Some(c2)) = (
                        timestamp_tz_components_via_lut(later_ts),
                        timestamp_tz_components_via_lut(earlier_ts),
                    ) {
                        return Result::Ok(calc_age_from_components(&c1, &c2, is_negative));
                    }
                    if let (Some(c1), Some(c2)) = (
                        fast_components_from_timestamp(t1, &zone),
                        fast_components_from_timestamp(t2_micros, &zone),
                    ) {
                        return Result::Ok(calc_age_from_components(&c1, &c2, is_negative));
                    }
                    let mut t1 = Timestamp::from_microsecond(t1)?.to_zoned(zone.clone());
                    let mut t2 = Timestamp::from_microsecond(t2_micros)?.to_zoned(zone.clone());

                    if t1 < t2 {
                        std::mem::swap(&mut t1, &mut t2);
                        is_negative = true;
                    }
                    Result::Ok(calc_age(t1, t2, is_negative))
                };

                match fn_eval_age(t2, ctx) {
                    Ok(result) => {
                        output.push(result);
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), e.to_string());
                        output.push(months_days_micros::new(0, 0, 0));
                    }
                }
            }),
        )
        .register();

    registry.register_passthrough_nullable_2_arg::<Int64Type, IntervalType, IntervalType, _, _>(
        "multiply",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<Int64Type, IntervalType, IntervalType>(|a, b, _ctx| {
            months_days_micros::new(
                b.months() * (a as i32),
                b.days() * (a as i32),
                b.microseconds() * a,
            )
        }),
    );

    registry.register_passthrough_nullable_2_arg::<IntervalType, Int64Type, IntervalType, _, _>(
        "multiply",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<IntervalType, Int64Type, IntervalType>(|b, a, _ctx| {
            months_days_micros::new(
                b.months() * (a as i32),
                b.days() * (a as i32),
                b.microseconds() * a,
            )
        }),
    );
}

fn eval_timestamp_plus<F1, F2, T>(
    a: T,
    b: months_days_micros,
    output: &mut Vec<T>,
    ctx: &mut EvalContext,
    fn_input: F1,
    fn_result: F2,
    timezone: TimeZone,
) where
    F1: FnOnce(T) -> i64,
    F2: FnOnce(i64) -> T,
    T: Default,
{
    // plus microseconds and days
    let ts = fn_input(a)
        .wrapping_add(b.microseconds())
        .wrapping_add((b.days() as i64).wrapping_mul(86_400_000_000));
    match EvalMonthsImpl::eval_timestamp(ts, &timezone, b.months(), false) {
        Ok(t) => output.push(fn_result(t)),
        Err(e) => {
            ctx.set_error(output.len(), e);
            output.push(T::default());
        }
    }
}

fn eval_timestamp_minus<F1, F2, T>(
    a: T,
    b: months_days_micros,
    output: &mut Vec<T>,
    ctx: &mut EvalContext,
    fn_input: F1,
    fn_result: F2,
    timezone: TimeZone,
) where
    F1: FnOnce(T) -> i64,
    F2: FnOnce(i64) -> T,
    T: Default,
{
    // plus microseconds and days
    let ts = fn_input(a)
        .wrapping_sub(b.microseconds())
        .wrapping_sub((b.days() as i64).wrapping_mul(86_400_000_000));
    match EvalMonthsImpl::eval_timestamp(ts, &timezone, -b.months(), false) {
        Ok(t) => output.push(fn_result(t)),
        Err(e) => {
            ctx.set_error(output.len(), e);
            output.push(T::default());
        }
    }
}

fn eval_date_plus(
    date: i32,
    interval: months_days_micros,
    output: &mut Vec<i32>,
    ctx: &mut EvalContext,
) {
    match apply_interval_to_date(date, interval, &ctx.func_ctx.tz, true) {
        Ok(result) => output.push(result),
        Err(err) => {
            ctx.set_error(output.len(), err);
            output.push(0);
        }
    }
}

fn eval_date_minus(
    date: i32,
    interval: months_days_micros,
    output: &mut Vec<i32>,
    ctx: &mut EvalContext,
) {
    match apply_interval_to_date(date, interval, &ctx.func_ctx.tz, false) {
        Ok(result) => output.push(result),
        Err(err) => {
            ctx.set_error(output.len(), err);
            output.push(0);
        }
    }
}

fn apply_interval_to_date(
    mut date: i32,
    interval: months_days_micros,
    tz: &TimeZone,
    is_addition: bool,
) -> std::result::Result<i32, String> {
    if interval.microseconds() != 0 {
        return Err(
            "DATE +/- INTERVAL with time parts should be evaluated as TIMESTAMP".to_string(),
        );
    }

    let (days, months) = if is_addition {
        (interval.days(), interval.months())
    } else {
        (-interval.days(), -interval.months())
    };

    if days != 0 {
        date = EvalDaysImpl::eval_date(date, days);
    }
    if months != 0 {
        date = EvalMonthsImpl::eval_date(date, tz, months, false)?;
    }

    Ok(date)
}

fn register_number_to_interval(registry: &mut FunctionRegistry) {
    fn register_i64_to_interval<F>(registry: &mut FunctionRegistry, name: &'static str, func: F)
    where F: Fn(i64, &mut EvalContext) -> months_days_micros + Send + Sync + Copy + 'static {
        registry
            .scalar_builder(name)
            .function()
            .typed_1_arg::<Int64Type, IntervalType>()
            .passthrough_nullable()
            .each_row(func)
            .register();
    }

    fn register_interval_to_i64<F>(registry: &mut FunctionRegistry, name: &'static str, func: F)
    where F: Fn(months_days_micros, &mut EvalContext) -> i64 + Send + Sync + Copy + 'static {
        registry
            .scalar_builder(name)
            .function()
            .typed_1_arg::<IntervalType, Int64Type>()
            .passthrough_nullable()
            .each_row(func)
            .register();
    }

    fn register_interval_to_f64<F>(registry: &mut FunctionRegistry, name: &'static str, func: F)
    where F: Fn(months_days_micros, &mut EvalContext) -> <Float64Type as AccessType>::Scalar
            + Send
            + Sync
            + Copy
            + 'static {
        registry
            .scalar_builder(name)
            .function()
            .typed_1_arg::<IntervalType, Float64Type>()
            .passthrough_nullable()
            .each_row(func)
            .register();
    }

    register_i64_to_interval(registry, "to_centuries", |val, _| {
        months_days_micros::new((val * 100 * 12) as i32, 0, 0)
    });
    register_i64_to_interval(registry, "to_days", |val, _| {
        months_days_micros::new(0, val as i32, 0)
    });
    register_i64_to_interval(registry, "to_weeks", |val, _| {
        months_days_micros::new(0, (val * 7) as i32, 0)
    });
    register_i64_to_interval(registry, "to_decades", |val, _| {
        months_days_micros::new((val * 10 * 12) as i32, 0, 0)
    });
    register_i64_to_interval(registry, "to_hours", |val, _| {
        months_days_micros::new(0, 0, val * 3600 * 1_000_000)
    });
    register_i64_to_interval(registry, "to_microseconds", |val, _| {
        months_days_micros::new(0, 0, val)
    });
    register_i64_to_interval(registry, "to_millennia", |val, _| {
        months_days_micros::new((val * 1000 * 12) as i32, 0, 0)
    });
    register_i64_to_interval(registry, "to_milliseconds", |val, _| {
        months_days_micros::new(0, 0, val * 1000)
    });
    register_i64_to_interval(registry, "to_minutes", |val, _| {
        months_days_micros::new(0, 0, val * 60 * 1_000_000)
    });
    register_i64_to_interval(registry, "to_months", |val, _| {
        months_days_micros::new(val as i32, 0, 0)
    });
    register_i64_to_interval(registry, "to_seconds", |val, _| {
        months_days_micros::new(0, 0, val * 1_000_000)
    });
    register_i64_to_interval(registry, "to_years", |val, _| {
        months_days_micros::new((val * 12) as i32, 0, 0)
    });

    register_interval_to_i64(registry, "to_year", |val, _| val.months() as i64 / 12);
    register_interval_to_i64(registry, "to_month", |val, _| val.months() as i64 % 12);
    register_interval_to_i64(registry, "to_day_of_month", |val, _| val.days() as i64);
    register_interval_to_i64(registry, "to_hour", |val, _| {
        let total_seconds = (val.microseconds() as f64) / 1_000_000.0;
        (total_seconds / 3600.0) as i64
    });
    register_interval_to_i64(registry, "to_minute", |val, _| {
        let total_seconds = (val.microseconds() as f64) / 1_000_000.0;
        ((total_seconds % 3600.0) / 60.0) as i64
    });
    register_interval_to_i64(registry, "to_microsecond", |val, _| {
        val.microseconds() % 60_000_000
    });

    register_interval_to_f64(registry, "to_second", |val, _| {
        let microseconds = val.microseconds() % 60_000_000;
        let seconds = microseconds as f64 / 1_000_000.0;
        seconds.into()
    });
    register_interval_to_f64(registry, "epoch", |val, _| {
        let total_seconds = (val.total_micros() as f64) / 1_000_000.0;
        total_seconds.into()
    });
}

fn calc_age_from_components(
    t1: &DateTimeComponents,
    t2: &DateTimeComponents,
    is_negative: bool,
) -> months_days_micros {
    let mut years = t1.year - t2.year;
    let mut months = t1.month as i32 - t2.month as i32;
    let mut days = t1.day as i32 - t2.day as i32;

    let t1_total_nanos = (t1.hour as i64 * 3600 + t1.minute as i64 * 60 + t1.second as i64)
        * 1_000_000_000
        + (t1.micro as i64) * 1_000;
    let t2_total_nanos = (t2.hour as i64 * 3600 + t2.minute as i64 * 60 + t2.second as i64)
        * 1_000_000_000
        + (t2.micro as i64) * 1_000;
    let mut total_nanoseconds_diff = t1_total_nanos - t2_total_nanos;

    if total_nanoseconds_diff < 0 {
        total_nanoseconds_diff += 24 * 3600 * 1_000_000_000;
        days -= 1;
    }

    if days < 0 {
        days += t2.days_in_month as i32;
        months -= 1;
    }

    if months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = months + years * 12;
    let diff_micros = total_nanoseconds_diff / 1_000;

    if is_negative {
        months_days_micros::new(-total_months, -days, -diff_micros)
    } else {
        months_days_micros::new(total_months, days, diff_micros)
    }
}

fn calc_age(t1: Zoned, t2: Zoned, is_negative: bool) -> months_days_micros {
    let mut years = t1.year() - t2.year();
    let mut months = t1.month() - t2.month();
    let mut days = t1.day() - t2.day();

    let t1_total_nanos = (t1.hour() as i64 * 3600 + t1.minute() as i64 * 60 + t1.second() as i64)
        * 1_000_000_000
        + t1.subsec_nanosecond() as i64;
    let t2_total_nanos = (t2.hour() as i64 * 3600 + t2.minute() as i64 * 60 + t2.second() as i64)
        * 1_000_000_000
        + t2.subsec_nanosecond() as i64;
    let mut total_nanoseconds_diff = t1_total_nanos - t2_total_nanos;

    if total_nanoseconds_diff < 0 {
        total_nanoseconds_diff += 24 * 3600 * 1_000_000_000;
        days -= 1;
    }

    if days < 0 {
        let days_in_month_of_t2 = t2.date().days_in_month();
        days += days_in_month_of_t2;
        months -= 1;
    }

    if months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = months as i32 + (years as i32 * 12);

    if is_negative {
        months_days_micros::new(-total_months, -days as i32, -total_nanoseconds_diff / 1000)
    } else {
        months_days_micros::new(total_months, days as i32, total_nanoseconds_diff / 1000)
    }
}
