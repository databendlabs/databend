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
use databend_common_expression::error_to_null;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::interval::string_to_interval;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::components_from_timestamp;
use databend_common_timezone::fast_components_from_timestamp;
use databend_common_timezone::utc_from_local;
use jiff::Timestamp;
use jiff::Zoned;
use jiff::tz::Offset;
use jiff::tz::TimeZone;

use crate::date_arithmetic::timestamp_tz_components_via_lut;
use crate::date_conversion::calc_date_to_timestamp;
use crate::date_conversion::today_date;

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

    registry.register_passthrough_nullable_2_arg::<DateType, IntervalType, TimestampType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, IntervalType, TimestampType>(
            |date, interval, output, ctx| {
                eval_date_interval(date, interval, output, ctx, true);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<IntervalType, DateType, TimestampType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, DateType, TimestampType>(
            |interval, date, output, ctx| {
                eval_date_interval(date, interval, output, ctx, true);
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
                        ensure_timestamp_range,
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
                let Some(local) = a.timestamp().checked_add(offset_micros) else {
                    ctx.set_error(output.len(), "invalid timestamp timezone value");
                    output.push(timestamp_tz::default());
                    return;
                };
                eval_timestamp_plus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.checked_sub(offset_micros).ok_or_else(|| {
                            "Invalid date: timestamp timezone value is out of range".to_string()
                        })?;
                        ensure_timestamp_tz_range(utc)?;
                        Ok(timestamp_tz::new(utc, offset))
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
                        ensure_timestamp_range,
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
                let Some(local) = a.timestamp().checked_add(offset_micros) else {
                    ctx.set_error(output.len(), "invalid timestamp timezone value");
                    output.push(timestamp_tz::default());
                    return;
                };
                eval_timestamp_plus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.checked_sub(offset_micros).ok_or_else(|| {
                            "Invalid date: timestamp timezone value is out of range".to_string()
                        })?;
                        ensure_timestamp_tz_range(utc)?;
                        Ok(timestamp_tz::new(utc, offset))
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

    registry.register_passthrough_nullable_2_arg::<DateType, IntervalType, TimestampType, _, _>(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, IntervalType, TimestampType>(
            |date, interval, output, ctx| {
                eval_date_interval(date, interval, output, ctx, false);
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
                        ensure_timestamp_range,
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
                let Some(local) = a.timestamp().checked_add(offset_micros) else {
                    ctx.set_error(output.len(), "invalid timestamp timezone value");
                    output.push(timestamp_tz::default());
                    return;
                };
                eval_timestamp_minus(
                    a,
                    b,
                    output,
                    ctx,
                    move |_| local,
                    move |result| {
                        let utc = result.checked_sub(offset_micros).ok_or_else(|| {
                            "Invalid date: timestamp timezone value is out of range".to_string()
                        })?;
                        ensure_timestamp_tz_range(utc)?;
                        Ok(timestamp_tz::new(utc, offset))
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
                        let t1 = timestamp_from_micros(t1, tz);
                        let t2 = timestamp_from_micros(t2, tz);
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
                            let mut t1 = timestamp_from_micros(t1, tz);
                            let mut t2 = timestamp_from_micros(t2_val, tz);

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
    F2: FnOnce(i64) -> std::result::Result<T, String>,
    T: Default,
{
    match apply_interval_to_timestamp(fn_input(a), b, &timezone, true).and_then(fn_result) {
        Ok(t) => output.push(t),
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
    F2: FnOnce(i64) -> std::result::Result<T, String>,
    T: Default,
{
    match apply_interval_to_timestamp(fn_input(a), b, &timezone, false).and_then(fn_result) {
        Ok(t) => output.push(t),
        Err(e) => {
            ctx.set_error(output.len(), e);
            output.push(T::default());
        }
    }
}

fn apply_interval_to_timestamp(
    timestamp: i64,
    interval: months_days_micros,
    timezone: &TimeZone,
    is_addition: bool,
) -> std::result::Result<i64, String> {
    if interval.months() == 0 && interval.days() == 0 && interval.microseconds() == 0 {
        return Ok(timestamp);
    }

    let components = components_from_timestamp(timestamp, timezone)
        .ok_or_else(|| "Invalid date: cannot resolve timestamp components".to_string())?;
    apply_interval_to_civil(
        components.year,
        components.month,
        components.day,
        components.hour,
        components.minute,
        components.second,
        components.micro,
        Some(components.offset_seconds),
        false,
        interval,
        timezone,
        is_addition,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_interval_to_civil(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
    preferred_offset: Option<i32>,
    enforce_date_range: bool,
    interval: months_days_micros,
    timezone: &TimeZone,
    is_addition: bool,
) -> std::result::Result<i64, String> {
    let direction = if is_addition { 1_i64 } else { -1_i64 };
    let months = direction * i64::from(interval.months());
    let month_index = i64::from(year)
        .checked_mul(12)
        .and_then(|value| value.checked_add(i64::from(month) - 1))
        .and_then(|value| value.checked_add(months))
        .ok_or_else(|| "Invalid date: month arithmetic is out of range".to_string())?;
    let year = month_index.div_euclid(12);
    let month = (month_index.rem_euclid(12) + 1) as u8;
    let day = day.min(days_in_month(year, month));

    // DuckDB preserves the three interval components and applies them as
    // months, days, then microseconds in civil time. Keep that civil
    // intermediate widened until every component has been applied. Besides
    // matching DST behavior, this permits a temporarily out-of-range year to
    // be brought back into Databend's range by a later interval component.
    let micros_per_day = i128::from(months_days_micros::MICROS_PER_DAY);
    let civil_days = civil_date_to_days(year, month, day);
    let time_micros = i128::from(hour) * 3_600_000_000
        + i128::from(minute) * 60_000_000
        + i128::from(second) * 1_000_000
        + i128::from(micro);
    let direction = i128::from(direction);
    let civil_micros = civil_days * micros_per_day
        + time_micros
        + direction * i128::from(interval.days()) * micros_per_day
        + direction * i128::from(interval.microseconds());

    let result_days = civil_micros.div_euclid(micros_per_day);
    let time_micros = civil_micros.rem_euclid(micros_per_day) as i64;
    let (year, month, day) = civil_date_from_days(result_days);
    let year = i32::try_from(year).map_err(|_| "Invalid date: year is out of range".to_string())?;
    let hour = (time_micros / 3_600_000_000) as u8;
    let minute = (time_micros % 3_600_000_000 / 60_000_000) as u8;
    let second = (time_micros % 60_000_000 / 1_000_000) as u8;
    let micro = (time_micros % 1_000_000) as u32;

    if enforce_date_range && !(1..=9999).contains(&year) {
        return Err("Invalid date: calendar arithmetic is out of range".to_string());
    }

    // Preserve the source side of a DST fold when that offset is still valid
    // for the target civil time. In particular, adding a zero interval must
    // not change either repeated local time into the other one.
    if let Some(offset_seconds) = preferred_offset {
        let candidate = civil_micros - i128::from(offset_seconds) * 1_000_000;
        if let Ok(candidate) = i64::try_from(candidate)
            && components_from_timestamp(candidate, timezone).is_some_and(|components| {
                components.year == year
                    && components.month == month
                    && components.day == day
                    && components.hour == hour
                    && components.minute == minute
                    && components.second == second
                    && components.micro == micro
                    && components.offset_seconds == offset_seconds
            })
        {
            return Ok(candidate);
        }
    }

    utc_from_local(timezone, year, month, day, hour, minute, second, micro)
        .ok_or_else(|| "Invalid date: calendar arithmetic is out of range".to_string())
}

fn days_in_month(year: i64, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => unreachable!("month arithmetic produced an invalid month"),
    }
}

// Proleptic Gregorian conversion based on a 400-year era. Day zero is
// 1970-01-01. Euclidean division keeps the conversion symmetric for years
// before year 1 and lets interval components cancel across the DATE bounds.
fn civil_date_to_days(year: i64, month: u8, day: u8) -> i128 {
    let mut year = i128::from(year);
    let month = i128::from(month);
    let day = i128::from(day);
    year -= i128::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_date_from_days(days: i128) -> (i128, u8, u8) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i128::from(month <= 2);
    (year, month as u8, day as u8)
}

fn ensure_timestamp_range(timestamp: i64) -> std::result::Result<i64, String> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&timestamp) {
        Ok(timestamp)
    } else {
        Err(format!(
            "Invalid date: timestamp value {timestamp} is out of range"
        ))
    }
}

fn ensure_timestamp_tz_range(timestamp: i64) -> std::result::Result<i64, String> {
    ensure_timestamp_range(timestamp)?;
    Timestamp::from_microsecond(timestamp)
        .map(|_| timestamp)
        .map_err(|_| "Invalid date: timestamp timezone value is out of supported range".to_string())
}

fn eval_date_interval(
    date: i32,
    interval: months_days_micros,
    output: &mut Vec<i64>,
    ctx: &mut EvalContext,
    is_addition: bool,
) {
    let date = date_from_days(date);
    let result = apply_interval_to_civil(
        i32::from(date.year()),
        date.month() as u8,
        date.day() as u8,
        0,
        0,
        0,
        0,
        None,
        true,
        interval,
        &ctx.func_ctx.tz,
        is_addition,
    )
    .and_then(ensure_timestamp_range);
    match result {
        Ok(result) => output.push(result),
        Err(err) => {
            ctx.set_error(output.len(), err);
            output.push(0);
        }
    }
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
