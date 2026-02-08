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

use core::fmt;
use std::borrow::Cow;
use std::fmt::Display;
use std::fmt::FormattingOptions;
use std::io::Write;

use chrono::Datelike;
use chrono::NaiveDate;
use databend_common_base::runtime::catch_unwind;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::error_to_null;
use databend_common_expression::serialize::EPOCH_DAYS_FROM_CE;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::UInt16Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::timestamp::MICROS_PER_MILLI;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::types::timestamp_tz::string_to_timestamp_tz;
use databend_common_expression::utils::date_helper::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_4_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_timezone::fast_components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;
use dtparse::parse;
use jiff::Timestamp;
use jiff::Unit;
use jiff::civil::Date;
use jiff::civil::Weekday;
use jiff::civil::date;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::Offset;
use jiff::tz::TimeZone;
use num_traits::AsPrimitive;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS timestamp)
    // to_timestamp(xx)
    register_string_to_timestamp(registry);
    register_date_to_timestamp(registry);
    register_date_to_timestamp_tz(registry);
    register_number_to_timestamp(registry);
    register_timestamp_to_timestamp_tz(registry);
    register_timestamp_tz_to_timestamp(registry);

    // cast(xx AS date)
    // to_date(xx)
    register_string_to_date(registry);
    register_timestamp_to_date(registry);
    register_timestamp_tz_to_date(registry);
    register_number_to_date(registry);

    // cast([date | timestamp] AS string)
    // to_string([date | timestamp])
    register_to_string(registry);

    // cast([date | timestamp] AS [uint8 | int8 | ...])
    // to_[uint8 | int8 | ...]([date | timestamp])
    register_to_number(registry);

    // [add | subtract]_[years | months | days | hours | minutes | seconds]([date | timestamp], number)
    // date_[add | sub]([year | quarter | month | week | day | hour | minute | second], [date | timestamp], number)
    // [date | timestamp] [+ | -] interval number [year | quarter | month | week | day | hour | minute | second]
    register_add_functions(registry);
    register_sub_functions(registry);

    // date_diff([year | quarter | month | week | day | hour | minute | second], [date | timestamp], [date | timestamp])
    // [date | timestamp] +/- [date | timestamp]
    register_diff_functions(registry);

    // datesub([year | quarter | month | week | day | hour | minute | second], [date | timestamp], [date | timestamp])
    // The number of complete partitions between the dates.
    register_between_functions(registry);

    // now, today, yesterday, tomorrow
    register_real_time_functions(registry);

    // to_*([date | timestamp]) -> number
    register_to_number_functions(registry);

    // to_*([date | timestamp]) -> [date | timestamp]
    register_rounder_functions(registry);

    // [date | timestamp] +/- number
    register_timestamp_add_sub(registry);

    // convert_timezone( target_timezone, 'timestamp')
    register_convert_timezone(registry);
}

/// Check if timestamp is within range, and return the timestamp in micros.
#[inline]
pub fn int64_to_timestamp(mut n: i64) -> i64 {
    if -31536000000 < n && n < 31536000000 {
        n * MICROS_PER_SEC
    } else if -31536000000000 < n && n < 31536000000000 {
        n * MICROS_PER_MILLI
    } else {
        clamp_timestamp(&mut n);
        n
    }
}

/// calc int64 domain to timestamp domain
#[inline]
pub fn calc_int64_to_timestamp_domain(n: i64) -> i64 {
    if -31536000000 < n && n < 31536000000 {
        n * MICROS_PER_SEC
    } else if -31536000000000 < n && n < 31536000000000 {
        n * MICROS_PER_MILLI
    } else {
        n.clamp(TIMESTAMP_MIN, TIMESTAMP_MAX)
    }
}

/// calc int32 domain to timestamp domain
#[inline]
pub fn calc_int32_to_timestamp_domain(n: i32) -> i64 {
    let n = n as i64 * 24 * 3600 * MICROS_PER_SEC;
    calc_int64_to_timestamp_domain(n)
}

fn int32_domain_to_timestamp_domain<T: AsPrimitive<i32>>(
    domain: &SimpleDomain<T>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: calc_int32_to_timestamp_domain(domain.min.as_()),
        max: calc_int32_to_timestamp_domain(domain.max.as_()),
    })
}

fn int64_domain_to_timestamp_domain<T: AsPrimitive<i64>>(
    domain: &SimpleDomain<T>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: calc_int64_to_timestamp_domain(domain.min.as_()),
        max: calc_int64_to_timestamp_domain(domain.max.as_()),
    })
}

fn timestamp_domain_to_timestamp_tz_domain(
    _domain: &SimpleDomain<i64>,
) -> Option<SimpleDomain<timestamp_tz>> {
    // We cannot infer a reliable offset without evaluating against the runtime timezone,
    // so skip static domain narrowing to avoid incorrect planner assumptions.
    None
}

fn timestamp_tz_domain_to_timestamp_domain(
    domain: &SimpleDomain<timestamp_tz>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: domain.min.timestamp(),
        max: domain.max.timestamp(),
    })
}

// jiff don't support local formats:
// https://github.com/BurntSushi/jiff/issues/219
fn replace_time_format(format: &str) -> Cow<'_, str> {
    if ["%c", "x", "X"].iter().any(|f| format.contains(f)) {
        let format = format
            .replace("%c", "%x %X")
            .replace("%x", "%F")
            .replace("%X", "%T");
        Cow::Owned(format)
    } else {
        Cow::Borrowed(format)
    }
}

fn register_convert_timezone(registry: &mut FunctionRegistry) {
    // 2 arguments function [target_timezone, src_timestamp]
    registry.register_passthrough_nullable_2_arg::<StringType, TimestampType, TimestampType, _, _>(
        "convert_timezone",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, TimestampType, TimestampType>(
            |target_tz, src_timestamp, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push(0);
                        return;
                    }
                }
                let source_tz = &ctx.func_ctx.tz;
                let t_tz = match TimeZone::get(target_tz) {
                    Ok(tz) => tz,
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("cannot parse target `timezone`. {}", e),
                        );
                        output.push(0);
                        return;
                    }
                };

                let source_components = fast_components_from_timestamp(src_timestamp, source_tz);
                let target_components = fast_components_from_timestamp(src_timestamp, &t_tz);

                let (instant_micros, src_dst_from_utc, target_dst_from_utc) =
                    if let (Some(src_comp), Some(target_comp)) =
                        (source_components, target_components)
                    {
                        (
                            src_timestamp,
                            src_comp.offset_seconds,
                            target_comp.offset_seconds,
                        )
                    } else {
                        // Fall back to the slower Jiff conversion for timestamps
                        // outside the LUT coverage (e.g. <1900 or >2299).
                        let src_zoned = src_timestamp.to_timestamp(source_tz);
                        let target_zoned = src_zoned.with_time_zone(t_tz.clone());
                        (
                            target_zoned.timestamp().as_microsecond(),
                            src_zoned.offset().seconds(),
                            target_zoned.offset().seconds(),
                        )
                    };

                let offset_as_micros_sec = (target_dst_from_utc - src_dst_from_utc) as i64;
                match offset_as_micros_sec.checked_mul(MICROS_PER_SEC) {
                    Some(offset) => match instant_micros.checked_add(offset) {
                        Some(res) => output.push(res),
                        None => {
                            ctx.set_error(output.len(), "calc final time error".to_string());
                            output.push(0);
                        }
                    },
                    None => {
                        ctx.set_error(output.len(), "calc time offset error".to_string());
                        output.push(0);
                    }
                }
            },
        ),
    );
}

fn register_string_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_date", &["str_to_date", "date"]);
    registry.register_aliases("to_year", &["str_to_year", "year"]);
    registry.register_aliases("to_day_of_month", &["day", "dayofmonth"]);
    registry.register_aliases("to_day_of_year", &["dayofyear"]);
    registry.register_aliases("to_month", &["month"]);
    registry.register_aliases("to_quarter", &["quarter"]);
    registry.register_aliases("to_week_of_year", &["week", "weekofyear"]);

    registry.register_aliases("to_timestamp", &["to_datetime", "str_to_timestamp"]);
    registry.register_aliases("try_to_timestamp", &["try_to_datetime"]);

    registry.register_passthrough_nullable_1_arg::<StringType, TimestampType, _>(
        "to_timestamp",
        |ctx, d| {
            let max = d.max.clone().unwrap_or_default();
            let mut res = Vec::with_capacity(2);
            for (i, v) in [&d.min, &max].iter().enumerate() {
                let mut extend_num = 0;
                if i == 1 && d.max.is_none() {
                    // the max domain is unbounded
                    res.push(TIMESTAMP_MAX);
                    break;
                }
                let mut d = string_to_timestamp(v, &ctx.tz);
                // the string max domain maybe truncated into `"2024-09-02 00:0�"`
                const MAX_LEN: usize = "1000-01-01".len();
                if d.is_err() && v.len() > MAX_LEN {
                    d = string_to_timestamp(&v[0..MAX_LEN], &ctx.tz);
                    if i == 0 {
                        extend_num = -1;
                    } else {
                        extend_num = 1;
                    }
                }

                if let Ok(ts) = d {
                    res.push(
                        ts.timestamp().as_microsecond()
                            + extend_num * (24 * 60 * 60 * MICROS_PER_SEC - 1),
                    );
                } else {
                    return FunctionDomain::MayThrow;
                }
            }
            FunctionDomain::Domain(SimpleDomain {
                min: res[0].clamp(TIMESTAMP_MIN, TIMESTAMP_MAX),
                max: res[1].clamp(TIMESTAMP_MIN, TIMESTAMP_MAX),
            })
        },
        eval_string_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<StringType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_timestamp),
    );

    fn eval_string_to_timestamp(
        val: Value<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<StringType, TimestampType>(|val, output, ctx| {
            let mut d = string_to_timestamp(val, &ctx.func_ctx.tz);
            if !ctx.func_ctx.enable_strict_datetime_parser {
                d = match catch_unwind(|| {
                    d.or_else(|_| {
                        parse(val)
                            .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
                            .and_then(|(naive_dt, _)| {
                                string_to_timestamp(naive_dt.to_string(), &ctx.func_ctx.tz)
                            })
                    })
                }) {
                    Ok(result) => result,
                    Err(_) => Err(ErrorCode::BadArguments(format!(
                        "TIMESTAMP '{}' is not recognized.",
                        val
                    ))),
                }
            }

            match d {
                Ok(ts) => output.push(ts.timestamp().as_microsecond()),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("cannot parse to type `TIMESTAMP`. {}", e),
                    );
                    output.push(0);
                }
            }
        })(val, ctx)
    }

    registry.register_passthrough_nullable_1_arg::<StringType, TimestampTzType, _>(
        "to_timestamp_tz",
        |_, _| FunctionDomain::Full,
        eval_string_to_timestamp_tz,
    );
    registry.register_combine_nullable_1_arg::<StringType, TimestampTzType, _, _>(
        "try_to_timestamp_tz",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_timestamp_tz),
    );

    fn eval_string_to_timestamp_tz(
        val: Value<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampTzType> {
        vectorize_with_builder_1_arg::<StringType, TimestampTzType>(|val, output, ctx| {
            let result = string_to_timestamp_tz(val.as_bytes(), || &ctx.func_ctx.tz);

            match result {
                Ok(ts_tz) => output.push(ts_tz),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("cannot parse to type `TIMESTAMP_TZ`. {}", e),
                    );
                    output.push(timestamp_tz::new(0, 0));
                }
            }
        })(val, ctx)
    }

    registry.register_combine_nullable_2_arg::<StringType, StringType, TimestampType, _, _>(
        "to_timestamp",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<TimestampType>>(
            |timestamp, format, output, ctx| match string_to_format_datetime(
                timestamp, format, ctx, true,
            ) {
                Ok((ts, need_null)) => {
                    if need_null {
                        output.push_null();
                    } else {
                        output.push(ts);
                    }
                }
                Err(e) => {
                    ctx.set_error(output.len(), e.to_string());
                    output.push(0);
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<TimestampType>>(
            |timestamp, format, output, ctx| match string_to_format_datetime(
                timestamp, format, ctx, true,
            ) {
                Ok((ts, need_null)) => {
                    if need_null {
                        output.push_null();
                    } else {
                        output.push(ts);
                    }
                }
                Err(_) => {
                    output.push_null();
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, DateType, _, _>(
        "to_date",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<DateType>>(
            |date_string, format, output, ctx| {
                if format.is_empty() {
                    output.push_null();
                } else {
                    let format = if ctx.func_ctx.date_format_style == *"oracle" {
                        pg_format_to_strftime(format)
                    } else {
                        format.to_string()
                    };
                    match NaiveDate::parse_from_str(date_string, &format) {
                        Ok(res) => {
                            output.push(res.num_days_from_ce() - EPOCH_DAYS_FROM_CE);
                        }
                        Err(e) => {
                            ctx.set_error(output.len(), e.to_string());
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );
    registry.register_combine_nullable_2_arg::<StringType, StringType, DateType, _, _>(
        "try_to_date",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, NullableType<DateType>>(
            |date, format, output, ctx| {
                if format.is_empty() {
                    output.push_null();
                } else {
                    let format = if ctx.func_ctx.date_format_style == *"oracle" {
                        pg_format_to_strftime(format)
                    } else {
                        format.to_string()
                    };
                    match NaiveDate::parse_from_str(date, &format) {
                        Ok(res) => {
                            output.push(res.num_days_from_ce() - EPOCH_DAYS_FROM_CE);
                        }
                        Err(_) => {
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );
}

fn string_to_format_datetime(
    timestamp: &str,
    format: &str,
    ctx: &mut EvalContext,
    parse_timestamp: bool,
) -> Result<(i64, bool), Box<ErrorCode>> {
    if format.is_empty() {
        return Ok((0, true));
    }

    let raw_format = format;
    let format = if ctx.func_ctx.date_format_style == *"oracle" {
        pg_format_to_strftime(format)
    } else {
        format.to_string()
    };

    let (mut tm, offset) = BrokenDownTime::parse_prefix(&format, timestamp)
        .map_err(|err| Box::new(ErrorCode::BadArguments(format!("{err}"))))?;
    let parsed_unix_timestamp = tm.timestamp();
    let had_explicit_time = tm.hour().is_some() || tm.minute().is_some() || tm.second().is_some();
    let had_civil_date = tm.year().is_some()
        || tm.month().is_some()
        || tm.day().is_some()
        || tm.day_of_year().is_some()
        || tm.iso_week_year().is_some()
        || tm.iso_week().is_some()
        || tm.sunday_based_week().is_some()
        || tm.monday_based_week().is_some()
        || tm.weekday().is_some();
    let had_subsecond = tm.subsec_nanosecond().is_some();
    let had_meridiem = tm.meridiem().is_some();
    let had_timezone = tm.offset().is_some() || tm.iana_time_zone().is_some();

    if !ctx.func_ctx.parse_datetime_ignore_remainder && offset != timestamp.len() {
        return Err(Box::new(ErrorCode::BadArguments(format!(
            "Can not fully parse timestamp {timestamp} by format {format}",
        ))));
    }

    if tm.hour().is_none() {
        let _ = tm.set_hour(Some(0));
    }
    if tm.minute().is_none() {
        let _ = tm.set_minute(Some(0));
    }
    if tm.second().is_none() {
        let _ = tm.set_second(Some(0));
    }

    // Jiff 0.2.16 requires a complete civil date when converting to a datetime.
    // To preserve historical to_timestamp() behaviour (which accepted inputs
    // like `%s,%Y`), synthesize missing date parts when we're parsing a
    // timestamp, but only when there isn't already alternate date information
    // (e.g. ISO week fields) present. Non-timestamp callers can still opt-in by
    // disabling the strict parser.
    if needs_civil_date_synthesis(&tm, ctx, parse_timestamp) {
        if tm.day().is_none() {
            let _ = tm.set_day(Some(1));
        }
        if tm.month().is_none() {
            let _ = tm.set_month(Some(1));
        }
        if parse_timestamp && tm.year().is_none() {
            let _ = tm.set_year(Some(1970));
        }
    }

    if parse_timestamp && parsed_unix_timestamp.is_some() {
        let has_conflicting_directives =
            had_civil_date || had_explicit_time || had_subsecond || had_meridiem || had_timezone;
        if has_conflicting_directives {
            return Err(Box::new(ErrorCode::BadArguments(format!(
                "Can't parse '{timestamp}' as timestamp with format '{raw_format}'"
            ))));
        }

        // When `%s` is present the parsed Unix timestamp already encodes the full
        // instant, so return it directly instead of trying to synthesize a civil
        // date (which would lose the seconds component).
        return Ok((parsed_unix_timestamp.unwrap().as_microsecond(), false));
    }

    if parse_timestamp
        && parsed_unix_timestamp.is_none()
        && tm.offset().is_none()
        && tm.iana_time_zone().is_none()
    {
        if let Some(micros) = fast_timestamp_from_tm(&tm, &ctx.func_ctx.tz) {
            return Ok((micros, false));
        }
    }

    let z = if tm.offset().is_none() {
        if parse_timestamp {
            ctx.func_ctx.tz.to_zoned(tm.to_datetime().map_err(|err| {
                ErrorCode::BadArguments(format!("{timestamp} to datetime error {err}"))
            })?)
        } else {
            TimeZone::UTC.to_zoned(tm.to_datetime().map_err(|err| {
                ErrorCode::BadArguments(format!("{timestamp} to datetime error {err}"))
            })?)
        }
    } else {
        tm.to_zoned()
    }
    .map_err(|err| ErrorCode::BadArguments(format!("{err}")))?;
    Ok((z.timestamp().as_microsecond(), false))
}

fn needs_civil_date_synthesis(
    tm: &BrokenDownTime,
    ctx: &EvalContext,
    parse_timestamp: bool,
) -> bool {
    if parse_timestamp || !ctx.func_ctx.enable_strict_datetime_parser {
        !(tm.day_of_year().is_some()
            || tm.iso_week_year().is_some()
            || tm.iso_week().is_some()
            || tm.sunday_based_week().is_some()
            || tm.monday_based_week().is_some())
    } else {
        false
    }
}

fn fast_timestamp_from_tm(tm: &BrokenDownTime, tz: &TimeZone) -> Option<i64> {
    let year = i32::from(tm.year()?);
    let month: u8 = tm.month()?.try_into().ok()?;
    let day: u8 = tm.day()?.try_into().ok()?;
    let hour: u8 = tm.hour().unwrap_or(0).try_into().ok()?;
    let minute: u8 = tm.minute().unwrap_or(0).try_into().ok()?;
    let second: u8 = tm.second().unwrap_or(0).try_into().ok()?;
    let nanos = tm.subsec_nanosecond().unwrap_or(0);
    let micro = (nanos / 1_000).max(0) as u32;
    fast_utc_from_local(tz, year, month, day, hour, minute, second, micro)
}

fn register_date_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampType, _>(
        "to_timestamp",
        |_, domain| {
            int32_domain_to_timestamp_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_date_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, domain| {
            if let Some(domain) = int32_domain_to_timestamp_domain(domain) {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_date_to_timestamp),
    );

    fn eval_date_to_timestamp(val: Value<DateType>, ctx: &mut EvalContext) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<DateType, TimestampType>(|val, output, ctx| {
            match calc_date_to_timestamp(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_date_to_timestamp_tz(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampTzType, _>(
        "to_timestamp_tz",
        |_, domain| {
            int32_domain_to_timestamp_domain(domain)
                .and_then(|domain| timestamp_domain_to_timestamp_tz_domain(&domain))
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_date_to_timestamp_tz,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampTzType, _, _>(
        "try_to_timestamp_tz",
        |_, domain| {
            if let Some(domain) = int32_domain_to_timestamp_domain(domain)
                .and_then(|domain| timestamp_domain_to_timestamp_tz_domain(&domain))
            {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_date_to_timestamp_tz),
    );

    fn eval_date_to_timestamp_tz(
        val: Value<DateType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampTzType> {
        vectorize_with_builder_1_arg::<DateType, TimestampTzType>(|val, output, ctx| {
            let (i, ts) = match calc_date_to_timestamp(val, &ctx.func_ctx.tz).and_then(|i| {
                Timestamp::from_microsecond(i)
                    .map_err(|err| err.to_string())
                    .map(|ts| (i, ts))
            }) {
                Ok(ts) => ts,
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(timestamp_tz::default());
                    return;
                }
            };
            let offset = ctx.func_ctx.tz.to_offset(ts);
            let ts_tz = timestamp_tz::new(i, offset.seconds());

            output.push(ts_tz)
        })(val, ctx)
    }
}

fn register_timestamp_to_timestamp_tz(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampTzType, _>(
        "to_timestamp_tz",
        |_, domain| {
            timestamp_domain_to_timestamp_tz_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_timestamp_to_timestamp_tz,
    );
    registry.register_combine_nullable_1_arg::<TimestampType, TimestampTzType, _, _>(
        "try_to_timestamp_tz",
        |_, domain| {
            if let Some(domain) = timestamp_domain_to_timestamp_tz_domain(domain) {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_timestamp_to_timestamp_tz),
    );

    fn eval_timestamp_to_timestamp_tz(
        val: Value<TimestampType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampTzType> {
        vectorize_with_builder_1_arg::<TimestampType, TimestampTzType>(|val, output, ctx| {
            if let Some(components) = fast_components_from_timestamp(val, &ctx.func_ctx.tz) {
                let offset = components.offset_seconds;
                let ts_tz = timestamp_tz::new(val - (offset as i64 * MICROS_PER_SEC), offset);
                output.push(ts_tz);
                return;
            }

            let ts = match Timestamp::from_microsecond(val) {
                Ok(ts) => ts,
                Err(err) => {
                    ctx.set_error(output.len(), err.to_string());
                    output.push(timestamp_tz::default());
                    return;
                }
            };
            let offset = ctx.func_ctx.tz.to_offset(ts);
            let ts_tz = timestamp_tz::new(
                val - (offset.seconds() as i64 * 1_000_000),
                offset.seconds(),
            );

            output.push(ts_tz)
        })(val, ctx)
    }
}

fn register_timestamp_tz_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<TimestampTzType, TimestampType, _>(
        "to_timestamp",
        |_, domain| {
            timestamp_tz_domain_to_timestamp_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        |val, _| val.timestamp(),
    );
}

fn register_number_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, TimestampType, _>(
        "to_timestamp",
        |_, domain| {
            int64_domain_to_timestamp_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_number_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "try_to_timestamp",
        |_, domain| {
            if let Some(domain) = int64_domain_to_timestamp_domain(domain) {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_number_to_timestamp),
    );

    registry.register_passthrough_nullable_2_arg::<Int64Type, UInt64Type, TimestampType, _, _>(
        "to_timestamp",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<Int64Type, UInt64Type, TimestampType>(
            |val, scale, output, _| {
                let mut n = val * 10i64.pow(6 - scale.clamp(0, 6) as u32);
                clamp_timestamp(&mut n);
                output.push(n)
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<Int64Type, UInt64Type, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<Int64Type, UInt64Type, TimestampType>(
            |val, scale, output, _| {
                let mut n = val * 10i64.pow(6 - scale.clamp(0, 6) as u32);
                clamp_timestamp(&mut n);
                output.push(n);
            },
        ),
    );

    fn eval_number_to_timestamp(
        val: Value<Int64Type>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output, _| {
            let ts = int64_to_timestamp(val);
            output.push(ts);
        })(val, ctx)
    }
}

fn register_string_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, DateType, _>(
        "to_date",
        |ctx, d| {
            let max = d.max.clone().unwrap_or_default();
            let mut res = Vec::with_capacity(2);
            for (i, v) in [&d.min, &max].iter().enumerate() {
                if i == 1 && d.max.is_none() {
                    // the max domain is unbounded
                    res.push(DATE_MAX);
                    break;
                }

                let mut extend_num = 0;
                let mut d = string_to_date(v, &ctx.tz);
                if d.is_err() && v.len() > 10 {
                    d = string_to_date(&v[0..10], &ctx.tz);
                    if i == 0 {
                        extend_num = -1;
                    } else {
                        extend_num = 1;
                    }
                }

                if d.is_err() {
                    return FunctionDomain::MayThrow;
                }
                let days = d
                    .unwrap()
                    .since((Unit::Day, date(1970, 1, 1)))
                    .unwrap()
                    .get_days();
                res.push(days + extend_num);
            }

            FunctionDomain::Domain(SimpleDomain {
                min: res[0].clamp(DATE_MIN, DATE_MAX),
                max: res[1].clamp(DATE_MIN, DATE_MAX),
            })
        },
        eval_string_to_date,
    );
    registry.register_combine_nullable_1_arg::<StringType, DateType, _, _>(
        "try_to_date",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_date),
    );

    fn eval_string_to_date(val: Value<StringType>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<StringType, DateType>(|val, output, ctx| {
            let mut d = string_to_date(val, &ctx.func_ctx.tz);
            if !ctx.func_ctx.enable_strict_datetime_parser {
                d = match catch_unwind(|| {
                    d.or_else(|_| {
                        parse(val)
                            .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
                            .and_then(|(naive_dt, _)| {
                                string_to_date(naive_dt.to_string(), &ctx.func_ctx.tz)
                            })
                    })
                }) {
                    Ok(result) => result,
                    Err(_) => Err(ErrorCode::BadArguments(format!(
                        "Date '{}' is not recognized.",
                        val
                    ))),
                }
            }

            match d {
                Ok(d) => match d.since((Unit::Day, date(1970, 1, 1))) {
                    Ok(s) => output.push(s.get_days()),
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot parse to type `DATE`. {}", e));
                        output.push(0);
                    }
                },
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `DATE`. {}", e));
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_timestamp_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _>(
        "to_date",
        |ctx, domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: calc_timestamp_to_date(domain.min, &ctx.tz),
                max: calc_timestamp_to_date(domain.max, &ctx.tz),
            })
        },
        eval_timestamp_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampType, DateType, _, _>(
        "try_to_date",
        |ctx, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain {
                    min: calc_timestamp_to_date(domain.min, &ctx.tz),
                    max: calc_timestamp_to_date(domain.max, &ctx.tz),
                })),
            })
        },
        error_to_null(eval_timestamp_to_date),
    );

    fn eval_timestamp_to_date(val: Value<TimestampType>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<TimestampType, DateType>(|val, output, ctx| {
            output.push(timestamp_to_date_days(val, &ctx.func_ctx.tz));
        })(val, ctx)
    }
    fn calc_timestamp_to_date(val: i64, tz: &TimeZone) -> i32 {
        timestamp_to_date_days(val, tz)
    }
}

fn timestamp_to_date_days(value: i64, tz: &TimeZone) -> i32 {
    timestamp_days_via_lut(value, tz).unwrap_or_else(|| timestamp_days_via_jiff(value, tz))
}

fn timestamp_days_via_lut(value: i64, tz: &TimeZone) -> Option<i32> {
    let components = fast_components_from_timestamp(value, tz)?;
    days_from_components(components.year, components.month, components.day)
}

fn days_from_components(year: i32, month: u8, day: u8) -> Option<i32> {
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
        .map(|d| clamp_date((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i64))
}

fn timestamp_days_via_jiff(value: i64, tz: &TimeZone) -> i32 {
    value
        .to_timestamp(tz)
        .date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

fn register_timestamp_tz_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampTzType, DateType, _>(
        "to_date",
        |_ctx, domain| {
            let (Ok(min), Ok(max)) = (
                calc_timestamp_tz_to_date(domain.min),
                calc_timestamp_tz_to_date(domain.max),
            ) else {
                return FunctionDomain::MayThrow;
            };

            FunctionDomain::Domain(SimpleDomain { min, max })
        },
        eval_timestamp_tz_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampTzType, DateType, _, _>(
        "try_to_date",
        |_ctx, domain| {
            let (Ok(min), Ok(max)) = (
                calc_timestamp_tz_to_date(domain.min),
                calc_timestamp_tz_to_date(domain.max),
            ) else {
                return FunctionDomain::MayThrow;
            };

            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain { min, max })),
            })
        },
        error_to_null(eval_timestamp_tz_to_date),
    );

    fn eval_timestamp_tz_to_date(
        val: Value<TimestampTzType>,
        ctx: &mut EvalContext,
    ) -> Value<DateType> {
        vectorize_with_builder_1_arg::<TimestampTzType, DateType>(|val, output, ctx| {
            match calc_timestamp_tz_to_date(val) {
                Ok(i) => {
                    output.push(i);
                }
                Err(err) => {
                    ctx.set_error(output.len(), err);
                }
            }
        })(val, ctx)
    }

    fn calc_timestamp_tz_to_date(val: timestamp_tz) -> Result<i32, String> {
        if let Some(days) = timestamp_tz_components_via_lut(val)
            .and_then(|c| days_from_components(c.year, c.month, c.day))
        {
            Ok(days)
        } else {
            let offset =
                Offset::from_seconds(val.seconds_offset()).map_err(|err| err.to_string())?;

            Ok(val
                .timestamp()
                .to_timestamp(&TimeZone::fixed(offset))
                .date()
                .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                .unwrap()
                .get_days())
        }
    }
}

fn register_number_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, DateType, _>(
        "to_date",
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            if overflowing {
                FunctionDomain::MayThrow
            } else {
                FunctionDomain::Domain(domain)
            }
        },
        eval_number_to_date,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, DateType, _, _>(
        "try_to_date",
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            FunctionDomain::Domain(NullableDomain {
                has_null: overflowing,
                value: Some(Box::new(domain)),
            })
        },
        error_to_null(eval_number_to_date),
    );

    fn eval_number_to_date(val: Value<Int64Type>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<Int64Type, DateType>(|val, output, _| {
            output.push(clamp_date(val))
        })(val, ctx)
    }
}

fn register_to_string(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_string", &["date_format", "strftime", "to_char"]);
    registry.register_combine_nullable_2_arg::<TimestampType, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, StringType, NullableType<StringType>>(
            |micros, format, output, ctx| {
                let ts = micros.to_timestamp(&ctx.func_ctx.tz);
                let format = prepare_format_string(format, &ctx.func_ctx.date_format_style);
                let mut buf = String::new();
                let mut formatter = fmt::Formatter::new(&mut buf, FormattingOptions::new());
                if Display::fmt(&ts.strftime(&format), &mut formatter).is_err() {
                    ctx.set_error(output.len(), format!("{format} is invalid time format"));
                    output.builder.commit_row();
                    output.validity.push(true);
                    return;
                }
                match write!(output.builder.row_buffer, "{}", buf) {
                    Ok(_) => {
                        output.builder.commit_row();
                        output.validity.push(true);
                    }
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("{format} is invalid time format, error {e}"),
                        );
                        output.builder.commit_row();
                        output.validity.push(true);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, StringType>(|val, output, ctx| {
            write!(
                output.row_buffer,
                "{}",
                date_to_string(val, &ctx.func_ctx.tz)
            )
            .unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampType, StringType>(|val, output, ctx| {
            write!(
                output.row_buffer,
                "{}",
                timestamp_to_string(val, &ctx.func_ctx.tz)
            )
            .unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampTzType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampTzType, StringType>(|val, output, _ctx| {
            write!(output.row_buffer, "{}", val).unwrap();
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<DateType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<DateType, NullableType<StringType>>(|val, output, ctx| {
            write!(
                output.builder.row_buffer,
                "{}",
                date_to_string(val, &ctx.func_ctx.tz)
            )
            .unwrap();
            output.builder.commit_row();
            output.validity.push(true);
        }),
    );

    registry.register_combine_nullable_1_arg::<TimestampType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<TimestampType, NullableType<StringType>>(
            |val, output, ctx| {
                write!(
                    output.builder.row_buffer,
                    "{}",
                    timestamp_to_string(val, &ctx.func_ctx.tz)
                )
                .unwrap();
                output.builder.commit_row();
                output.validity.push(true);
            },
        ),
    );
}

fn register_to_number(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<DateType, NumberType<i64>, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(domain.overflow_cast().0),
        |val, _| val as i64,
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, NumberType<i64>, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(*domain),
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(scalar),
            Value::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<DateType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.overflow_cast().0)),
            })
        },
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(Some(scalar as i64)),
            Value::Column(col) => Value::Column(NullableColumn::new_unchecked(
                col.iter().map(|val| *val as i64).collect(),
                Bitmap::new_constant(true, col.len()),
            )),
        },
    );

    registry.register_combine_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(*domain)),
            })
        },
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new_unchecked(col, validity))
            }
        },
    );
}

fn register_year_arith_function(
    registry: &mut FunctionRegistry,
    name: &'static str,
    delta_sign: i64,
) {
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(
            move |date, delta, builder, ctx| match EvalYearsImpl::eval_date(
                date,
                &ctx.func_ctx.tz,
                delta * delta_sign,
                false,
            ) {
                Ok(t) => builder.push(t),
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(0);
                }
            },
        ),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            move |ts, delta, builder, ctx| match EvalYearsImpl::eval_timestamp(
                ts,
                &ctx.func_ctx.tz,
                delta * delta_sign,
                false,
            ) {
                Ok(t) => builder.push(t),
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(0);
                }
            },
        ),
    );
}

fn register_month_based_arith_function(
    registry: &mut FunctionRegistry,
    name: &'static str,
    month_multiplier: i64,
    keep_end_of_month: bool,
) {
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(
            move |date, delta, builder, ctx| match EvalMonthsImpl::eval_date(
                date,
                &ctx.func_ctx.tz,
                delta * month_multiplier,
                keep_end_of_month,
            ) {
                Ok(t) => builder.push(t),
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(0);
                }
            },
        ),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            move |ts, delta, builder, ctx| match EvalMonthsImpl::eval_timestamp(
                ts,
                &ctx.func_ctx.tz,
                delta * month_multiplier,
                keep_end_of_month,
            ) {
                Ok(t) => builder.push(t),
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(0);
                }
            },
        ),
    );
}

fn register_day_based_arith_function(
    registry: &mut FunctionRegistry,
    name: &'static str,
    day_multiplier: i64,
) {
    registry.register_2_arg::<DateType, Int64Type, DateType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |date, delta, _| EvalDaysImpl::eval_date(date, delta * day_multiplier),
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |ts, delta, _| EvalDaysImpl::eval_timestamp(ts, delta * day_multiplier),
    );
}

fn register_time_arith_function(
    registry: &mut FunctionRegistry,
    name: &'static str,
    delta_sign: i64,
    factor: i64,
) {
    registry.register_2_arg::<DateType, Int64Type, TimestampType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |date, delta, _| {
            let val = (date as i64) * 24 * 3600 * MICROS_PER_SEC;
            EvalTimesImpl::eval_timestamp(val, delta * delta_sign, factor)
        },
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |ts, delta, _| EvalTimesImpl::eval_timestamp(ts, delta * delta_sign, factor),
    );
}

fn register_add_functions(registry: &mut FunctionRegistry) {
    register_year_arith_function(registry, "add_years", 1);
    register_month_based_arith_function(registry, "add_quarters", 3, false);
    register_month_based_arith_function(registry, "date_add_months", 1, false);
    // For both ADD_MONTHS and DATEADD, if the result month has fewer days than the original day, the result day of the month is the last day of the result month.
    // For ADD_MONTHS only, if the original day is the last day of the month, the result day of month will be the last day of the result month.
    register_month_based_arith_function(registry, "add_months", 1, true);
    register_day_based_arith_function(registry, "add_days", 1);
    register_day_based_arith_function(registry, "add_weeks", 7);
    register_time_arith_function(registry, "add_hours", 1, FACTOR_HOUR);
    register_time_arith_function(registry, "add_minutes", 1, FACTOR_MINUTE);
    register_time_arith_function(registry, "add_seconds", 1, FACTOR_SECOND);
}

fn register_sub_functions(registry: &mut FunctionRegistry) {
    register_year_arith_function(registry, "subtract_years", -1);
    register_month_based_arith_function(registry, "subtract_quarters", -3, false);
    register_month_based_arith_function(registry, "date_subtract_months", -1, false);
    register_month_based_arith_function(registry, "subtract_months", -1, true);
    register_day_based_arith_function(registry, "subtract_days", -1);
    register_day_based_arith_function(registry, "subtract_weeks", -7);
    register_time_arith_function(registry, "subtract_hours", -1, FACTOR_HOUR);
    register_time_arith_function(registry, "subtract_minutes", -1, FACTOR_MINUTE);
    register_time_arith_function(registry, "subtract_seconds", -1, FACTOR_SECOND);
}

fn register_diff_functions(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_years",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalYearsImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_years as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_years",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalYearsImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_years);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_quarters",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalQuartersImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_years as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_quarters",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalQuartersImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_years);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_months",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_months =
                    EvalMonthsImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_months as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_months",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_months =
                    EvalMonthsImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff_months);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_weeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_years = EvalWeeksImpl::eval_date_diff(date_start, date_end);
                builder.push(diff_years as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_weeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_years = EvalWeeksImpl::eval_timestamp_diff(date_start, date_end);
                builder.push(diff_years);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_days",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_days = EvalDaysImpl::eval_date_diff(date_start, date_end);
                builder.push(diff_days as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_days",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_days = EvalDaysImpl::eval_timestamp_diff(date_start, date_end);
                builder.push(diff_days);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_hours",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_hours =
                    EvalTimesImpl::eval_timestamp_diff(date_start, date_end, FACTOR_HOUR);
                builder.push(diff_hours);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_minutes",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_minutes =
                    EvalTimesImpl::eval_timestamp_diff(date_start, date_end, FACTOR_MINUTE);
                builder.push(diff_minutes);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_seconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_seconds =
                    EvalTimesImpl::eval_timestamp_diff(date_start, date_end, FACTOR_SECOND);
                builder.push(diff_seconds);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_microseconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_microseconds =
                    EvalDaysImpl::eval_date_diff(date_start, date_end) as i64 * MICROSECS_PER_DAY;
                builder.push(diff_microseconds);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_microseconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                builder.push(date_end - date_start);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_yearweeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff =
                    EvalYearWeeksImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_yearweeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff =
                    EvalYearWeeksImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_isoyears",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff = EvalISOYearsImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_isoyears",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff =
                    EvalISOYearsImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(diff);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "diff_millenniums",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalYearsImpl::eval_date_diff(date_start, date_end, &ctx.func_ctx.tz);
                builder.push((diff_years / 1000) as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_millenniums",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let diff_years =
                    EvalYearsImpl::eval_timestamp_diff(date_start, date_end, &ctx.func_ctx.tz);

                builder.push(diff_years / 1000);
            },
        ),
    );
    registry.register_aliases("diff_seconds", &["diff_epochs"]);
    registry.register_aliases("diff_days", &["diff_dows", "diff_isodows", "diff_doys"]);

    registry.register_2_arg::<DateType, DateType, Int32Type, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_2_arg::<TimestampType, TimestampType, IntervalType, _>(
        "timestamp_diff",
        |_, _, _| FunctionDomain::MayThrow,
        |a, b, _| months_days_micros::new(0, 0, a - b),
    );

    registry.register_2_arg::<TimestampType, TimestampType, Int64Type, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Float64Type, _, _>(
        "months_between",
        |_, lhs, rhs| {
            let lm = lhs.max;
            let ln = lhs.min;
            let rm = rhs.max;
            let rn = rhs.min;

            let min = EvalMonthsImpl::months_between(ln, rm);
            let max = EvalMonthsImpl::months_between(lm, rn);
            FunctionDomain::Domain(SimpleDomain::<F64> {
                min: min.into(),
                max: max.into(),
            })
        },
        vectorize_2_arg::<DateType, DateType, Float64Type>(|a, b, _ctx| {
            EvalMonthsImpl::months_between(a, b).into()
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Float64Type, _, _>(
            "months_between",
            |_, lhs, rhs| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                FunctionDomain::Domain(SimpleDomain::<F64> {
                    min: EvalMonthsImpl::months_between_ts(ln, rm).into(),
                    max: EvalMonthsImpl::months_between_ts(lm, rn).into(),
                })
            },
            vectorize_2_arg::<TimestampType, TimestampType, Float64Type>(|a, b, _ctx| {
                EvalMonthsImpl::months_between_ts(a, b).into()
            }),
        );
}

fn register_between_functions(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_years",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_years =
                    EvalYearsImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_years as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_years",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_years =
                    EvalYearsImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_years);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_quarters",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_quarters =
                    EvalMonthsImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz) / 3;
                builder.push(between_quarters as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_quarters",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_quarters =
                    EvalMonthsImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz)
                        / 3;
                builder.push(between_quarters);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_months",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_months =
                    EvalMonthsImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_months as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_months",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_months =
                    EvalMonthsImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_months);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_weeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_weeks =
                    EvalWeeksImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_weeks as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_weeks",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_weeks =
                    EvalWeeksImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_weeks);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_days",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                // day is date type unit
                let between_days = EvalDaysImpl::eval_date_diff(date_start, date_end);
                builder.push(between_days as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_days",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_days =
                    EvalDaysImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_days);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_hours",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_hours = EvalDaysImpl::eval_date_diff(date_start, date_end) as i64 * 24;
                builder.push(between_hours);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_hours",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_hours =
                    EvalTimesImpl::eval_timestamp_between("hours", date_start, date_end);
                builder.push(between_hours);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_minutes",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_minutes =
                    EvalDaysImpl::eval_date_diff(date_start, date_end) as i64 * 24 * 60;
                builder.push(between_minutes);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_minutes",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_minutes =
                    EvalTimesImpl::eval_timestamp_between("minutes", date_start, date_end);
                builder.push(between_minutes);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_seconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_seconds =
                    EvalDaysImpl::eval_date_diff(date_start, date_end) as i64 * 24 * 3600;
                builder.push(between_seconds);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_seconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_seconds =
                    EvalTimesImpl::eval_timestamp_between("seconds", date_start, date_end);
                builder.push(between_seconds);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_microseconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, _| {
                let between_microseconds =
                    EvalDaysImpl::eval_date_diff(date_start, date_end) as i64 * MICROSECS_PER_DAY;
                builder.push(between_microseconds);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_microseconds",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                builder.push(date_end - date_start);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_isoyears",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_isoyears =
                    EvalISOYearsImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push(between_isoyears as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_isoyears",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_isoyears = EvalISOYearsImpl::eval_timestamp_between(
                    date_start,
                    date_end,
                    &ctx.func_ctx.tz,
                );
                builder.push(between_isoyears);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, DateType, Int64Type, _, _>(
        "between_millenniums",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, DateType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_millenniums =
                    EvalYearsImpl::eval_date_between(date_start, date_end, &ctx.func_ctx.tz);
                builder.push((between_millenniums / 1000) as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "between_millenniums",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, ctx| {
                let between_millenniums =
                    EvalYearsImpl::eval_timestamp_between(date_start, date_end, &ctx.func_ctx.tz);

                builder.push(between_millenniums / 1000);
            },
        ),
    );
    registry.register_aliases("between_seconds", &["between_epochs"]);
    registry.register_aliases("between_weeks", &["between_yearweeks"]);
    registry.register_aliases("between_days", &[
        "between_dows",
        "between_isodows",
        "between_doys",
    ]);
}

fn normalize_time_precision(raw: i64) -> Result<u8, String> {
    if (0..=9).contains(&raw) {
        Ok(raw as u8)
    } else {
        Err(format!(
            "Invalid fractional seconds precision `{raw}` for `current_time` (expect 0-9)"
        ))
    }
}

fn current_time_string(func_ctx: &FunctionContext, precision: Option<u8>) -> String {
    let datetime = func_ctx.now.with_time_zone(func_ctx.tz.clone()).datetime();
    let nanos = datetime.subsec_nanosecond() as u32;
    let mut value = format!(
        "{:02}:{:02}:{:02}",
        datetime.hour(),
        datetime.minute(),
        datetime.second()
    );

    let precision = precision.unwrap_or(9).min(9);
    if precision > 0 {
        let divisor = 10_u32.pow(9 - precision as u32);
        let truncated = nanos / divisor;
        let frac = format!("{:0width$}", truncated, width = precision as usize);
        value.push('.');
        value.push_str(&frac);
    }

    value
}

fn register_real_time_functions(registry: &mut FunctionRegistry) {
    registry.register_aliases("now", &["current_timestamp"]);
    registry.register_aliases("today", &["current_date"]);

    registry.properties.insert(
        "now".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "current_time".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "today".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "yesterday".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "tomorrow".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    for name in &[
        "to_timestamp",
        "to_timestamp_tz",
        "to_date",
        "to_yyyymm",
        "to_yyyymmdd",
    ] {
        registry
            .properties
            .insert(name.to_string(), FunctionProperty::default().monotonicity());
    }

    registry.properties.insert(
        "to_string".to_string(),
        FunctionProperty::default()
            .monotonicity_type(DataType::Timestamp)
            .monotonicity_type(DataType::Timestamp.wrap_nullable()),
    );

    registry.properties.insert(
        "to_string".to_string(),
        FunctionProperty::default()
            .monotonicity_type(DataType::Date)
            .monotonicity_type(DataType::Date.wrap_nullable()),
    );

    registry.register_0_arg_core::<TimestampType, _>(
        "now",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(ctx.func_ctx.now.timestamp().as_microsecond()),
    );

    registry.register_0_arg_core::<StringType, _>(
        "current_time",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(current_time_string(ctx.func_ctx, None)),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, StringType, _>(
        "current_time",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, StringType>(|precision, output, ctx| {
            match normalize_time_precision(precision) {
                Ok(valid_precision) => {
                    output.put_and_commit(current_time_string(ctx.func_ctx, Some(valid_precision)));
                }
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.commit_row();
                }
            }
        }),
    );

    registry.register_0_arg_core::<DateType, _>(
        "today",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz)),
    );

    registry.register_0_arg_core::<DateType, _>(
        "yesterday",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz) - 1),
    );

    registry.register_0_arg_core::<DateType, _>(
        "tomorrow",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz) + 1),
    );
}

fn register_to_number_functions(registry: &mut FunctionRegistry) {
    // date
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt32Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToYYYYMM, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt32Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToYYYYMMDD, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt16Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToYear, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_iso_year",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt16Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToISOYear, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt8Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToQuarter, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt8Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToMonth, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt16Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToDayOfYear, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt8Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToDayOfMonth, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt8Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToDayOfWeek, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "dayofweek",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt8Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<DayOfWeek, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "yearweek",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt32Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToYYYYWW, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "millennium",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt16Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToMillennium, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, UInt32Type>(|val, output, ctx| {
            match ToNumberImpl::eval_date::<ToWeekOfYear, _>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    // timestamp
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMM, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDD, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhh",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDDHH, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhhmmss",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDDHHMMSS, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYear, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_iso_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToISOYear, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToQuarter, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToMonth, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfYear, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfMonth, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfWeek, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "dayofweek",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<DayOfWeek, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "yearweek",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYWW, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "millennium",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToMillennium, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToWeekOfYear, _>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, Int64Type, _>(
        "to_unix_timestamp",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToUnixTimestamp, _>(val, &ctx.func_ctx.tz),
    );

    registry.register_1_arg::<TimestampType, Float64Type, _>(
        "epoch",
        |_, domain| {
            FunctionDomain::Domain(SimpleDomain::<F64> {
                min: (domain.min as f64 / 1_000_000f64).into(),
                max: (domain.max as f64 / 1_000_000f64).into(),
            })
        },
        |val, _| (val as f64 / 1_000_000f64).into(),
    );

    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_hour",
        |_, _| FunctionDomain::Full,
        |val, ctx| {
            let datetime = val.to_timestamp(&ctx.func_ctx.tz);
            datetime.hour() as u8
        },
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_minute",
        |_, _| FunctionDomain::Full,
        |val, ctx| {
            let datetime = val.to_timestamp(&ctx.func_ctx.tz);
            datetime.minute() as u8
        },
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_second",
        |_, _| FunctionDomain::Full,
        |val, ctx| {
            let datetime = val.to_timestamp(&ctx.func_ctx.tz);
            datetime.second() as u8
        },
    );
}

fn register_timestamp_add_sub(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "plus",
        |_, lhs, rhs| {
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: clamp_date(ln + rn),
                    max: clamp_date(lm + rm),
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date((a as i64) + b))
        }),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "plus",
        |_, lhs, rhs| {
            {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                let mut min = ln + rn;
                clamp_timestamp(&mut min);
                let mut max = lm + rm;
                clamp_timestamp(&mut max);
                Some(FunctionDomain::Domain(SimpleDomain::<i64> { min, max }))
            }
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, _| {
                let mut sum = a + b;
                clamp_timestamp(&mut sum);
                output.push(sum);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: clamp_date(ln - rn),
                    max: clamp_date(lm - rm),
                }))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date((a as i64) - b));
        }),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "minus",
        |_, lhs, rhs| {
            {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                let mut min = ln - rn;
                clamp_timestamp(&mut min);
                let mut max = lm - rm;
                clamp_timestamp(&mut max);
                Some(FunctionDomain::Domain(SimpleDomain::<i64> { min, max }))
            }
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, _| {
                let mut minus = a - b;
                clamp_timestamp(&mut minus);
                output.push(minus);
            },
        ),
    );
}

fn register_rounder_functions(registry: &mut FunctionRegistry) {
    // timestamp -> timestamp
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_second",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::Second),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_minute",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::Minute),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_five_minutes",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::FiveMinutes),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_ten_minutes",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::TenMinutes),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_fifteen_minutes",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::FifteenMinutes),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_hour",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::Hour),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "to_start_of_day",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::Day),
    );
    registry.register_1_arg::<TimestampType, TimestampType, _>(
        "time_slot",
        |_, _| FunctionDomain::Full,
        |val, ctx| round_timestamp(val, &ctx.func_ctx.tz, Round::TimeSlot),
    );
    registry.register_passthrough_nullable_4_arg::<DateType, UInt64Type, StringType,StringType, DateType, _, _>(
        "time_slice",
        |_,_,_, _,_| FunctionDomain::Full,
        vectorize_with_builder_4_arg::<DateType, UInt64Type, StringType,StringType, DateType>(|ts, slice_length, start_or_end, part,output, ctx| {
            let start_or_end = StartOrEnd::from(start_or_end);
            let part = TimePart::from(part);
            if !part.date_part() {
                ctx.set_error(output.len(), "Date type only support Year | Quarter | Month | Week | Day".to_string());
                output.push(0);
            } else {
                let mode = ctx.func_ctx.week_start;
                let res = if mode == 0 {
                    time_slice_date(ts, slice_length, part, start_or_end, Weekday::Sunday)
                } else {
                    time_slice_date(ts, slice_length, part, start_or_end, Weekday::Monday)
                };
                output.push(res)
            }
        }),
    );
    registry.register_passthrough_nullable_4_arg::<TimestampType, UInt64Type, StringType,StringType, TimestampType, _, _>(
        "time_slice",
        |_,_,_, _,_| FunctionDomain::Full,
        vectorize_4_arg::<TimestampType, UInt64Type, StringType,StringType, TimestampType>(|ts, slice_length, start_or_end, part, ctx| {
            let start_or_end = StartOrEnd::from(start_or_end);
            let part = TimePart::from(part);
            let mode = ctx.func_ctx.week_start;
            if mode == 0 {
                time_slice_timestamp(ts, slice_length, part, start_or_end, Weekday::Sunday, &ctx.func_ctx.tz)
            } else {
                time_slice_timestamp(ts, slice_length, part, start_or_end, Weekday::Monday, &ctx.func_ctx.tz)
            }
        }),
    );

    // date | timestamp -> date
    registry.register_aliases("to_monday", &["to_start_of_iso_week"]);
    rounder_functions_helper::<ToLastMonday>(registry, "to_monday");
    rounder_functions_helper::<ToLastSunday>(registry, "to_start_of_week");
    rounder_functions_helper::<ToStartOfMonth>(registry, "to_start_of_month");
    rounder_functions_helper::<ToStartOfQuarter>(registry, "to_start_of_quarter");
    rounder_functions_helper::<ToStartOfYear>(registry, "to_start_of_year");
    rounder_functions_helper::<ToStartOfISOYear>(registry, "to_start_of_iso_year");
    rounder_functions_helper::<ToLastOfWeek>(registry, "to_last_of_week");
    rounder_functions_helper::<ToLastOfMonth>(registry, "to_last_of_month");
    rounder_functions_helper::<ToLastOfQuarter>(registry, "to_last_of_quarter");
    rounder_functions_helper::<ToLastOfYear>(registry, "to_last_of_year");
    rounder_functions_helper::<ToPreviousMonday>(registry, "to_previous_monday");
    rounder_functions_helper::<ToPreviousTuesday>(registry, "to_previous_tuesday");
    rounder_functions_helper::<ToPreviousWednesday>(registry, "to_previous_wednesday");
    rounder_functions_helper::<ToPreviousThursday>(registry, "to_previous_thursday");
    rounder_functions_helper::<ToPreviousFriday>(registry, "to_previous_friday");
    rounder_functions_helper::<ToPreviousSaturday>(registry, "to_previous_saturday");
    rounder_functions_helper::<ToPreviousSunday>(registry, "to_previous_sunday");
    rounder_functions_helper::<ToNextMonday>(registry, "to_next_monday");
    rounder_functions_helper::<ToNextTuesday>(registry, "to_next_tuesday");
    rounder_functions_helper::<ToNextWednesday>(registry, "to_next_wednesday");
    rounder_functions_helper::<ToNextThursday>(registry, "to_next_thursday");
    rounder_functions_helper::<ToNextFriday>(registry, "to_next_friday");
    rounder_functions_helper::<ToNextSaturday>(registry, "to_next_saturday");
    rounder_functions_helper::<ToNextSunday>(registry, "to_next_sunday");

    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|val, mode, output, ctx| {
            if mode == 0 {
                match DateRounder::eval_date::<ToLastSunday>(val, &ctx.func_ctx.tz) {
                    Ok(t) => output.push(t),
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                        output.push(0);
                    }
                }
            } else {
                match DateRounder::eval_date::<ToLastMonday>(val, &ctx.func_ctx.tz) {
                    Ok(t) => output.push(t),
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                        output.push(0);
                    }
                }
            }
        }),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<TimestampType, Int64Type, DateType>(|val, mode, ctx| {
            if mode == 0 {
                DateRounder::eval_timestamp::<ToLastSunday>(val, &ctx.func_ctx.tz)
            } else {
                DateRounder::eval_timestamp::<ToLastMonday>(val, &ctx.func_ctx.tz)
            }
        }),
    );
}

fn rounder_functions_helper<T>(registry: &mut FunctionRegistry, name: &str)
where T: ToNumber<i32> {
    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _>(
        name,
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, DateType>(|val, output, ctx| {
            match DateRounder::eval_date::<T>(val, &ctx.func_ctx.tz) {
                Ok(t) => output.push(t),
                Err(e) => {
                    ctx.set_error(output.len(), format!("cannot parse to type `Date`. {}", e));
                    output.push(0);
                }
            }
        }),
    );
    registry.register_1_arg::<TimestampType, DateType, _>(
        name,
        |_, _| FunctionDomain::Full,
        |val, ctx| DateRounder::eval_timestamp::<T>(val, &ctx.func_ctx.tz),
    );
}

fn prepare_format_string(format: &str, date_format_style: &str) -> String {
    let processed_format = if date_format_style == "oracle" {
        pg_format_to_strftime(format)
    } else {
        format.to_string()
    };
    replace_time_format(&processed_format).to_string()
}
