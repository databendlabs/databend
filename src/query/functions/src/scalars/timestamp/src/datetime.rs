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

use chrono::Datelike;
use chrono::NaiveDate;
use databend_common_base::runtime::catch_unwind;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::error_to_null;
use databend_common_expression::serialize::EPOCH_DAYS_FROM_CE;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_date;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_timestamp;
use databend_common_expression::utils::auto_detect_datetime::calc_int64_to_timestamp_domain;
use databend_common_expression::utils::auto_detect_datetime::fast_timestamp_from_tm;
use databend_common_expression::utils::auto_detect_datetime::int64_to_timestamp;
use databend_common_expression::utils::auto_detect_datetime::parse_epoch_str;
use databend_common_expression::utils::auto_detect_datetime::parse_timestamp_tz_with_auto;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_timezone::fast_components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;
use dtparse::parse;
use jiff::SignedDuration;
use jiff::Span;
use jiff::Timestamp;
use jiff::Unit;
use jiff::civil::Date;
use jiff::civil::date;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::Offset;
use jiff::tz::TimeZone;
use num_traits::AsPrimitive;

use crate::date_arithmetic::timestamp_tz_components_via_lut;
use crate::date_conversion::calc_date_to_timestamp;
use crate::date_format::pg_format_to_strftime;

const MONTHS_PER_YEAR: i64 = 12;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS timestamp)
    // to_timestamp(xx)
    registry.register_context_dependent(register_string_to_timestamp);
    registry.register_context_dependent(register_date_to_timestamp);
    registry.register_context_dependent(register_date_to_timestamp_tz);
    register_number_to_timestamp(registry);
    registry.register_context_dependent(register_timestamp_to_timestamp_tz);
    registry.register_context_dependent(register_timestamp_tz_to_timestamp);

    // cast(xx AS date)
    // to_date(xx)
    registry.register_context_dependent(register_string_to_date);
    registry.register_context_dependent(register_timestamp_to_date);
    registry.register_context_dependent(register_timestamp_tz_to_date);
    register_number_to_date(registry);

    // cast([date | timestamp] AS string)
    // to_string([date | timestamp])
    registry.register_context_dependent(crate::date_format::register);

    // cast([date | timestamp] AS [uint8 | int8 | ...])
    // to_[uint8 | int8 | ...]([date | timestamp])
    registry.register_context_dependent(crate::date_extract::register_cast);

    // [add | subtract]_[years | months | days | hours | minutes | seconds]([date | timestamp], number)
    // date_[add | sub]([year | quarter | month | week | day | hour | minute | second], [date | timestamp], number)
    // [date | timestamp] [+ | -] interval number [year | quarter | month | week | day | hour | minute | second]
    // date_diff([year | quarter | month | week | day | hour | minute | second], [date | timestamp], [date | timestamp])
    // [date | timestamp] +/- [date | timestamp]
    // datesub([year | quarter | month | week | day | hour | minute | second], [date | timestamp], [date | timestamp])
    // The number of complete partitions between the dates.
    registry.register_context_dependent(crate::date_arithmetic::register);

    // now, today, yesterday, tomorrow
    crate::date_conversion::register_real_time_functions(registry);

    // to_*([date | timestamp]) -> number
    registry.register_context_dependent(crate::date_extract::register);

    // to_*([date | timestamp]) -> [date | timestamp]
    registry.register_context_dependent(crate::date_round::register);

    // [date | timestamp] +/- number
    registry.register_context_dependent(crate::date_arithmetic::register_timestamp_add_sub);

    // convert_timezone( target_timezone, 'timestamp')
    registry.register_context_dependent(register_convert_timezone);

    // date_from_parts(year, month, day)
    // timestamp_from_parts(year, month, day, hour, minute, second [, nanosecond])
    // timestamp_tz_from_parts(year, month, day, hour, minute, second [, nanosecond] [, time_zone])
    register_date_from_parts(registry);
    registry.register_context_dependent(register_timestamp_from_parts);
    registry.register_context_dependent(register_timestamp_tz_from_parts);
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
                        let src_zoned = timestamp_from_micros(src_timestamp, source_tz);
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

/// Parse a string to timestamp with full fallback chain:
/// ISO -> epoch+auto -> dtparse.
#[allow(clippy::result_large_err)]
fn parse_string_to_timestamp(val: &str, func_ctx: &FunctionContext) -> Result<i64, ErrorCode> {
    // Layer 1: ISO parse
    let iso_err = match string_to_timestamp(val, &func_ctx.tz) {
        Ok(ts) => return Ok(ts.timestamp().as_microsecond()),
        Err(e) => e,
    };
    // Layer 2+3: Epoch detection + AUTO structured format detection
    if func_ctx.enable_auto_detect_datetime_format {
        if let Some(mut micros) = parse_epoch_str(val) {
            clamp_timestamp(&mut micros);
            return Ok(micros);
        }
        if let Some(micros) = auto_detect_timestamp(val, &func_ctx.tz) {
            return Ok(micros);
        }
    }
    // Layer 4: function-only dtparse fallback (not reused by VARIANT/COPY)
    if !func_ctx.enable_strict_datetime_parser {
        let dtparse_result = catch_unwind(|| {
            parse(val)
                .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
                .and_then(|(naive_dt, offset)| {
                    let naive_dt = match offset {
                        Some(off) => format!("{}{}", naive_dt, off),
                        None => naive_dt.to_string(),
                    };
                    string_to_timestamp(naive_dt, &func_ctx.tz)
                })
        })
        .unwrap_or_else(|_| {
            Err(ErrorCode::BadArguments(format!(
                "TIMESTAMP '{}' is not recognized.",
                val
            )))
        });
        match dtparse_result {
            Ok(ts) => return Ok(ts.timestamp().as_microsecond()),
            Err(e) => return Err(e),
        }
    }
    Err(iso_err)
}

/// Parse a string to date with full fallback chain:
/// ISO -> numeric-day+auto -> dtparse.
#[allow(clippy::result_large_err)]
fn parse_string_to_date(val: &str, func_ctx: &FunctionContext) -> Result<i32, ErrorCode> {
    // Layer 1: ISO parse
    let iso_err = match string_to_date(val, &func_ctx.tz) {
        Ok(d) => match d.since((Unit::Day, date(1970, 1, 1))) {
            Ok(s) => return Ok(s.get_days()),
            Err(e) => ErrorCode::BadArguments(format!("{}", e)),
        },
        Err(e) => e,
    };
    // Layer 2+3: Numeric day + AUTO structured format detection
    if func_ctx.enable_auto_detect_datetime_format {
        if let Ok(days) = val.parse::<i64>() {
            return Ok(clamp_date(days));
        }
        if let Some(days) = auto_detect_date(val) {
            return Ok(days);
        }
    }
    // Layer 4: function-only dtparse fallback (not reused by VARIANT/COPY)
    if !func_ctx.enable_strict_datetime_parser {
        let dtparse_result = catch_unwind(|| {
            parse(val)
                .map_err(|err| ErrorCode::BadArguments(format!("{err}")))
                .and_then(|(naive_dt, _)| string_to_date(naive_dt.to_string(), &func_ctx.tz))
        })
        .unwrap_or_else(|_| {
            Err(ErrorCode::BadArguments(format!(
                "Date '{}' is not recognized.",
                val
            )))
        });
        match dtparse_result {
            Ok(d) => match d.since((Unit::Day, date(1970, 1, 1))) {
                Ok(s) => return Ok(s.get_days()),
                Err(e) => return Err(ErrorCode::BadArguments(format!("{}", e))),
            },
            Err(e) => return Err(e),
        }
    }
    Err(iso_err)
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
                if d.is_err()
                    && v.len() > MAX_LEN
                    && let Some(prefix) = v.get(..MAX_LEN)
                {
                    d = string_to_timestamp(prefix, &ctx.tz);
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
            match parse_string_to_timestamp(val, ctx.func_ctx) {
                Ok(micros) => output.push(micros),
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
            match parse_timestamp_tz_with_auto(
                val,
                &ctx.func_ctx.tz,
                ctx.func_ctx.enable_auto_detect_datetime_format,
            ) {
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
                if d.is_err()
                    && v.len() > 10
                    && let Some(prefix) = v.get(..10)
                {
                    d = string_to_date(prefix, &ctx.tz);
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
            match parse_string_to_date(val, ctx.func_ctx) {
                Ok(days) => output.push(days),
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
    timestamp_from_micros(value, tz)
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

            Ok(
                timestamp_from_micros(val.timestamp(), &TimeZone::fixed(offset))
                    .date()
                    .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                    .unwrap()
                    .get_days(),
            )
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

fn normalize_date_parts(year: i64, month: i64, day: i64) -> std::result::Result<Date, String> {
    let month_offset = month
        .checked_sub(1)
        .ok_or_else(|| format!("Date parts out of bounds: year={year}, month={month}"))?;
    let total_months = year
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|y| y.checked_add(month_offset))
        .ok_or_else(|| format!("Date parts out of bounds: year={year}, month={month}"))?;

    let norm_year_i64 = total_months.div_euclid(MONTHS_PER_YEAR);
    let norm_month = (total_months.rem_euclid(MONTHS_PER_YEAR) + 1) as i8;
    let norm_year =
        i16::try_from(norm_year_i64).map_err(|_| format!("Year out of bounds: {norm_year_i64}"))?;

    let base = Date::new(norm_year, norm_month, 1)
        .map_err(|_| format!("Invalid date: year={year}, month={month}, day={day}"))?;
    let day_offset = day
        .checked_sub(1)
        .ok_or_else(|| format!("Day value out of bounds: {day}"))?;
    let days = Span::new()
        .try_days(day_offset)
        .map_err(|_| format!("Day value out of bounds: {day}"))?;

    base.checked_add(days)
        .map_err(|_| format!("Date out of range: year={year}, month={month}, day={day}"))
}

fn duration_from_time_parts(
    ctx: &mut EvalContext,
    row: usize,
    hour: i64,
    minute: i64,
    second: i64,
    nanosecond: i64,
) -> Option<SignedDuration> {
    let hour_duration = match SignedDuration::try_from_hours(hour) {
        Some(duration) => duration,
        None => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Timestamp hour component is out of range"),
            );
            return None;
        }
    };
    let minute_duration = match SignedDuration::try_from_mins(minute) {
        Some(duration) => duration,
        None => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Timestamp minute component is out of range"),
            );
            return None;
        }
    };

    match hour_duration
        .checked_add(minute_duration)
        .and_then(|d| d.checked_add(SignedDuration::from_secs(second)))
        .and_then(|d| d.checked_add(SignedDuration::from_nanos(nanosecond)))
    {
        Some(duration) => Some(duration),
        None => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Timestamp components overflow"),
            );
            None
        }
    }
}

fn validate_timestamp_bounds(ctx: &mut EvalContext, row: usize, utc_micros: i64) -> Option<i64> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&utc_micros) {
        Some(utc_micros)
    } else {
        ctx.set_error(
            row,
            ErrorCode::BadArguments(format!("Timestamp out of range: {utc_micros}")),
        );
        None
    }
}

fn timestamp_from_parts_to_micros(
    ctx: &mut EvalContext,
    row: usize,
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    nanosecond: i64,
    tz: &TimeZone,
) -> Option<i64> {
    let base_date = match normalize_date_parts(year, month, day) {
        Ok(date) => date,
        Err(e) => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments(format!("Cannot construct timestamp: {e}")),
            );
            return None;
        }
    };
    let duration = duration_from_time_parts(ctx, row, hour, minute, second, nanosecond)?;
    let local_dt = base_date
        .at(0, 0, 0, 0)
        .checked_add(duration)
        .map_err(|e| ErrorCode::BadArguments(format!("Cannot construct timestamp: {e}")));
    let local_dt = match local_dt {
        Ok(local_dt) => local_dt,
        Err(e) => {
            ctx.set_error(row, e);
            return None;
        }
    };

    if let Some(micros) = fast_utc_from_local(
        tz,
        local_dt.year() as i32,
        local_dt.month() as u8,
        local_dt.day() as u8,
        local_dt.hour() as u8,
        local_dt.minute() as u8,
        local_dt.second() as u8,
        (local_dt.subsec_nanosecond() / 1_000) as u32,
    ) {
        return validate_timestamp_bounds(ctx, row, micros);
    }

    match tz.to_zoned(local_dt) {
        Ok(zoned) => validate_timestamp_bounds(ctx, row, zoned.timestamp().as_microsecond()),
        Err(e) => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments(format!("Cannot construct timestamp: {e}")),
            );
            None
        }
    }
}

fn register_date_from_parts(registry: &mut FunctionRegistry) {
    registry.register_aliases("date_from_parts", &["datefromparts"]);

    registry
        .register_passthrough_nullable_3_arg::<Int64Type, Int64Type, Int64Type, DateType, _, _>(
            "date_from_parts",
            |_, _, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_3_arg::<Int64Type, Int64Type, Int64Type, DateType>(
                |year, month, day, output, ctx| match normalize_date_parts(year, month, day) {
                    Ok(date) => {
                        let days = date
                            .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                            .unwrap()
                            .get_days();
                        if (DATE_MIN as i64..=DATE_MAX as i64).contains(&(days as i64)) {
                            output.push(days);
                        } else {
                            ctx.set_error(output.len(), format!("Date out of range: {days}"));
                            output.push(0);
                        }
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), format!("cannot create date from parts: {e}"));
                        output.push(0);
                    }
                },
            ),
        );
}

fn register_timestamp_from_parts(registry: &mut FunctionRegistry) {
    registry.register_aliases("timestamp_from_parts", &["timestampfromparts"]);

    let factory = FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());
        let int64_type = DataType::Number(NumberDataType::Int64);

        let (sig_args, func): (
            Vec<DataType>,
            fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType>,
        ) = match args_type.len() {
            // 6 args: year, month, day, hour, minute, second
            6 => (vec![int64_type; 6], |args, ctx| {
                timestamp_from_parts_fn(args, ctx, false)
            }),

            // 7 args: includes nanosecond
            7 => (vec![int64_type; 7], |args, ctx| {
                timestamp_from_parts_fn(args, ctx, true)
            }),

            _ => return None,
        };

        let signature = FunctionSignature {
            name: "timestamp_from_parts".to_string(),
            args_type: sig_args,
            return_type: DataType::Timestamp,
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            func,
            None,
            has_null,
        )))
    }));

    registry.register_function_factory("timestamp_from_parts", factory);
}

fn register_timestamp_tz_from_parts(registry: &mut FunctionRegistry) {
    registry.register_aliases("timestamp_tz_from_parts", &["timestamptzfromparts"]);

    let factory = FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
        let has_null = args_type.iter().any(|t| t.is_nullable_or_null());
        let int64_type = DataType::Number(NumberDataType::Int64);

        let (sig_args, func): (
            Vec<DataType>,
            fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType>,
        ) = match args_type.len() {
            // 6 args: no nanoseconds, no timezone
            6 => (vec![int64_type; 6], |args, ctx| {
                timestamp_tz_from_parts_fn(args, ctx, false, false)
            }),

            // 7 args: timezone provided
            7 if args_type[6].remove_nullable() == DataType::String => {
                let mut v = vec![int64_type; 6];
                v.push(DataType::String);

                // year, month, day, hour, minute, second, timezone
                (v, |args, ctx| {
                    timestamp_tz_from_parts_fn(args, ctx, false, true)
                })
            }

            // 7 args: nanoseconds
            7 => (vec![int64_type; 7], |args, ctx| {
                timestamp_tz_from_parts_fn(args, ctx, true, false)
            }),

            // 8 args: nanoseconds + timezone
            8 => {
                let mut v = vec![int64_type; 7];
                v.push(DataType::String);

                // year, month, day, hour, minute, second, nanosecond, timezone
                (v, |args, ctx| {
                    timestamp_tz_from_parts_fn(args, ctx, true, true)
                })
            }

            _ => return None,
        };

        let signature = FunctionSignature {
            name: "timestamp_tz_from_parts".to_string(),
            args_type: sig_args,
            return_type: DataType::TimestampTz,
        };

        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::MayThrow,
            func,
            None,
            has_null,
        )))
    }));

    registry.register_function_factory("timestamp_tz_from_parts", factory);
}

fn build_timestamp_parts(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    has_nano: bool,
    has_tz: bool,
) -> (Vec<i64>, Vec<i32>, Option<usize>) {
    let len = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });

    let year_arg = args[0].try_downcast::<Int64Type>().unwrap();
    let month_arg = args[1].try_downcast::<Int64Type>().unwrap();
    let day_arg = args[2].try_downcast::<Int64Type>().unwrap();
    let hour_arg = args[3].try_downcast::<Int64Type>().unwrap();
    let minute_arg = args[4].try_downcast::<Int64Type>().unwrap();
    let second_arg = args[5].try_downcast::<Int64Type>().unwrap();

    let nanosecond_arg = if has_nano {
        Some(args[6].try_downcast::<Int64Type>().unwrap())
    } else {
        None
    };

    let tz_arg = if has_tz {
        let idx = if has_nano { 7 } else { 6 };
        Some(args[idx].try_downcast::<StringType>().unwrap())
    } else {
        None
    };

    let size = len.unwrap_or(1);

    let mut ts_values = Vec::with_capacity(size);
    let mut offset_values = Vec::with_capacity(size);

    for idx in 0..size {
        let year = unsafe { year_arg.index_unchecked(idx) };
        let month = unsafe { month_arg.index_unchecked(idx) };
        let day = unsafe { day_arg.index_unchecked(idx) };
        let hour = unsafe { hour_arg.index_unchecked(idx) };
        let minute = unsafe { minute_arg.index_unchecked(idx) };
        let second = unsafe { second_arg.index_unchecked(idx) };

        let nanosecond = nanosecond_arg
            .as_ref()
            .map(|a| unsafe { a.index_unchecked(idx) })
            .unwrap_or(0);

        let tz = if let Some(ref tz_arg) = tz_arg {
            let tz_str = unsafe { tz_arg.index_unchecked(idx) };
            match TimeZone::get(tz_str) {
                Ok(tz) => tz,
                Err(e) => {
                    ctx.set_error(ts_values.len(), format!("cannot parse timezone: {e}"));
                    ts_values.push(0);
                    offset_values.push(0);
                    continue;
                }
            }
        } else {
            ctx.func_ctx.tz.clone()
        };

        match timestamp_from_parts_to_micros(
            ctx,
            ts_values.len(),
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            &tz,
        ) {
            Some(utc_micros) => match Timestamp::from_microsecond(utc_micros) {
                Ok(ts) => {
                    let offset = tz.to_offset(ts);
                    ts_values.push(utc_micros);
                    offset_values.push(offset.seconds());
                }
                Err(e) => {
                    ctx.set_error(ts_values.len(), format!("{e}"));
                    ts_values.push(0);
                    offset_values.push(0);
                }
            },
            None => {
                ts_values.push(0);
                offset_values.push(0);
            }
        }
    }

    (ts_values, offset_values, len)
}

fn timestamp_from_parts_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    has_nano: bool,
) -> Value<AnyType> {
    let (ts_values, _, len) = build_timestamp_parts(args, ctx, has_nano, false);

    match len {
        Some(_) => Value::Column(Column::Timestamp(ts_values.into())),
        None => Value::Scalar(Scalar::Timestamp(ts_values[0])),
    }
}

fn timestamp_tz_from_parts_fn(
    args: &[Value<AnyType>],
    ctx: &mut EvalContext,
    has_nano: bool,
    has_tz: bool,
) -> Value<AnyType> {
    let (ts_values, offset_values, len) = build_timestamp_parts(args, ctx, has_nano, has_tz);

    match len {
        Some(_) => {
            let col = Column::TimestampTz(
                ts_values
                    .iter()
                    .zip(offset_values.iter())
                    .map(|(&ts, &off)| timestamp_tz::new(ts, off))
                    .collect(),
            );
            Value::Column(col)
        }
        None => Value::Scalar(Scalar::TimestampTz(timestamp_tz::new(
            ts_values[0],
            offset_values[0],
        ))),
    }
}
