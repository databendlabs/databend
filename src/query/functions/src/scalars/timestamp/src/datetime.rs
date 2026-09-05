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

use std::iter::once;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::TimeDelta;
use chrono::Timelike;
use chrono::Utc;
use chrono::format::Fixed;
use chrono::format::Item;
use chrono::format::Numeric;
use chrono::format::Parsed;
use chrono::format::StrftimeItems;
use chrono::format::parse_and_remainder;
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
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::date::check_input_year;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_date;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_timestamp;
use databend_common_expression::utils::auto_detect_datetime::int64_to_timestamp;
use databend_common_expression::utils::auto_detect_datetime::parse_epoch_str;
use databend_common_expression::utils::auto_detect_datetime::parse_timestamp_tz_with_auto;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_timezone::Tz;
use databend_common_timezone::components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;
use databend_common_timezone::wall_clock_is_monotonic;
use dtparse::parse;
use num_traits::AsPrimitive;

use crate::date_arithmetic::timestamp_tz_components_via_lut;
use crate::date_conversion::calc_date_to_timestamp;
use crate::date_format::pg_format_to_strftime;
use crate::interval::civil_date_to_days;

const MONTHS_PER_YEAR: i64 = 12;
const YEARS_PER_CENTURY: i64 = 100;
const TWO_DIGIT_YEAR_WINDOW_START: i64 = 1969;
const HOURS_PER_HALF_DAY: u32 = 12;
const MAX_FRACTIONAL_SECOND_DIGITS: usize = 9;

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

fn int64_domain_to_timestamp_domain<T: AsPrimitive<i64>>(
    domain: &SimpleDomain<T>,
) -> Option<SimpleDomain<i64>> {
    // Numeric AUTO conversion is monotonic only within one unit segment.
    let unit = |n: i64| {
        if -31536000000 < n && n < 31536000000 {
            0
        } else if -31536000000000 < n && n < 31536000000000 {
            1
        } else {
            2
        }
    };
    let min = domain.min.as_();
    let max = domain.max.as_();
    if unit(min) != unit(max) || (min < 0 && max > 0 && unit(min) != 0) {
        return None;
    }
    Some(SimpleDomain {
        min: int64_to_timestamp(min).ok()?,
        max: int64_to_timestamp(max).ok()?,
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
    // Only the UTC microsecond domain is preserved by this cast.
    // Values outside the SQL range must never reach datetime consumers.
    Some(SimpleDomain {
        min: check_timestamp(domain.min.timestamp()).ok()?,
        max: check_timestamp(domain.max.timestamp()).ok()?,
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
                let t_tz = match target_tz.parse::<Tz>() {
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

                let source_components = components_from_timestamp(src_timestamp, source_tz);
                let target_components = components_from_timestamp(src_timestamp, &t_tz);
                let src_dst_from_utc = source_components.offset_seconds;
                let target_dst_from_utc = target_components.offset_seconds;

                // `convert_timezone` returns the target wall clock reinterpreted in
                // the session zone, so shift the instant by the offset difference.
                let instant_micros = src_timestamp;
                let offset_as_micros_sec = (target_dst_from_utc - src_dst_from_utc) as i64;
                let result = offset_as_micros_sec
                    .checked_mul(MICROS_PER_SEC)
                    .and_then(|offset| instant_micros.checked_add(offset))
                    .filter(|result| (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(result));
                match result {
                    Some(result) => output.push(result),
                    None => {
                        ctx.set_error(
                            output.len(),
                            "Invalid date: converted timestamp is out of range".to_string(),
                        );
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
        Ok(ts) => return Ok(ts),
        Err(e) => e,
    };
    // Layer 2+3: Epoch detection + AUTO structured format detection
    if func_ctx.enable_auto_detect_datetime_format {
        if let Some(micros) = parse_epoch_str(val) {
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
            Ok(ts) => return Ok(ts),
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
        Ok(days) => return Ok(days),
        Err(e) => e,
    };
    // Layer 2+3: Numeric day + AUTO structured format detection
    if func_ctx.enable_auto_detect_datetime_format {
        if let Ok(days) = val.parse::<i64>() {
            return check_date(days).map_err(ErrorCode::BadArguments);
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
            Ok(days) => return Ok(days),
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
            // String domains alone do not establish a valid, ordered calendar
            // format (AUTO formats and explicit offsets may differ).
            if d.max.as_ref() != Some(&d.min) {
                return FunctionDomain::MayThrow;
            }
            match string_to_timestamp(&d.min, &ctx.tz) {
                Ok(value) => FunctionDomain::Domain(SimpleDomain {
                    min: value,
                    max: value,
                }),
                Err(_) => FunctionDomain::MayThrow,
            }
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
        |_, _| FunctionDomain::MayThrow,
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
                    match parse_date_with_format(date_string, &format) {
                        Ok(days) => {
                            output.push(days);
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
                    match parse_date_with_format(date, &format) {
                        Ok(days) => {
                            output.push(days);
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

fn parse_date_with_format(value: &str, format: &str) -> Result<i32, String> {
    let date = NaiveDate::parse_from_str(value, format).map_err(|err| err.to_string())?;
    check_input_year(date.year()).map_err(|err| err.message().to_string())?;
    check_date(i64::from(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE))
}

fn ensure_formatted_timestamp_range(micros: i64) -> Result<i64, Box<ErrorCode>> {
    check_timestamp(micros).map_err(|err| Box::new(ErrorCode::BadArguments(err)))
}

fn complete_two_digit_year(year_mod_100: i32) -> i64 {
    let century = TWO_DIGIT_YEAR_WINDOW_START.div_euclid(YEARS_PER_CENTURY);
    let year = century * YEARS_PER_CENTURY + i64::from(year_mod_100);
    if year < TWO_DIGIT_YEAR_WINDOW_START {
        year + YEARS_PER_CENTURY
    } else {
        year
    }
}

fn apply_meridiem(parsed: &mut Parsed, is_pm: bool) {
    let hour = match (parsed.hour_div_12(), parsed.hour_mod_12()) {
        (Some(div), Some(rem)) => div * HOURS_PER_HALF_DAY + rem,
        (None, Some(0)) => HOURS_PER_HALF_DAY,
        (None, Some(rem)) => rem,
        _ => 0,
    };
    let hour = hour % HOURS_PER_HALF_DAY + u32::from(is_pm) * HOURS_PER_HALF_DAY;
    parsed.hour_div_12 = Some(hour / HOURS_PER_HALF_DAY);
    parsed.hour_mod_12 = Some(hour % HOURS_PER_HALF_DAY);
}

fn parse_bare_fraction<'a>(parsed: &mut Parsed, input: &'a str) -> Result<&'a str, Box<ErrorCode>> {
    let digits = input
        .trim_start()
        .bytes()
        .take(MAX_FRACTIONAL_SECOND_DIGITS)
        .take_while(u8::is_ascii_digit)
        .count();
    let trimmed = input.trim_start();
    let mut fraction = String::with_capacity(digits + 1);
    fraction.push('.');
    fraction.push_str(&trimmed[..digits]);

    let mut field = Parsed::new();
    parse_and_remainder(&mut field, &fraction, once(Item::Fixed(Fixed::Nanosecond)))
        .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
    let nanosecond = field
        .nanosecond()
        .ok_or_else(|| Box::new(ErrorCode::BadArguments("Missing fractional second")))?;
    parsed
        .set_nanosecond(i64::from(nanosecond))
        .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;

    let consumed = input.len() - trimmed.len() + digits;
    Ok(&input[consumed..])
}

fn parse_formatted_fields<'a>(
    parsed: &mut Parsed,
    input: &'a str,
    format: &str,
) -> Result<&'a str, Box<ErrorCode>> {
    let mut has_bare_fraction = false;
    let mut has_24_hour = false;
    let mut has_meridiem = false;
    for item in StrftimeItems::new(format) {
        has_bare_fraction |= matches!(item, Item::Numeric(Numeric::Nanosecond, _));
        has_24_hour |= matches!(item, Item::Numeric(Numeric::Hour, _));
        has_meridiem |= matches!(item, Item::Fixed(Fixed::LowerAmPm | Fixed::UpperAmPm));
    }

    if !(has_bare_fraction || has_24_hour && has_meridiem) {
        return parse_and_remainder(parsed, input, StrftimeItems::new(format))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())));
    }

    let mut remainder = input;
    let mut is_pm = None;
    for item in StrftimeItems::new(format) {
        match &item {
            Item::Fixed(Fixed::LowerAmPm | Fixed::UpperAmPm) => {
                let mut field = Parsed::new();
                remainder = parse_and_remainder(&mut field, remainder, once(item))
                    .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
                is_pm = field.hour_div_12().map(|value| value == 1);
            }
            Item::Numeric(Numeric::Nanosecond, _) => {
                remainder = parse_bare_fraction(parsed, remainder)?;
            }
            _ => {
                remainder = parse_and_remainder(parsed, remainder, once(item))
                    .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
            }
        }
    }

    if let Some(is_pm) = is_pm {
        apply_meridiem(parsed, is_pm);
    }
    Ok(remainder)
}

fn complete_formatted_timestamp_fields(parsed: &mut Parsed) -> Result<(), Box<ErrorCode>> {
    match (parsed.year(), parsed.year_div_100(), parsed.year_mod_100()) {
        (None, Some(century), None) => parsed
            .set_year(i64::from(century) * YEARS_PER_CENTURY)
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?,
        (None, None, Some(year)) => parsed
            .set_year(complete_two_digit_year(year))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?,
        _ => {}
    }
    if parsed.isoyear().is_none()
        && parsed.isoyear_div_100().is_none()
        && let Some(year) = parsed.isoyear_mod_100()
    {
        parsed
            .set_isoyear(complete_two_digit_year(year))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
    }

    let epoch = DateTime::<Utc>::UNIX_EPOCH;
    match (parsed.hour_div_12(), parsed.hour_mod_12()) {
        (None, None) => parsed
            .set_hour(i64::from(epoch.hour()))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?,
        (None, Some(hour)) => parsed
            .set_ampm(hour == 0)
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?,
        (Some(_), None) => parsed
            .set_hour12(i64::from(HOURS_PER_HALF_DAY))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?,
        (Some(_), Some(_)) => {}
    }
    if parsed.minute().is_none() {
        parsed
            .set_minute(i64::from(epoch.minute()))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
    }
    if parsed.second().is_none() {
        parsed
            .set_second(i64::from(epoch.second()))
            .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
    }

    let has_alternate_date = parsed.ordinal().is_some()
        || parsed.isoweek().is_some()
        || parsed.week_from_sun().is_some()
        || parsed.week_from_mon().is_some();
    if !has_alternate_date {
        if parsed.day().is_none() {
            parsed
                .set_day(i64::from(epoch.day()))
                .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
        }
        if parsed.month().is_none() {
            parsed
                .set_month(i64::from(epoch.month()))
                .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
        }
        if parsed.year().is_none()
            && parsed.year_div_100().is_none()
            && parsed.year_mod_100().is_none()
        {
            parsed
                .set_year(i64::from(epoch.year()))
                .map_err(|err| Box::new(ErrorCode::BadArguments(err.to_string())))?;
        }
    }
    Ok(())
}

fn string_to_format_datetime(
    timestamp: &str,
    format: &str,
    ctx: &mut EvalContext,
    _parse_timestamp: bool,
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

    let mut parsed = Parsed::new();
    let remainder = parse_formatted_fields(&mut parsed, timestamp, &format)?;
    if !ctx.func_ctx.parse_datetime_ignore_remainder && !remainder.is_empty() {
        return Err(Box::new(ErrorCode::BadArguments(format!(
            "Can not fully parse timestamp {timestamp} by format {format}",
        ))));
    }
    if parsed.second().is_some_and(|second| second > 59) {
        return Err(Box::new(ErrorCode::BadArguments(
            "Seconds out of range, expected a value between 0 and 59",
        )));
    }

    let parsed_unix_timestamp = parsed.timestamp();
    let has_civil_fields = parsed.year().is_some()
        || parsed.year_div_100().is_some()
        || parsed.year_mod_100().is_some()
        || parsed.month().is_some()
        || parsed.day().is_some()
        || parsed.ordinal().is_some()
        || parsed.isoweek().is_some()
        || parsed.week_from_sun().is_some()
        || parsed.week_from_mon().is_some()
        || parsed.weekday().is_some();
    let has_time_fields = parsed.hour_div_12().is_some()
        || parsed.hour_mod_12().is_some()
        || parsed.minute().is_some()
        || parsed.second().is_some()
        || parsed.nanosecond().is_some();
    let has_timezone = parsed.offset().is_some();

    if let Some(seconds) = parsed_unix_timestamp {
        if has_civil_fields || has_time_fields || has_timezone {
            return Err(Box::new(ErrorCode::BadArguments(format!(
                "Can't parse '{timestamp}' as timestamp with format '{raw_format}'"
            ))));
        }
        let micros = seconds
            .checked_mul(MICROS_PER_SEC)
            .ok_or_else(|| Box::new(ErrorCode::BadArguments("Timestamp is out of range")))?;
        return Ok((ensure_formatted_timestamp_range(micros)?, false));
    }

    complete_formatted_timestamp_fields(&mut parsed)?;

    let local = parsed.to_naive_datetime_with_offset(0).map_err(|err| {
        Box::new(ErrorCode::BadArguments(format!(
            "{timestamp} to datetime error {err}"
        )))
    })?;
    check_input_year(local.year()).map_err(Box::new)?;
    let micro = local.and_utc().timestamp_subsec_micros();

    let micros = match parsed.offset() {
        Some(offset) => local
            .and_utc()
            .timestamp()
            .checked_sub(i64::from(offset))
            .and_then(|seconds| seconds.checked_mul(MICROS_PER_SEC))
            .and_then(|seconds| seconds.checked_add(i64::from(micro))),
        None => fast_utc_from_local(
            &ctx.func_ctx.tz,
            local.year(),
            local.month() as u8,
            local.day() as u8,
            local.hour() as u8,
            local.minute() as u8,
            local.second() as u8,
            micro,
        ),
    }
    .ok_or_else(|| Box::new(ErrorCode::BadArguments("Timestamp is out of range")))?;

    Ok((ensure_formatted_timestamp_range(micros)?, false))
}

fn register_date_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampType, _>(
        "to_timestamp",
        |ctx, domain| {
            date_domain_to_timestamp_domain(ctx, domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_date_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampType, _, _>(
        "try_to_timestamp",
        |ctx, domain| {
            if let Some(domain) = date_domain_to_timestamp_domain(ctx, domain) {
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

fn date_domain_to_timestamp_domain(
    ctx: &FunctionContext,
    domain: &SimpleDomain<i32>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: calc_date_to_timestamp(domain.min, &ctx.tz).ok()?,
        max: calc_date_to_timestamp(domain.max, &ctx.tz).ok()?,
    })
}

fn register_date_to_timestamp_tz(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampTzType, _>(
        "to_timestamp_tz",
        |_, _| FunctionDomain::MayThrow,
        eval_date_to_timestamp_tz,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampTzType, _, _>(
        "try_to_timestamp_tz",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_date_to_timestamp_tz),
    );

    fn eval_date_to_timestamp_tz(
        val: Value<DateType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampTzType> {
        vectorize_with_builder_1_arg::<DateType, TimestampTzType>(|val, output, ctx| {
            let timestamp = match calc_date_to_timestamp(val, &ctx.func_ctx.tz) {
                Ok(timestamp) => timestamp,
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(timestamp_tz::default());
                    return;
                }
            };
            let components = components_from_timestamp(timestamp, &ctx.func_ctx.tz);
            output.push(timestamp_tz::new(timestamp, components.offset_seconds));
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
            let components = components_from_timestamp(val, &ctx.func_ctx.tz);
            let offset = components.offset_seconds;
            let Some(offset_micros) = i64::from(offset).checked_mul(MICROS_PER_SEC) else {
                ctx.set_error(
                    output.len(),
                    "Invalid date: timezone offset is out of range",
                );
                output.push(timestamp_tz::default());
                return;
            };
            let Some(timestamp) = val.checked_sub(offset_micros) else {
                ctx.set_error(
                    output.len(),
                    "Invalid date: timestamp timezone value is out of range",
                );
                output.push(timestamp_tz::default());
                return;
            };
            let timestamp = match check_timestamp(timestamp) {
                Ok(timestamp) => timestamp,
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(timestamp_tz::default());
                    return;
                }
            };
            output.push(timestamp_tz::new(timestamp, offset));
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
        |_, _, _| FunctionDomain::MayThrow,
        eval_scaled_timestamp,
    );
    registry.register_combine_nullable_2_arg::<Int64Type, UInt64Type, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<Int64Type, UInt64Type, NullableType<TimestampType>>(
            |val, scale, output, _| {
                let value = val
                    .checked_mul(10i64.pow(6 - scale.min(6) as u32))
                    .and_then(|value| check_timestamp(value).ok());
                match value {
                    Some(value) => output.push(value),
                    None => output.push_null(),
                }
            },
        ),
    );

    fn eval_scaled_timestamp(
        val: Value<Int64Type>,
        scale: Value<UInt64Type>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_2_arg::<Int64Type, UInt64Type, TimestampType>(
            |val, scale, output, ctx| {
                let result = val
                    .checked_mul(10i64.pow(6 - scale.min(6) as u32))
                    .ok_or_else(|| "Invalid date: timestamp arithmetic overflow".to_string())
                    .and_then(check_timestamp);
                match result {
                    Ok(value) => output.push(value),
                    Err(err) => {
                        ctx.set_error(output.len(), err);
                        output.push(0);
                    }
                }
            },
        )(val, scale, ctx)
    }

    fn eval_number_to_timestamp(
        val: Value<Int64Type>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output, ctx| {
            match int64_to_timestamp(val) {
                Ok(ts) => output.push(ts),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_string_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, DateType, _>(
        "to_date",
        |ctx, d| {
            // Only singleton calendar input domains can be narrowed safely.
            if d.max.as_ref() != Some(&d.min) {
                return FunctionDomain::MayThrow;
            }
            match string_to_date(&d.min, &ctx.tz) {
                Ok(value) => FunctionDomain::Domain(SimpleDomain {
                    min: value,
                    max: value,
                }),
                Err(_) => FunctionDomain::MayThrow,
            }
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
            if !wall_clock_is_monotonic(&ctx.tz, domain.min, domain.max) {
                return FunctionDomain::MayThrow;
            }
            let (Ok(min), Ok(max)) = (
                calc_timestamp_to_date(domain.min, &ctx.tz),
                calc_timestamp_to_date(domain.max, &ctx.tz),
            ) else {
                return FunctionDomain::MayThrow;
            };
            FunctionDomain::Domain(SimpleDomain { min, max })
        },
        eval_timestamp_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampType, DateType, _, _>(
        "try_to_date",
        |ctx, domain| {
            if !wall_clock_is_monotonic(&ctx.tz, domain.min, domain.max) {
                return FunctionDomain::Full;
            }
            let (Ok(min), Ok(max)) = (
                calc_timestamp_to_date(domain.min, &ctx.tz),
                calc_timestamp_to_date(domain.max, &ctx.tz),
            ) else {
                return FunctionDomain::Full;
            };
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain { min, max })),
            })
        },
        error_to_null(eval_timestamp_to_date),
    );

    fn eval_timestamp_to_date(val: Value<TimestampType>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<TimestampType, DateType>(|val, output, ctx| {
            match timestamp_to_date_days(val, &ctx.func_ctx.tz) {
                Ok(days) => output.push(days),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
    fn calc_timestamp_to_date(val: i64, tz: &Tz) -> Result<i32, String> {
        timestamp_to_date_days(val, tz)
    }
}

fn timestamp_to_date_days(value: i64, tz: &Tz) -> Result<i32, String> {
    let components = components_from_timestamp(value, tz);
    days_from_components(components.year, components.month, components.day)
}

fn days_from_components(year: i32, month: u8, day: u8) -> Result<i32, String> {
    let days = civil_date_to_days(i64::from(year), month, day);
    let days = i64::try_from(days)
        .map_err(|_| "Invalid date: local date is out of civil range".to_string())?;
    check_date(days)
}

fn register_timestamp_tz_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampTzType, DateType, _>(
        "to_date",
        // TIMESTAMP_TZ ordering only bounds UTC instants, not their per-row
        // offsets. Endpoint dates cannot rule out a local-date overflow.
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_tz_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampTzType, DateType, _, _>(
        "try_to_date",
        |_, _| FunctionDomain::Full,
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
                    output.push(0);
                }
            }
        })(val, ctx)
    }

    fn calc_timestamp_tz_to_date(val: timestamp_tz) -> Result<i32, String> {
        // `timestamp_tz_components_via_lut` resolves the local calendar over
        // Databend's whole timestamp range, so a failure here means the stored
        // offset or instant is itself invalid rather than merely unusual.
        let components = timestamp_tz_components_via_lut(val)
            .ok_or_else(|| "Invalid date: timestamp timezone value is out of range".to_string())?;
        days_from_components(components.year, components.month, components.day)
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
        vectorize_with_builder_1_arg::<Int64Type, DateType>(|val, output, ctx| {
            match check_date(val) {
                Ok(days) => output.push(days),
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn normalize_date_parts(year: i64, month: i64, day: i64) -> Result<NaiveDate, String> {
    // Constructors keep their calendar-input contract. The extended range is
    // reserved for operations on already constructed DATE/TIMESTAMP values.
    let year =
        i32::try_from(year).map_err(|_| "Invalid date: input year out of range".to_string())?;
    check_input_year(year).map_err(|err| err.message().to_string())?;
    let year = i64::from(year);
    let month_offset = month
        .checked_sub(1)
        .ok_or_else(|| format!("Date parts out of bounds: year={year}, month={month}"))?;
    let total_months = year
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|value| value.checked_add(month_offset))
        .ok_or_else(|| format!("Date parts out of bounds: year={year}, month={month}"))?;
    let norm_year = i32::try_from(total_months.div_euclid(MONTHS_PER_YEAR))
        .map_err(|_| format!("Year out of bounds: {year}"))?;
    let norm_month = (total_months.rem_euclid(MONTHS_PER_YEAR) + 1) as u32;
    let base = NaiveDate::from_ymd_opt(norm_year, norm_month, 1)
        .ok_or_else(|| format!("Invalid date: year={year}, month={month}, day={day}"))?;
    let day_offset = day
        .checked_sub(1)
        .ok_or_else(|| format!("Day value out of bounds: {day}"))?;
    let duration = TimeDelta::try_days(day_offset)
        .ok_or_else(|| format!("Invalid date: day value out of bounds: {day}"))?;
    let date = base
        .checked_add_signed(duration)
        .ok_or_else(|| format!("Date out of range: year={year}, month={month}, day={day}"))?;
    check_input_year(date.year()).map_err(|err| err.message().to_string())?;
    Ok(date)
}

fn duration_from_time_parts(
    ctx: &mut EvalContext,
    row: usize,
    hour: i64,
    minute: i64,
    second: i64,
    nanosecond: i64,
) -> Option<TimeDelta> {
    let hour_duration = TimeDelta::try_hours(hour).or_else(|| {
        ctx.set_error(
            row,
            ErrorCode::BadArguments("Timestamp hour component is out of range"),
        );
        None
    })?;
    let minute_duration = TimeDelta::try_minutes(minute).or_else(|| {
        ctx.set_error(
            row,
            ErrorCode::BadArguments("Timestamp minute component is out of range"),
        );
        None
    })?;
    let second_duration = TimeDelta::try_seconds(second).or_else(|| {
        ctx.set_error(
            row,
            "Invalid date: timestamp second component is out of range",
        );
        None
    })?;
    let nano_duration = TimeDelta::nanoseconds(nanosecond);

    hour_duration
        .checked_add(&minute_duration)
        .and_then(|duration| duration.checked_add(&second_duration))
        .and_then(|duration| duration.checked_add(&nano_duration))
        .or_else(|| {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Timestamp components overflow"),
            );
            None
        })
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
    tz: &Tz,
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
        .and_hms_opt(0, 0, 0)
        .and_then(|datetime| datetime.checked_add_signed(duration));
    let local_dt = match local_dt {
        Some(local_dt) => local_dt,
        None => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Cannot construct timestamp: components are out of range"),
            );
            return None;
        }
    };

    if let Err(err) = check_input_year(local_dt.year()) {
        ctx.set_error(row, err);
        return None;
    }
    let micros = fast_utc_from_local(
        tz,
        local_dt.year(),
        local_dt.month() as u8,
        local_dt.day() as u8,
        local_dt.hour() as u8,
        local_dt.minute() as u8,
        local_dt.second() as u8,
        (local_dt.nanosecond() / 1_000) as u32,
    );
    match micros {
        Some(micros) => validate_timestamp_bounds(ctx, row, micros),
        None => {
            ctx.set_error(
                row,
                ErrorCode::BadArguments("Cannot construct timestamp: local time is out of range"),
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
                            .signed_duration_since(
                                NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"),
                            )
                            .num_days();
                        if (i64::from(DATE_MIN)..=i64::from(DATE_MAX)).contains(&days) {
                            output.push(days as i32);
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
            match tz_str.parse::<Tz>() {
                Ok(tz) => tz,
                Err(e) => {
                    ctx.set_error(ts_values.len(), format!("cannot parse timezone: {e}"));
                    ts_values.push(0);
                    offset_values.push(0);
                    continue;
                }
            }
        } else {
            ctx.func_ctx.tz
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
            Some(utc_micros) => {
                let components = components_from_timestamp(utc_micros, &tz);
                ts_values.push(utc_micros);
                offset_values.push(components.offset_seconds);
            }
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
