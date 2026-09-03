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

use chrono::Datelike;
use chrono::NaiveDate as Date;
use chrono::TimeDelta;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::DateType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::Tz;
use databend_common_timezone::components_from_timestamp;
use num_traits::AsPrimitive;

use crate::interval::apply_months_to_timestamp;
use crate::interval::civil_date_to_days;

const MICROSECS_PER_DAY: i64 = 86_400_000_000;

// Timestamp arithmetic factors.
const FACTOR_HOUR: i64 = 3600;
const FACTOR_MINUTE: i64 = 60;
const FACTOR_SECOND: i64 = 1;
const LAST_DAY_LUT: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn eval_years_base(
    year: i32,
    month: u32,
    day: u32,
    delta: i64,
    _add_months: bool,
) -> Result<Date, String> {
    let new_year = i64::from(year)
        .checked_add(delta)
        .and_then(|year| i32::try_from(year).ok())
        .ok_or_else(|| "Invalid date: year arithmetic is out of range".to_string())?;
    if !(-9999..=9999).contains(&new_year) {
        return Err("Invalid date: year arithmetic is out of range".to_string());
    }
    let new_day = if month == 2 && day == 29 {
        last_day_of_year_month(new_year, month)
    } else {
        day
    };
    Date::from_ymd_opt(new_year, month, new_day)
        .ok_or_else(|| "Invalid date: year arithmetic is out of range".to_string())
}

fn eval_months_base(
    year: i32,
    month: u32,
    day: u32,
    delta: i64,
    add_months: bool,
) -> Result<Date, String> {
    let total_months = i64::from(year)
        .checked_mul(12)
        .and_then(|value| value.checked_add(i64::from(month) - 1))
        .and_then(|value| value.checked_add(delta))
        .ok_or_else(|| "Invalid date: month arithmetic is out of range".to_string())?;
    let new_year = i32::try_from(total_months.div_euclid(12))
        .map_err(|_| "Invalid date: month arithmetic is out of range".to_string())?;
    if !(-9999..=9999).contains(&new_year) {
        return Err("Invalid date: month arithmetic is out of range".to_string());
    }
    let new_month = total_months.rem_euclid(12) as u32 + 1;
    let max_day = last_day_of_year_month(new_year, new_month);
    let new_day = if add_months && day == last_day_of_year_month(year, month) {
        max_day
    } else {
        day.min(max_day)
    };

    Date::from_ymd_opt(new_year, new_month, new_day)
        .ok_or_else(|| "Invalid date: month arithmetic is out of range".to_string())
}

pub(super) fn last_day_of_year_month(year: i32, month: u32) -> u32 {
    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if month == 2 && is_leap_year {
        return 29;
    }
    LAST_DAY_LUT[month as usize]
}

macro_rules! impl_interval_year_month {
    ($vis:vis $name:ident, $op:expr, $timestamp_month_multiplier:expr) => {
        #[derive(Clone)]
        $vis struct $name;

        impl $name {
            $vis fn eval_date(
                date: i32,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i32, String> {
                let date = date_from_days(date);
                let new_date = $op(
                    date.year(),
                    date.month(),
                    date.day(),
                    delta.as_(),
                    add_months,
                )?;

                ensure_date_range(
                    new_date
                        .signed_duration_since(
                            Date::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"),
                        )
                        .num_days(),
                )
            }

            $vis fn eval_timestamp(
                us: i64,
                tz: &Tz,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i64, String> {
                let months = delta
                    .as_()
                    .checked_mul($timestamp_month_multiplier)
                    .ok_or_else(|| "Invalid date: month arithmetic is out of range".to_string())?;
                apply_months_to_timestamp(us, months, tz, add_months)
            }
        }
    };
}

impl_interval_year_month!(EvalYearsImpl, eval_years_base, 12);
impl_interval_year_month!(pub EvalMonthsImpl, eval_months_base, 1);

/// Compare two `DateTimeComponents` by their time-of-day portion only.
fn components_time_less_than(a: &DateTimeComponents, b: &DateTimeComponents) -> bool {
    (a.hour, a.minute, a.second, a.micro) < (b.hour, b.minute, b.second, b.micro)
}

fn timestamp_components(timestamp: i64, timezone: &Tz) -> DateTimeComponents {
    components_from_timestamp(timestamp, timezone)
}

pub(crate) fn ensure_date_range(value: i64) -> std::result::Result<i32, String> {
    Ok(clamp_date(value))
}

#[inline]
pub(super) fn timestamp_tz_components_via_lut(value: timestamp_tz) -> Option<DateTimeComponents> {
    let offset = value.micros_offset()?;
    let local = value.timestamp().checked_add(offset)?;
    Some(components_from_timestamp(local, &Tz::UTC))
}

impl EvalYearsImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        (date_end.year() - date_start.year()) as i32
    }

    fn eval_date_between(date_start: i32, date_end: i32) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start);
        }

        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);

        let mut years = date_end.year() - date_start.year();

        // If the end month is less than the start month,
        // or the months are equal but the end day is less than the start day,
        // the last year is incomplete, minus 1
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        i64::from(end.year) - i64::from(start.year)
    }

    fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        let mut years = end.year - start.year;
        let start_is_feb_29 = start.month == 2 && start.day == 29;
        let end_is_feb_28 = end.month == 2 && end.day == 28;
        let end_before_start_date = (end.month < start.month)
            || (end.month == start.month && end.day < start.day)
            || (end.month == start.month
                && end.day == start.day
                && components_time_less_than(&end, &start));
        if !(start_is_feb_29 && end_is_feb_28) && end_before_start_date {
            years -= 1;
        }
        i64::from(years)
    }
}

struct EvalISOYearsImpl;
impl EvalISOYearsImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        date_end.iso_week().year() as i32 - date_start.iso_week().year() as i32
    }

    fn eval_date_between(date_start: i32, date_end: i32) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start);
        }
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        let mut years = date_end.iso_week().year() - date_start.iso_week().year();
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        let (start_year, _) = start.iso_year_week();
        let (end_year, _) = end.iso_year_week();
        i64::from(end_year - start_year)
    }

    fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        let (start_iso_year, _) = start.iso_year_week();
        let (end_iso_year, _) = end.iso_year_week();
        let mut years = i64::from(end_iso_year - start_iso_year);
        let start_is_feb_29 = start.month == 2 && start.day == 29;
        let end_is_feb_28 = end.month == 2 && end.day == 28;
        let end_before_start_date = (end.month < start.month)
            || (end.month == start.month && end.day < start.day)
            || (end.month == start.month
                && end.day == start.day
                && components_time_less_than(&end, &start));
        if !(start_is_feb_29 && end_is_feb_28) && end_before_start_date {
            years -= 1;
        }
        years
    }
}

struct EvalYearWeeksImpl;
impl EvalYearWeeksImpl {
    fn yearweek(date: Date) -> i32 {
        let iso_week = date.iso_week();
        (iso_week.year() * 100) + iso_week.week() as i32
    }

    fn yearweek_from_components(components: &DateTimeComponents) -> i32 {
        let (year, week) = components.iso_year_week();
        year * 100 + week as i32
    }

    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        let end = Self::yearweek(date_end);
        let start = Self::yearweek(date_start);

        end - start
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        i64::from(Self::yearweek_from_components(&end))
            - i64::from(Self::yearweek_from_components(&start))
    }
}

struct EvalQuartersImpl;

impl EvalQuartersImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        let start_quarter = (date_start.month() as i32 - 1) / 3 + 1;
        let end_quarter = (date_end.month() as i32 - 1) / 3 + 1;
        (date_end.year() - date_start.year()) as i32 * 4 + end_quarter - start_quarter
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &Tz) -> i64 {
        let start = timestamp_components(date_start, tz);
        let end = timestamp_components(date_end, tz);
        let start_quarter = (i64::from(start.month) - 1) / 3 + 1;
        let end_quarter = (i64::from(end.month) - 1) / 3 + 1;
        i64::from(end.year - start.year) * 4 + end_quarter - start_quarter
    }
}

impl EvalMonthsImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        (date_end.year() - date_start.year()) as i32 * 12 + date_end.month() as i32
            - date_start.month() as i32
    }

    fn eval_date_between(start: i32, end: i32) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start);
        }

        let start = date_from_days(start);
        let end = date_from_days(end);

        let year_diff = end.year() - start.year();
        let month_diff = end.month() as i32 - start.month() as i32;
        let mut months = year_diff as i32 * 12 + month_diff;

        if end.day() < start.day() {
            months -= 1;
        }

        months
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalMonthsImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    fn eval_timestamp_between(start: i64, end: i64, tz: &Tz) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }
        let start = timestamp_components(start, tz);
        let end = timestamp_components(end, tz);
        let year_diff = end.year - start.year;
        let month_diff = i32::from(end.month) - i32::from(start.month);
        let mut months = i64::from(year_diff) * 12 + i64::from(month_diff);
        if (end.day < start.day)
            || (end.day == start.day && components_time_less_than(&end, &start))
        {
            months -= 1;
        }
        months
    }

    // current we don't consider tz here
    fn months_between_ts(ts_a: i64, ts_b: i64) -> f64 {
        EvalMonthsImpl::months_between(
            (ts_a / 86_400_000_000) as i32,
            (ts_b / 86_400_000_000) as i32,
        )
    }

    fn months_between(date_a: i32, date_b: i32) -> f64 {
        let epoch = Date::from_ymd_opt(1970, 1, 1).expect("epoch date is valid");
        let date_a = epoch
            .checked_add_signed(TimeDelta::days(date_a as i64))
            .expect("valid Databend date");
        let date_b = epoch
            .checked_add_signed(TimeDelta::days(date_b as i64))
            .expect("valid Databend date");

        let year_diff = (date_a.year() - date_b.year()) as i64;
        let month_diff = date_a.month() as i64 - date_b.month() as i64;

        // Calculate total months difference
        let total_months_diff = year_diff * 12 + month_diff;

        // Determine if special case for fractional part applies
        let is_same_day_of_month = date_a.day() == date_b.day();

        let are_both_end_of_month = date_a.day()
            == last_day_of_year_month(date_a.year(), date_a.month())
            && date_b.day() == last_day_of_year_month(date_b.year(), date_b.month());
        let day_fraction = if is_same_day_of_month || are_both_end_of_month {
            0.0
        } else {
            let day_diff = date_a.day() as i32 - date_b.day() as i32;
            day_diff as f64 / 31.0 // Using 31-day month for fractional part
        };

        // Total difference including fractional part
        total_months_diff as f64 + day_fraction
    }
}

struct EvalWeeksImpl;

impl EvalWeeksImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        // 1970-01-01 is ThursDay
        let date_start = date_start / 7 + (date_start % 7 >= 4) as i32;
        let date_end = date_end / 7 + (date_end % 7 >= 4) as i32;
        date_end - date_start
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalWeeksImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    fn calculate_weeks_between_years(
        start_year: i32,
        end_year: i32,
        start_week: u32,
        end_week: u32,
    ) -> i32 {
        let mut weeks = 0;
        let mut current_year = start_year + 1;

        fn iso_weeks(year: i32) -> i32 {
            let jan1_days = civil_date_to_days(i64::from(year), 1, 1);
            let monday_zero = (jan1_days + 3).rem_euclid(7);
            let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            if monday_zero == 3 || (monday_zero == 2 && is_leap) {
                53
            } else {
                52
            }
        }
        while current_year < end_year {
            weeks += iso_weeks(current_year);
            current_year += 1;
        }

        // add start_year weeks and end_year weeks
        weeks += iso_weeks(start_year) - start_week as i32 + end_week as i32;
        weeks
    }

    fn eval_date_between(start: i32, end: i32) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start);
        }

        let earlier = date_from_days(start);
        let later = date_from_days(end);
        let mut weeks = Self::calculate_weeks_between_years(
            earlier.year() as i32,
            later.year() as i32,
            earlier.iso_week().week() as u32,
            later.iso_week().week() as u32,
        );
        // Judge whether it is complete after the last week
        let end_weekday = later.weekday();
        let days_since_monday = end_weekday.number_from_monday() - 1;
        let monday_of_end_week = later
            .checked_sub_signed(TimeDelta::days(days_since_monday as i64))
            .expect("subtracting at most six days stays in range");

        if later < monday_of_end_week {
            weeks -= 1;
        }

        weeks
    }

    fn eval_timestamp_between(start: i64, end: i64, tz: &Tz) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }
        let start = timestamp_components(start, tz);
        let end = timestamp_components(end, tz);
        let (_, start_week) = start.iso_year_week();
        let (_, end_week) = end.iso_year_week();
        i64::from(Self::calculate_weeks_between_years(
            start.year, end.year, start_week, end_week,
        ))
    }
}

pub(super) struct EvalDaysImpl;

impl EvalDaysImpl {
    pub(super) fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> i32 {
        clamp_date(i64::from(date).wrapping_add(delta.as_()))
    }

    pub(super) fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        date_end - date_start
    }

    pub(super) fn eval_timestamp(date: i64, delta: impl AsPrimitive<i64>) -> i64 {
        let mut value = date.wrapping_add(delta.as_().wrapping_mul(MICROSECS_PER_DAY));
        clamp_timestamp(&mut value);
        value
    }

    pub(super) fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalDaysImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    pub(super) fn eval_timestamp_between(start: i64, end: i64, tz: &Tz) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let start = timestamp_components(start, tz);
        let end = timestamp_components(end, tz);
        let start_days = civil_date_to_days(i64::from(start.year), start.month, start.day);
        let end_days = civil_date_to_days(i64::from(end.year), end.month, end.day);
        let mut full_days = (end_days - start_days) as i64;
        if components_time_less_than(&end, &start) {
            full_days -= 1;
        }
        full_days
    }
}

struct EvalTimesImpl;

impl EvalTimesImpl {
    fn eval_timestamp(us: i64, delta: impl AsPrimitive<i64>, factor: i64) -> i64 {
        let mut timestamp = us.wrapping_add(
            delta
                .as_()
                .wrapping_mul(factor.wrapping_mul(MICROS_PER_SEC)),
        );
        clamp_timestamp(&mut timestamp);
        timestamp
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, factor: i64) -> i64 {
        let date_start = date_start / (MICROS_PER_SEC * factor);
        let date_end = date_end / (MICROS_PER_SEC * factor);
        date_end - date_start
    }

    fn eval_timestamp_between(unit: &str, start: i64, end: i64) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(unit, end, start);
        }

        let micros = end - start;
        match unit {
            "hours" => micros / (3600 * MICROS_PER_SEC),
            "minutes" => micros / (60 * MICROS_PER_SEC),
            "seconds" => micros / MICROS_PER_SEC,
            _ => unreachable!("Unsupported unit: {}", unit),
        }
    }
}

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_add_functions(registry);
    register_sub_functions(registry);
    register_diff_functions(registry);
    register_between_functions(registry);
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
                delta.wrapping_mul(delta_sign),
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
                delta.wrapping_mul(delta_sign),
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
                delta.wrapping_mul(month_multiplier),
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
                delta.wrapping_mul(month_multiplier),
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
        move |date, delta, _| EvalDaysImpl::eval_date(date, delta.wrapping_mul(day_multiplier)),
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |timestamp, delta, _| {
            EvalDaysImpl::eval_timestamp(timestamp, delta.wrapping_mul(day_multiplier))
        },
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
            let timestamp = i64::from(date) * 24 * 3600 * MICROS_PER_SEC;
            EvalTimesImpl::eval_timestamp(timestamp, delta.wrapping_mul(delta_sign), factor)
        },
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        move |timestamp, delta, _| {
            EvalTimesImpl::eval_timestamp(timestamp, delta.wrapping_mul(delta_sign), factor)
        },
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
            |date_end, date_start, builder, _| {
                let diff_years = EvalYearsImpl::eval_date_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let diff_years = EvalQuartersImpl::eval_date_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let diff_months = EvalMonthsImpl::eval_date_diff(date_start, date_end);
                builder.push(diff_months as i64);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "diff_months",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, TimestampType, Int64Type>(
            |date_end, date_start, builder, _| {
                let diff_months = EvalMonthsImpl::eval_timestamp_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let diff = EvalYearWeeksImpl::eval_date_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let diff = EvalISOYearsImpl::eval_date_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let diff_years = EvalYearsImpl::eval_date_diff(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let between_years = EvalYearsImpl::eval_date_between(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let between_quarters = EvalMonthsImpl::eval_date_between(date_start, date_end) / 3;
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
            |date_end, date_start, builder, _| {
                let between_months = EvalMonthsImpl::eval_date_between(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let between_weeks = EvalWeeksImpl::eval_date_between(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let between_isoyears = EvalISOYearsImpl::eval_date_between(date_start, date_end);
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
            |date_end, date_start, builder, _| {
                let between_millenniums = EvalYearsImpl::eval_date_between(date_start, date_end);
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

fn clamp_date_domain(raw_min: i64, raw_max: i64) -> SimpleDomain<i32> {
    if raw_min >= i64::from(DATE_MIN) && raw_max <= i64::from(DATE_MAX) {
        SimpleDomain {
            min: raw_min as i32,
            max: raw_max as i32,
        }
    } else if raw_min > i64::from(DATE_MAX) || raw_max < i64::from(DATE_MIN) {
        SimpleDomain {
            min: DATE_MIN,
            max: DATE_MIN,
        }
    } else {
        SimpleDomain {
            min: DATE_MIN,
            max: raw_max.min(i64::from(DATE_MAX)) as i32,
        }
    }
}

fn clamp_timestamp_domain(raw_min: i64, raw_max: i64) -> SimpleDomain<i64> {
    if raw_min >= TIMESTAMP_MIN && raw_max <= TIMESTAMP_MAX {
        SimpleDomain {
            min: raw_min,
            max: raw_max,
        }
    } else if raw_min > TIMESTAMP_MAX || raw_max < TIMESTAMP_MIN {
        SimpleDomain {
            min: TIMESTAMP_MIN,
            max: TIMESTAMP_MIN,
        }
    } else {
        SimpleDomain {
            min: TIMESTAMP_MIN,
            max: raw_max.min(TIMESTAMP_MAX),
        }
    }
}

pub(super) fn register_timestamp_add_sub(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "plus",
        |_, lhs, rhs| {
            let raw_min = i64::from(lhs.min).saturating_add(rhs.min);
            let raw_max = i64::from(lhs.max).saturating_add(rhs.max);
            FunctionDomain::Domain(clamp_date_domain(raw_min, raw_max))
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date(i64::from(a).saturating_add(b)))
        }),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "plus",
        |_, lhs, rhs| {
            let raw_min = lhs.min.saturating_add(rhs.min);
            let raw_max = lhs.max.saturating_add(rhs.max);
            FunctionDomain::Domain(clamp_timestamp_domain(raw_min, raw_max))
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, _| {
                let mut sum = a.saturating_add(b);
                clamp_timestamp(&mut sum);
                output.push(sum);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "minus",
        |_, lhs, rhs| {
            let raw_min = i64::from(lhs.min).saturating_sub(rhs.max);
            let raw_max = i64::from(lhs.max).saturating_sub(rhs.min);
            FunctionDomain::Domain(clamp_date_domain(raw_min, raw_max))
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date(i64::from(a).saturating_sub(b)));
        }),
    );

    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "minus",
        |_, lhs, rhs| {
            let raw_min = lhs.min.saturating_sub(rhs.max);
            let raw_max = lhs.max.saturating_sub(rhs.min);
            FunctionDomain::Domain(clamp_timestamp_domain(raw_min, raw_max))
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, _| {
                let mut difference = a.saturating_sub(b);
                clamp_timestamp(&mut difference);
                output.push(difference);
            },
        ),
    );
}
