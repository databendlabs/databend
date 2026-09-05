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
use chrono::Weekday;
use databend_common_expression::Domain;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::DateType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_timezone::Tz;
use databend_common_timezone::components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;

use crate::date_arithmetic::last_day_of_year_month;
use crate::date_extract::calendar_monotonicity;

#[derive(Clone, Copy)]
enum Round {
    Second,
    Minute,
    FiveMinutes,
    TenMinutes,
    FifteenMinutes,
    TimeSlot,
    Hour,
    Day,
}

fn round_timestamp(ts: i64, tz: &Tz, round: Round) -> Result<i64, String> {
    let components = components_from_timestamp(ts, tz);
    let (hour, minute, second) = match round {
        Round::Second => (components.hour, components.minute, components.second),
        Round::Minute => (components.hour, components.minute, 0),
        Round::FiveMinutes => (components.hour, components.minute / 5 * 5, 0),
        Round::TenMinutes => (components.hour, components.minute / 10 * 10, 0),
        Round::FifteenMinutes => (components.hour, components.minute / 15 * 15, 0),
        Round::TimeSlot => (components.hour, components.minute / 30 * 30, 0),
        Round::Hour => (components.hour, 0, 0),
        Round::Day => (0, 0, 0),
    };
    let timestamp = fast_utc_from_local(
        tz,
        components.year,
        components.month,
        components.day,
        hour,
        minute,
        second,
        0,
    )
    .ok_or_else(|| "Invalid date: rounded timestamp is out of range".to_string())?;
    check_timestamp(timestamp)
}

trait RoundDate {
    fn round(date: &Date) -> i32;
}

struct DateRounder;

impl DateRounder {
    fn eval_timestamp<T>(us: i64, tz: &Tz) -> Result<i32, String>
    where T: RoundDate {
        let components = components_from_timestamp(us, tz);
        let date = Date::from_ymd_opt(
            components.year,
            components.month as u32,
            components.day as u32,
        )
        .ok_or_else(|| "Invalid date: timestamp is out of range".to_string())?;
        check_date(i64::from(T::round(&date)))
    }

    fn eval_date<T>(date: i32) -> Result<i32, String>
    where T: RoundDate {
        check_date(i64::from(T::round(&date_from_days(date))))
    }
}

#[inline]
fn date_to_inner_number(date: &Date) -> i32 {
    date.signed_duration_since(Date::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32
}

struct ToLastMonday;
struct ToLastSunday;
struct ToStartOfMonth;
struct ToStartOfQuarter;
struct ToStartOfYear;
struct ToStartOfISOYear;

struct ToLastOfYear;
struct ToLastOfWeek;
struct ToLastOfMonth;
struct ToLastOfQuarter;
struct ToPreviousMonday;
struct ToPreviousTuesday;
struct ToPreviousWednesday;
struct ToPreviousThursday;
struct ToPreviousFriday;
struct ToPreviousSaturday;
struct ToPreviousSunday;
struct ToNextMonday;
struct ToNextTuesday;
struct ToNextWednesday;
struct ToNextThursday;
struct ToNextFriday;
struct ToNextSaturday;
struct ToNextSunday;

impl RoundDate for ToLastMonday {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(date) - date.weekday().num_days_from_monday() as i32
    }
}

impl RoundDate for ToLastSunday {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(date) - date.weekday().num_days_from_sunday() as i32
    }
}

impl RoundDate for ToStartOfMonth {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(&Date::from_ymd_opt(date.year(), date.month(), 1).unwrap())
    }
}

impl RoundDate for ToStartOfQuarter {
    fn round(input: &Date) -> i32 {
        let new_month = (input.month() - 1) / 3 * 3 + 1;
        date_to_inner_number(&Date::from_ymd_opt(input.year(), new_month, 1).unwrap())
    }
}

impl RoundDate for ToStartOfYear {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(&Date::from_ymd_opt(date.year(), 1, 1).unwrap())
    }
}

impl RoundDate for ToStartOfISOYear {
    fn round(input: &Date) -> i32 {
        let iso_year = input.iso_week().year();
        for day in 1..=7 {
            let new_date = Date::from_ymd_opt(iso_year, 1, day).unwrap();
            if new_date.weekday() == Weekday::Mon {
                return date_to_inner_number(&new_date);
            }
        }
        0
    }
}

impl RoundDate for ToLastOfWeek {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(date) - date.weekday().num_days_from_monday() as i32 + 6
    }
}

impl RoundDate for ToLastOfMonth {
    fn round(input: &Date) -> i32 {
        let day = last_day_of_year_month(input.year(), input.month());
        date_to_inner_number(&Date::from_ymd_opt(input.year(), input.month(), day).unwrap())
    }
}

impl RoundDate for ToLastOfQuarter {
    fn round(input: &Date) -> i32 {
        let new_month = (input.month() - 1) / 3 * 3 + 3;
        let day = last_day_of_year_month(input.year(), new_month);
        date_to_inner_number(&Date::from_ymd_opt(input.year(), new_month, day).unwrap())
    }
}

impl RoundDate for ToLastOfYear {
    fn round(input: &Date) -> i32 {
        let day = last_day_of_year_month(input.year(), 12);
        date_to_inner_number(&Date::from_ymd_opt(input.year(), 12, day).unwrap())
    }
}

macro_rules! impl_round_to_weekday {
    ($type:ident, $weekday:ident, $is_previous:literal) => {
        impl RoundDate for $type {
            fn round(date: &Date) -> i32 {
                previous_or_next_date_day(date, Weekday::$weekday, $is_previous)
            }
        }
    };
}

impl_round_to_weekday!(ToPreviousMonday, Mon, true);
impl_round_to_weekday!(ToPreviousTuesday, Tue, true);
impl_round_to_weekday!(ToPreviousWednesday, Wed, true);
impl_round_to_weekday!(ToPreviousThursday, Thu, true);
impl_round_to_weekday!(ToPreviousFriday, Fri, true);
impl_round_to_weekday!(ToPreviousSaturday, Sat, true);
impl_round_to_weekday!(ToPreviousSunday, Sun, true);
impl_round_to_weekday!(ToNextMonday, Mon, false);
impl_round_to_weekday!(ToNextTuesday, Tue, false);
impl_round_to_weekday!(ToNextWednesday, Wed, false);
impl_round_to_weekday!(ToNextThursday, Thu, false);
impl_round_to_weekday!(ToNextFriday, Fri, false);
impl_round_to_weekday!(ToNextSaturday, Sat, false);
impl_round_to_weekday!(ToNextSunday, Sun, false);

fn previous_or_next_date_day(date: &Date, target: Weekday, is_previous: bool) -> i32 {
    let dir = if is_previous { -1 } else { 1 };
    let mut days_diff = (dir
        * (target.num_days_from_monday() as i32 - date.weekday().num_days_from_monday() as i32)
        + 7)
        % 7;

    days_diff = if days_diff == 0 { 7 } else { days_diff };

    date_to_inner_number(date) + dir * days_diff
}

fn rounding_monotonicity(ctx: &FunctionContext, args: &[Domain]) -> Option<usize> {
    // Endpoint folding must not suppress an overflow from a rounder. Keep a
    // full calendar-year margin (including ISO weeks and timezone offsets).
    let [domain] = args else {
        return None;
    };
    let domain = match domain {
        Domain::Nullable(domain) => match domain.value.as_deref() {
            Some(domain) => domain,
            None => return Some(0),
        },
        domain => domain,
    };
    let safe = match domain {
        Domain::Date(d) => d.min >= DATE_MIN + 370 && d.max <= DATE_MAX - 370,
        Domain::Timestamp(d) => {
            let margin = 370 * 86_400_000_000;
            d.min >= TIMESTAMP_MIN + margin && d.max <= TIMESTAMP_MAX - margin
        }
        _ => false,
    };
    if safe {
        calendar_monotonicity(ctx, args)
    } else {
        None
    }
}

pub(super) fn register(registry: &mut FunctionRegistry) {
    // Calendar rounders are monotonic only while the wall clock is monotonic; sub-day
    // rounders stay unregistered because every fallback can break them.
    for name in [
        "to_start_of_day",
        "to_monday",
        "to_start_of_week",
        "to_start_of_month",
        "to_start_of_quarter",
        "to_start_of_year",
        "to_start_of_iso_year",
    ] {
        registry.properties.insert(
            name.to_string(),
            FunctionProperty::default().monotonicity_check(rounding_monotonicity),
        );
    }

    // timestamp -> timestamp
    register_timestamp_round(registry, "to_start_of_second", Round::Second);
    register_timestamp_round(registry, "to_start_of_minute", Round::Minute);
    register_timestamp_round(registry, "to_start_of_five_minutes", Round::FiveMinutes);
    register_timestamp_round(registry, "to_start_of_ten_minutes", Round::TenMinutes);
    register_timestamp_round(
        registry,
        "to_start_of_fifteen_minutes",
        Round::FifteenMinutes,
    );
    register_timestamp_round(registry, "to_start_of_hour", Round::Hour);
    register_timestamp_round(registry, "to_start_of_day", Round::Day);
    register_timestamp_round(registry, "time_slot", Round::TimeSlot);
    crate::date_time_slice::register(registry);

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
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|val, mode, output, ctx| {
            let result = if mode == 0 {
                DateRounder::eval_date::<ToLastSunday>(val)
            } else {
                DateRounder::eval_date::<ToLastMonday>(val)
            };
            match result {
                Ok(value) => output.push(value),
                Err(error) => {
                    ctx.set_error(output.len(), error);
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, DateType>(
            |val, mode, output, ctx| {
                let result = if mode == 0 {
                    DateRounder::eval_timestamp::<ToLastSunday>(val, &ctx.func_ctx.tz)
                } else {
                    DateRounder::eval_timestamp::<ToLastMonday>(val, &ctx.func_ctx.tz)
                };
                match result {
                    Ok(value) => output.push(value),
                    Err(error) => {
                        ctx.set_error(output.len(), error);
                        output.push(0);
                    }
                }
            },
        ),
    );
}

fn register_timestamp_round(registry: &mut FunctionRegistry, name: &'static str, round: Round) {
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<TimestampType, TimestampType>(move |value, output, ctx| {
            match round_timestamp(value, &ctx.func_ctx.tz, round) {
                Ok(value) => output.push(value),
                Err(error) => {
                    ctx.set_error(output.len(), error);
                    output.push(0);
                }
            }
        }),
    );
}

fn rounder_functions_helper<T>(registry: &mut FunctionRegistry, name: &str)
where T: RoundDate {
    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<DateType, DateType>(|value, output, ctx| {
            match DateRounder::eval_date::<T>(value) {
                Ok(value) => output.push(value),
                Err(error) => {
                    ctx.set_error(output.len(), error);
                    output.push(0);
                }
            }
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<TimestampType, DateType>(|value, output, ctx| {
            match DateRounder::eval_timestamp::<T>(value, &ctx.func_ctx.tz) {
                Ok(value) => output.push(value),
                Err(error) => {
                    ctx.set_error(output.len(), error);
                    output.push(0);
                }
            }
        }),
    );
}
