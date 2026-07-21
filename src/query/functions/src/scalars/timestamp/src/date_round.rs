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

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::DateType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_2_arg;
use jiff::Unit;
use jiff::civil::Date;
use jiff::civil::Weekday;
use jiff::civil::date;
use jiff::civil::datetime;
use jiff::tz::TimeZone;

use crate::date_arithmetic::last_day_of_year_month;

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

fn round_timestamp(ts: i64, tz: &TimeZone, round: Round) -> i64 {
    let dtz = timestamp_from_micros(ts, tz);
    let res = match round {
        Round::Second => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute(),
                dtz.second(),
                0,
            ))
            .unwrap(),
        Round::Minute => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute(),
                0,
                0,
            ))
            .unwrap(),
        Round::FiveMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 5 * 5,
                0,
                0,
            ))
            .unwrap(),
        Round::TenMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 10 * 10,
                0,
                0,
            ))
            .unwrap(),
        Round::FifteenMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 15 * 15,
                0,
                0,
            ))
            .unwrap(),
        Round::TimeSlot => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 30 * 30,
                0,
                0,
            ))
            .unwrap(),
        Round::Hour => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                0,
                0,
                0,
            ))
            .unwrap(),
        Round::Day => tz
            .to_zoned(datetime(dtz.year(), dtz.month(), dtz.day(), 0, 0, 0, 0))
            .unwrap(),
    };
    res.timestamp().as_microsecond()
}

trait RoundDate {
    fn round(date: &Date) -> i32;
}

struct DateRounder;

impl DateRounder {
    fn eval_timestamp<T: RoundDate>(us: i64, tz: &TimeZone) -> i32 {
        T::round(&timestamp_from_micros(us, tz).date())
    }

    fn eval_date<T: RoundDate>(date: i32) -> i32 {
        T::round(&date_from_days(date))
    }
}

#[inline]
fn date_to_inner_number(date: &Date) -> i32 {
    date.since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
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
        date_to_inner_number(date) - date.weekday().to_monday_zero_offset() as i32
    }
}

impl RoundDate for ToLastSunday {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(date) - date.weekday().to_sunday_zero_offset() as i32
    }
}

impl RoundDate for ToStartOfMonth {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(&date.first_of_month())
    }
}

impl RoundDate for ToStartOfQuarter {
    fn round(input: &Date) -> i32 {
        let new_month = (input.month() - 1) / 3 * 3 + 1;
        date_to_inner_number(&date(input.year(), new_month, 1))
    }
}

impl RoundDate for ToStartOfYear {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(&date.first_of_year())
    }
}

impl RoundDate for ToStartOfISOYear {
    fn round(input: &Date) -> i32 {
        let iso_year = input.iso_week_date().year();
        for i in 1..=7 {
            let new_date = date(iso_year, 1, i);
            if new_date.iso_week_date().weekday() == Weekday::Monday {
                return date_to_inner_number(&new_date);
            }
        }
        0
    }
}

impl RoundDate for ToLastOfWeek {
    fn round(date: &Date) -> i32 {
        date_to_inner_number(date) - date.weekday().to_monday_zero_offset() as i32 + 6
    }
}

impl RoundDate for ToLastOfMonth {
    fn round(input: &Date) -> i32 {
        let day = last_day_of_year_month(input.year(), input.month());
        date_to_inner_number(&date(input.year(), input.month(), day))
    }
}

impl RoundDate for ToLastOfQuarter {
    fn round(input: &Date) -> i32 {
        let new_month = (input.month() - 1) / 3 * 3 + 3;
        let day = last_day_of_year_month(input.year(), new_month);
        date_to_inner_number(&date(input.year(), new_month, day))
    }
}

impl RoundDate for ToLastOfYear {
    fn round(input: &Date) -> i32 {
        let day = last_day_of_year_month(input.year(), 12);
        date_to_inner_number(&date(input.year(), 12, day))
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

impl_round_to_weekday!(ToPreviousMonday, Monday, true);
impl_round_to_weekday!(ToPreviousTuesday, Tuesday, true);
impl_round_to_weekday!(ToPreviousWednesday, Wednesday, true);
impl_round_to_weekday!(ToPreviousThursday, Thursday, true);
impl_round_to_weekday!(ToPreviousFriday, Friday, true);
impl_round_to_weekday!(ToPreviousSaturday, Saturday, true);
impl_round_to_weekday!(ToPreviousSunday, Sunday, true);
impl_round_to_weekday!(ToNextMonday, Monday, false);
impl_round_to_weekday!(ToNextTuesday, Tuesday, false);
impl_round_to_weekday!(ToNextWednesday, Wednesday, false);
impl_round_to_weekday!(ToNextThursday, Thursday, false);
impl_round_to_weekday!(ToNextFriday, Friday, false);
impl_round_to_weekday!(ToNextSaturday, Saturday, false);
impl_round_to_weekday!(ToNextSunday, Sunday, false);

fn previous_or_next_date_day(date: &Date, target: Weekday, is_previous: bool) -> i32 {
    let dir = if is_previous { -1 } else { 1 };
    let mut days_diff = (dir
        * (target.to_monday_zero_offset() as i32 - date.weekday().to_monday_zero_offset() as i32)
        + 7)
        % 7;

    days_diff = if days_diff == 0 { 7 } else { days_diff };

    clamp_date(date_to_inner_number(date) as i64 + (dir * days_diff) as i64)
}

pub(super) fn register(registry: &mut FunctionRegistry) {
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
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<DateType, Int64Type, DateType>(|val, mode, _| {
            if mode == 0 {
                DateRounder::eval_date::<ToLastSunday>(val)
            } else {
                DateRounder::eval_date::<ToLastMonday>(val)
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
where T: RoundDate {
    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _>(
        name,
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, _| DateRounder::eval_date::<T>(val)),
    );
    registry.register_1_arg::<TimestampType, DateType, _>(
        name,
        |_, _| FunctionDomain::Full,
        |val, ctx| DateRounder::eval_timestamp::<T>(val, &ctx.func_ctx.tz),
    );
}
