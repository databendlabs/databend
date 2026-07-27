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
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::vectorize_4_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use jiff::SignedDuration;
use jiff::Timestamp;
use jiff::ToSpan;
use jiff::Unit;
use jiff::civil::Date;
use jiff::civil::DateTime;
use jiff::civil::Weekday;
use jiff::tz::TimeZone;

#[derive(Debug, Clone, Copy)]
enum TimePart {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    None,
}

impl TimePart {
    fn from(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "YEAR" => Self::Year,
            "QUARTER" => Self::Quarter,
            "MONTH" => Self::Month,
            "WEEK" => Self::Week,
            "DAY" => Self::Day,
            "HOUR" => Self::Hour,
            "MINUTE" => Self::Minute,
            "SECOND" => Self::Second,
            _ => Self::None,
        }
    }

    fn date_part(&self) -> bool {
        matches!(
            self,
            Self::Year | Self::Quarter | Self::Month | Self::Week | Self::Day
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum StartOrEnd {
    Start,
    End,
    None,
}

impl StartOrEnd {
    fn from(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "START" => Self::Start,
            "END" => Self::End,
            _ => Self::None,
        }
    }
}

/// Floor division for i64 (uses div_euclid which floors toward -inf for positive divisor).
fn floordiv(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

/// Epoch reference for alignment
fn epoch_date() -> Date {
    Date::new(1970, 1, 1).unwrap()
}

/// Add months to a date safely (handles negative months and day overflow)
fn add_months_to_date(date: Date, months: i64) -> Date {
    // Represent months from year 0 as i64 to avoid overflow; year is i32 so convert
    let year = date.year() as i64;
    let month0 = (date.month() - 1) as i64; // 0..11
    let total_month0 = year * 12 + month0 + months;
    let new_year = (total_month0 / 12) as i32;
    let new_month0 = total_month0.rem_euclid(12) as u32; // 0..11
    let new_month = new_month0 + 1; // 1..12

    // day adjust: cap by last day of new month
    let new_day = std::cmp::min(date.day() as u32, last_day_of_month(new_year, new_month)) as i8;
    Date::new(new_year as i16, new_month as i8, new_day).unwrap()
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    // get first day of next month then -1 day
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next = Date::new(ny as i16, nm as i8, 1).unwrap();

    (first_of_next - 1.day()).day() as u32
}

fn time_slice_timestamp(
    ts: i64,
    slice_length: u64,
    part: TimePart,
    start_or_end: StartOrEnd,
    week_start: Weekday,
    tz: &TimeZone,
) -> i64 {
    let slice_length = slice_length as i64;

    let ts = timestamp_from_micros(ts, tz);
    let dt = ts.datetime();

    let start = match part {
        TimePart::Year | TimePart::Quarter | TimePart::Month | TimePart::Week | TimePart::Day => {
            let date = convert_start_date(dt.date(), part, week_start, slice_length);
            DateTime::from(date)
        }
        TimePart::Hour | TimePart::Minute | TimePart::Second => {
            let unit_seconds = match part {
                TimePart::Hour => 3600,
                TimePart::Minute => 60,
                TimePart::Second => 1,
                _ => unreachable!(),
            };
            let total_unit_seconds = unit_seconds * slice_length;
            let secs = ts.timestamp().as_second();
            let slice_index = floordiv(secs, total_unit_seconds);
            let start_secs = slice_index * total_unit_seconds;
            Timestamp::new(start_secs, 0)
                .unwrap()
                .to_zoned(tz.clone())
                .datetime()
        }
        TimePart::None => unreachable!(),
    };

    let result = match start_or_end {
        StartOrEnd::Start => start,
        StartOrEnd::End => add_units_to_datetime(start, slice_length, part),
        _ => unreachable!(),
    };

    result
        .to_zoned(tz.clone())
        .unwrap()
        .timestamp()
        .as_microsecond()
}

fn add_units_to_datetime(start: DateTime, slice_length: i64, part: TimePart) -> DateTime {
    match part {
        TimePart::Year | TimePart::Quarter | TimePart::Month | TimePart::Week | TimePart::Day => {
            let new_date = add_units_to_date(start.date(), slice_length, part);
            DateTime::from(new_date)
        }
        TimePart::Hour => start + (slice_length * 3600).seconds(),
        TimePart::Minute => start + (slice_length * 60).seconds(),
        TimePart::Second => start + slice_length.seconds(),
        TimePart::None => unreachable!(),
    }
}

fn convert_start_date(date: Date, part: TimePart, week_start: Weekday, slice_length: i64) -> Date {
    match part {
        TimePart::Year => {
            // Years: convert year to offset from 1970, floor to slice, align to Jan 1
            let year_offset = (date.year() as i64) - 1970;
            let slice_index = floordiv(year_offset, slice_length);
            let start_year = 1970 + slice_index * slice_length;
            // START aligned to first day of that year
            Date::new(start_year as i16, 1, 1).unwrap()
        }
        TimePart::Quarter | TimePart::Month => {
            // Months/Quarters: compute months since 1970-01 (0-based), slice by slice_months
            let unit_months = match part {
                TimePart::Quarter => 3,
                TimePart::Month => 1,
                _ => unreachable!(),
            };
            // months_since_epoch: months since 1970-01 (1970-01 = 0)
            let months_since_epoch = (date.year() as i64 - 1970) * 12 + ((date.month() - 1) as i64);
            let slice_months = slice_length * unit_months;
            let slice_index = floordiv(months_since_epoch, slice_months);
            let start_months_total = slice_index * slice_months;
            let start_year = (1970 + start_months_total / 12) as i32;
            let start_month0 = (start_months_total.rem_euclid(12)) as u32;
            let start_month = start_month0 + 1;
            // START aligned to first day of that month
            Date::new(start_year as i16, start_month as i8, 1).unwrap()
        }
        TimePart::Week => {
            // Weeks: compute epoch's week start per week_start, then floor slices by weeks
            let epoch = epoch_date();
            let epoch_wd = epoch.weekday();
            // convert weekday to Monday=0 offset
            let ep = epoch_wd.to_monday_zero_offset() as i32;
            let ws = week_start.to_monday_zero_offset() as i32;
            // delta = days from epoch back to the week's start
            let delta = (ep - ws).rem_euclid(7) as i64;
            let start_of_epoch_week = epoch - delta.days();
            // days from start_of_epoch_week to input date
            let days_since_start = (date - start_of_epoch_week).get_days();
            let slice_days = slice_length * 7;
            let slice_index = floordiv(days_since_start as i64, slice_days);
            start_of_epoch_week + (slice_index * slice_days).days()
        }
        TimePart::Day => {
            // Days: floor by days since epoch
            let days_since_epoch = (date - epoch_date()).get_days();
            let slice_index = floordiv(days_since_epoch as i64, slice_length);
            epoch_date() + (slice_index * slice_length).days()
        }
        _ => unreachable!(),
    }
}

fn time_slice_date(
    date: i32,
    slice_length: u64,
    part: TimePart,
    start_or_end: StartOrEnd,
    week_start: Weekday,
) -> i32 {
    let dur = SignedDuration::from_hours((date * 24) as i64);
    let date = jiff::civil::date(1970, 1, 1).checked_add(dur).unwrap();
    let slice_length = slice_length as i64;

    let start = convert_start_date(date, part, week_start, slice_length);

    // Return Start or End (End = Start + slice_length * unit)
    let result = match start_or_end {
        StartOrEnd::Start => start,
        StartOrEnd::End => add_units_to_date(start, slice_length, part),
        _ => unreachable!(),
    };

    result
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

fn add_units_to_date(start: Date, slice_length: i64, part: TimePart) -> Date {
    match part {
        TimePart::Year => add_months_to_date(start, slice_length * 12),
        TimePart::Quarter => add_months_to_date(start, slice_length * 3),
        TimePart::Month => add_months_to_date(start, slice_length),
        TimePart::Week => start + (slice_length * 7).days(),
        TimePart::Day => start + slice_length.days(),
        _ => unreachable!(),
    }
}

pub(super) fn register(registry: &mut FunctionRegistry) {
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
}
