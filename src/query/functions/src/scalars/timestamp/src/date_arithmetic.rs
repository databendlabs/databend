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
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::fast_components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;
use jiff::SignedDuration;
use jiff::SpanRelativeTo;
use jiff::Unit;
use jiff::civil::Date;
use jiff::civil::DateTime;
use jiff::civil::Weekday;
use jiff::civil::date;
use jiff::tz::TimeZone;
use num_traits::AsPrimitive;

use crate::date_extract::ToNumber;
use crate::date_extract::ToQuarter;

const MICROSECS_PER_DAY: i64 = 86_400_000_000;

// Timestamp arithmetic factors.
const FACTOR_HOUR: i64 = 3600;
const FACTOR_MINUTE: i64 = 60;
const FACTOR_SECOND: i64 = 1;
const LAST_DAY_LUT: [i8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn eval_years_base(
    year: i16,
    month: i8,
    day: i8,
    delta: i64,
    _add_months: bool,
) -> std::result::Result<Date, String> {
    let new_year = year as i64 + delta;
    let mut new_day = day;
    if std::intrinsics::unlikely(month == 2 && day == 29) {
        new_day = last_day_of_year_month(new_year as i16, month);
    }
    match Date::new(new_year as i16, month, new_day) {
        Ok(d) => Ok(d),
        Err(e) => Err(format!("Invalid date: {}", e)),
    }
}

fn eval_months_base(
    year: i16,
    month: i8,
    day: i8,
    delta: i64,
    add_months: bool,
) -> std::result::Result<Date, String> {
    let total_months = (month as i64 + delta - 1) as i16;
    let mut new_year = year + (total_months / 12);
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    // For ADD_MONTHS only, if the original day is the last day of the month, the result day of month will be the last day of the result month.
    let new_month = (new_month0 + 1) as i8;
    // Determine the correct day
    let max_day = last_day_of_year_month(new_year, new_month);
    let new_day = if add_months && day == last_day_of_year_month(year, month) {
        max_day
    } else {
        day.min(max_day)
    };

    match Date::new(new_year, (new_month0 + 1) as i8, new_day) {
        Ok(d) => Ok(d),
        Err(e) => Err(format!("Invalid date: {}", e)),
    }
}

// Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
pub(super) fn last_day_of_year_month(year: i16, month: i8) -> i8 {
    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    LAST_DAY_LUT[month as usize]
}

macro_rules! impl_interval_year_month {
    ($vis:vis $name:ident, $op:expr) => {
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

                Ok(clamp_date(
                    new_date
                        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                        .unwrap()
                        .get_days() as i64,
                ))
            }

            $vis fn eval_timestamp(
                us: i64,
                tz: &TimeZone,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i64, String> {
                let ts = timestamp_from_micros(us, tz);
                let original_offset = ts.offset().seconds();

                if let Some(components) = fast_components_from_timestamp(us, tz) {
                    let new_date = $op(
                        components.year as i16,
                        components.month as i8,
                        components.day as i8,
                        delta.as_(),
                        add_months,
                    )?;
                    if let Some(mut new_ts) = fast_utc_from_local(
                        tz,
                        new_date.year() as i32,
                        new_date.month() as u8,
                        new_date.day() as u8,
                        components.hour,
                        components.minute,
                        components.second,
                        components.micro,
                    ) {
                        if let Some(new_components) = fast_components_from_timestamp(new_ts, tz) {
                            if new_components.offset_seconds != original_offset {
                                let shift_secs =
                                    (new_components.offset_seconds - original_offset) as i64;
                                let shift_micros = shift_secs.saturating_mul(MICROS_PER_SEC);
                                new_ts = new_ts.checked_add(shift_micros).unwrap_or_else(|| {
                                    if shift_micros.is_negative() {
                                        i64::MIN
                                    } else {
                                        i64::MAX
                                    }
                                });
                            }
                            clamp_timestamp(&mut new_ts);
                            return Ok(new_ts);
                        }
                    }
                }

                let new_date = $op(ts.year(), ts.month(), ts.day(), delta.as_(), add_months)?;

                let local =
                    new_date.at(ts.hour(), ts.minute(), ts.second(), ts.subsec_nanosecond());
                let mut zoned = match local.to_zoned(tz.clone()) {
                    Ok(z) => z,
                    Err(e) => match local.checked_add(SignedDuration::from_secs(3600)) {
                        Ok(res2) => res2
                            .to_zoned(tz.clone())
                            .map_err(|err| format!("{}", err))?,
                        Err(_) => return Err(format!("{}", e)),
                    },
                };
                if zoned.offset().seconds() != original_offset {
                    let shift = (zoned.offset().seconds() - original_offset) as i64;
                    if let Ok(adj_local) = local.checked_add(SignedDuration::from_secs(shift)) {
                        if let Ok(adj_zoned) = adj_local.to_zoned(tz.clone()) {
                            zoned = adj_zoned;
                        }
                    }
                }
                let mut ts = zoned.timestamp().as_microsecond();
                clamp_timestamp(&mut ts);
                Ok(ts)
            }
        }
    };
}

impl_interval_year_month!(EvalYearsImpl, eval_years_base);
impl_interval_year_month!(pub EvalMonthsImpl, eval_months_base);

/// Compare two `DateTimeComponents` by their time-of-day portion only.
fn components_time_less_than(a: &DateTimeComponents, b: &DateTimeComponents) -> bool {
    (a.hour, a.minute, a.second, a.micro) < (b.hour, b.minute, b.second, b.micro)
}

fn date_from_components(c: &DateTimeComponents) -> Option<Date> {
    Date::new(c.year as i16, c.month as i8, c.day as i8).ok()
}

#[inline]
pub(super) fn timestamp_tz_components_via_lut(value: timestamp_tz) -> Option<DateTimeComponents> {
    let offset = value.micros_offset()?;
    let local = value.timestamp().checked_add(offset)?;
    fast_components_from_timestamp(local, &TimeZone::UTC)
}

fn datetime_from_components(c: &DateTimeComponents) -> Option<DateTime> {
    let date = date_from_components(c)?;
    Some(date.at(
        c.hour as i8,
        c.minute as i8,
        c.second as i8,
        (c.micro * 1_000) as i32,
    ))
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

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            return (end.year as i64) - (start.year as i64);
        }
        let date_start = timestamp_from_micros(date_start, tz);
        let date_end = timestamp_from_micros(date_end, tz);
        date_end.year() as i64 - date_start.year() as i64
    }

    fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
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
            return years as i64;
        }
        let start = timestamp_from_micros(date_start, tz);
        let end = timestamp_from_micros(date_end, tz);

        let mut years = end.year() - start.year();

        // Handle special cases on February 29 in leap years:
        // If the start date is February 29 and the end date is February 28, it is considered a full year (leap year to regular year).
        // Otherwise, the end date, month day, must be >= the start date, month day, and the time must be reached
        let start_month = start.month();
        let start_day = start.day();

        let end_month = end.month();
        let end_day = end.day();

        let start_is_feb_29 = start_month == 2 && start_day == 29;
        let end_is_feb_28 = end_month == 2 && end_day == 28;

        let end_before_start_date = (end_month < start_month)
            || (end_month == start_month && end_day < start_day)
            || (end_month == start_month && end_day == start_day && end.time() < start.time());

        if start_is_feb_29 && end_is_feb_28 {
        } else if end_before_start_date {
            years -= 1;
        }

        years as i64
    }
}

struct EvalISOYearsImpl;
impl EvalISOYearsImpl {
    fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        let date_start = date_from_days(date_start);
        let date_end = date_from_days(date_end);
        date_end.iso_week_date().year() as i32 - date_start.iso_week_date().year() as i32
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
        let mut years = date_end.iso_week_date().year() - date_start.iso_week_date().year();
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let (start_year, _) = start.iso_year_week();
            let (end_year, _) = end.iso_year_week();
            return (end_year - start_year) as i64;
        }
        let date_start = timestamp_from_micros(date_start, tz);
        let date_end = timestamp_from_micros(date_end, tz);
        date_end.date().iso_week_date().year() as i64 - date_start.iso_week_date().year() as i64
    }

    fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
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
            return years as i64;
        }

        let start = timestamp_from_micros(date_start, tz);
        let end = timestamp_from_micros(date_end, tz);
        let mut years =
            end.date().iso_week_date().year() as i64 - start.date().iso_week_date().year() as i64;
        let start_month = start.month();
        let start_day = start.day();

        let end_month = end.month();
        let end_day = end.day();

        let start_is_feb_29 = start_month == 2 && start_day == 29;
        let end_is_feb_28 = end_month == 2 && end_day == 28;

        let end_before_start_date = (end_month < start_month)
            || (end_month == start_month && end_day < start_day)
            || (end_month == start_month && end_day == start_day && end.time() < start.time());

        if start_is_feb_29 && end_is_feb_28 {
        } else if end_before_start_date {
            years -= 1;
        }

        years
    }
}

struct EvalYearWeeksImpl;
impl EvalYearWeeksImpl {
    fn yearweek(date: Date) -> i32 {
        let iso_week = date.iso_week_date();
        (iso_week.year() as i32 * 100) + iso_week.week() as i32
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

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let start_yw = Self::yearweek_from_components(&start) as i64;
            let end_yw = Self::yearweek_from_components(&end) as i64;
            return end_yw - start_yw;
        }
        let date_start = timestamp_from_micros(date_start, tz);
        let date_end = timestamp_from_micros(date_end, tz);
        let end = Self::yearweek(date_end.date()) as i64;
        let start = Self::yearweek(date_start.date()) as i64;

        end - start
    }

    // In duckdb datesub(yearweek, ) is same as datesub(week, ) But we can contain these logic
    // fn week_end(date: Date) -> Date {
    // let weekday = date.weekday();
    //
    // let days_to_sunday = 7 - weekday.to_monday_one_offset(); // monday=1, sunday=7
    // let dur = SignedDuration::from_hours(days_to_sunday as i64 * 24);
    // date.checked_add(dur).unwrap()
    // }
    // pub fn eval_date_between(start: i32, end: i32, tz: &TimeZone) -> i32 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = date_from_days(earlier);
    // let later = date_from_days(later);
    //
    // let start_yw = Self::yearweek(earlier);
    // let end_yw = Self::yearweek(later);
    //
    // let mut diff = end_yw - start_yw;
    //
    // If the end week is incomplete, subtract 1
    // if later < Self::week_end(later) {
    // diff -= 1;
    // }
    //
    // diff * sign
    // }
    // pub fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = timestamp_from_micros(earlier, tz);
    // let later = timestamp_from_micros(later, tz);
    //
    // let start_yw = Self::yearweek(earlier.date());
    // let end_yw = Self::yearweek(later.date());
    //
    // let mut diff = end_yw - start_yw;
    //
    // let week_end = EvalYearWeeksImpl::week_end(later.date());
    // if later.datetime() < week_end.at(23, 59, 59, 999_999_999) {
    // diff -= 1;
    // }
    //
    // diff as i64 * sign
    // }
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

    fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let start_quarter = ((start.month as i64 - 1) / 3) + 1;
            let end_quarter = ((end.month as i64 - 1) / 3) + 1;
            return (end.year as i64 - start.year as i64) * 4 + end_quarter - start_quarter;
        }
        let date_start = timestamp_from_micros(date_start, tz);
        let date_end = timestamp_from_micros(date_end, tz);
        (date_end.year() - date_start.year()) as i64 * 4 + ToQuarter::to_number(&date_end) as i64
            - ToQuarter::to_number(&date_start) as i64
    }

    // Return date corresponding to quarter number (1~4)
    // fn quarter(month: i8) -> i32 {
    // ((month - 1) / 3 + 1) as i32
    // }
    //
    //
    // fn quarter_start(year: i16, month: i8) -> (i16, i8) {
    // let q = ((month - 1) / 3) + 1;
    // let start_month = (q - 1) * 3 + 1;
    // (year, start_month)
    // }
    //
    // DuckDB directly calc month/3
    // pub fn eval_date_between(start: i32, end: i32, tz: &TimeZone) -> i32 {
    // if start == end {
    // return 0;
    // }
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = date_from_days(earlier);
    // let later = date_from_days(later);
    //
    // let start_year = earlier.year();
    // let start_quarter = Self::quarter(earlier.month());
    // let end_year = later.year();
    // let end_quarter = Self::quarter(later.month());
    //
    // let mut diff =
    // (end_year - start_year) as i64 * 4 + (end_quarter as i64 - start_quarter as i64);
    //
    // let (last_quarter_start_year, last_quarter_start_month) =
    // Self::quarter_start(end_year, later.month());
    // let last_quarter_start_date = date(last_quarter_start_year, last_quarter_start_month, 1);
    //
    //
    // if later < last_quarter_start_date {
    // diff -= 1;
    // }
    //
    // (diff * sign) as i32
    // }
    // pub fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = timestamp_from_micros(earlier, tz);
    // let later = timestamp_from_micros(later, tz);
    //
    // let start_year = earlier.year();
    // let start_quarter = Self::quarter(earlier.month());
    // let end_year = later.year();
    // let end_quarter = Self::quarter(later.month());
    //
    // let mut diff =
    // (end_year - start_year) as i64 * 4 + (end_quarter as i64 - start_quarter as i64);
    //
    // let (last_quarter_start_year, last_quarter_start_month) =
    // Self::quarter_start(later.year(), later.month());
    // let last_quarter_start_date = date(last_quarter_start_year, last_quarter_start_month, 1);
    // let last_quarter_start_datetime = last_quarter_start_date.to_datetime(earlier.time());
    //
    // if later.datetime() < last_quarter_start_datetime {
    // diff -= 1;
    // }
    // diff * sign
    // }
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

    fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }
        if let (Some(start_c), Some(end_c)) = (
            fast_components_from_timestamp(start, tz),
            fast_components_from_timestamp(end, tz),
        ) {
            let year_diff = end_c.year - start_c.year;
            let month_diff = end_c.month as i32 - start_c.month as i32;
            let mut months = year_diff as i64 * 12 + month_diff as i64;
            if (end_c.day < start_c.day)
                || (end_c.day == start_c.day && components_time_less_than(&end_c, &start_c))
            {
                months -= 1;
            }
            return months;
        }

        let start = timestamp_from_micros(start, tz);
        let end = timestamp_from_micros(end, tz);
        let year_diff = end.year() - start.year();
        let month_diff = end.month() as i64 - start.month() as i64;
        let mut months = year_diff as i64 * 12 + month_diff;

        // Determine the time sequence. If the end time is less than the start time, it is incomplete
        if (end.day() < start.day()) || (end.day() == start.day() && end.time() < start.time()) {
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
        let date_a = Date::new(1970, 1, 1)
            .unwrap()
            .checked_add(SignedDuration::from_hours(date_a as i64 * 24))
            .unwrap();
        let date_b = Date::new(1970, 1, 1)
            .unwrap()
            .checked_add(SignedDuration::from_hours(date_b as i64 * 24))
            .unwrap();

        let year_diff = (date_a.year() - date_b.year()) as i64;
        let month_diff = date_a.month() as i64 - date_b.month() as i64;

        // Calculate total months difference
        let total_months_diff = year_diff * 12 + month_diff;

        // Determine if special case for fractional part applies
        let is_same_day_of_month = date_a.day() == date_b.day();

        let are_both_end_of_month =
            date_a.last_of_month() == date_a && date_b.last_of_month() == date_b;
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
            // Get the first day of the year
            let first_day = date(year as i16, 1, 1);

            // Determine the weekday of the first day
            let weekday = first_day.weekday();

            // Check if the year starts on a Thursday.
            if weekday == Weekday::Thursday {
                return 53;
            }

            // Check if the year starts on a Wednesday and is a leap year.
            if weekday == Weekday::Wednesday
                && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))
            {
                return 53;
            }
            52
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
            earlier.iso_week_date().week() as u32,
            later.iso_week_date().week() as u32,
        );
        // Judge whether it is complete after the last week
        let end_weekday = later.weekday();
        let days_since_monday = end_weekday.to_monday_one_offset() - 1;
        let dur = SignedDuration::from_hours(days_since_monday as i64 * 24);
        let monday_of_end_week = later.checked_sub(dur).unwrap();

        if later < monday_of_end_week {
            weeks -= 1;
        }

        weeks
    }

    fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }
        if let (Some(start_c), Some(end_c)) = (
            fast_components_from_timestamp(start, tz),
            fast_components_from_timestamp(end, tz),
        ) {
            if let (Some(start_date), Some(end_date)) =
                (date_from_components(&start_c), date_from_components(&end_c))
            {
                let mut weeks = Self::calculate_weeks_between_years(
                    start_date.year() as i32,
                    end_date.year() as i32,
                    start_date.iso_week_date().week() as u32,
                    end_date.iso_week_date().week() as u32,
                ) as i64;
                let days_since_monday = end_c.weekday.to_monday_one_offset() - 1;
                let dur = SignedDuration::from_hours(days_since_monday as i64 * 24);
                let monday_of_end_week = end_date.checked_sub(dur).unwrap();
                let monday_dt = monday_of_end_week.at(0, 0, 0, 0);
                if let Some(end_dt) = datetime_from_components(&end_c) {
                    if end_dt < monday_dt {
                        weeks -= 1;
                    }
                }
                return weeks;
            }
        }

        let earlier = timestamp_from_micros(start, tz);
        let later = timestamp_from_micros(end, tz);

        let mut weeks = Self::calculate_weeks_between_years(
            earlier.year() as i32,
            later.year() as i32,
            earlier.date().iso_week_date().week() as u32,
            later.date().iso_week_date().week() as u32,
        ) as i64;
        // Judge whether it is complete after the last week
        let end_date = later.date();
        let end_weekday = end_date.weekday();
        let days_since_monday = end_weekday.to_monday_one_offset() - 1;
        let dur = SignedDuration::from_hours(days_since_monday as i64 * 24);
        let monday_of_end_week = end_date.checked_sub(dur).unwrap();
        let monday_of_end_week_datetime = monday_of_end_week.at(0, 0, 0, 0);

        if later.datetime() < monday_of_end_week_datetime {
            weeks -= 1;
        }
        weeks
    }
}

pub(super) struct EvalDaysImpl;

impl EvalDaysImpl {
    pub(super) fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> i32 {
        clamp_date((date as i64).wrapping_add(delta.as_()))
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

    pub(super) fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let start = timestamp_from_micros(start, tz);
        let end = timestamp_from_micros(end, tz);
        let mut full_days = (end.date() - start.date())
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .unwrap()
            .as_hours()
            / 24;
        let end_time = end.time();
        let start_time = start.time();
        if end_time < start_time {
            full_days -= 1;
        }
        full_days
    }
}

struct EvalTimesImpl;

impl EvalTimesImpl {
    fn eval_timestamp(us: i64, delta: impl AsPrimitive<i64>, factor: i64) -> i64 {
        let mut ts = us.wrapping_add(delta.as_().wrapping_mul(factor * MICROS_PER_SEC));
        clamp_timestamp(&mut ts);
        ts
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

        let duration = SignedDuration::from_micros(end - start);

        match unit {
            "hours" => duration.as_hours(),
            "minutes" => duration.as_mins(),
            "seconds" => duration.as_secs(),
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

// Compute a correct date domain from a raw arithmetic range.
// `clamp_date` maps out-of-range values to DATE_MIN (non-monotonic), so naively
// clamping endpoints can produce a reversed domain. This function accounts for
// partial overlap with the valid date range.
fn clamp_date_domain(raw_min: i64, raw_max: i64) -> SimpleDomain<i32> {
    if raw_min >= DATE_MIN as i64 && raw_max <= DATE_MAX as i64 {
        SimpleDomain {
            min: raw_min as i32,
            max: raw_max as i32,
        }
    } else if raw_min > DATE_MAX as i64 || raw_max < DATE_MIN as i64 {
        SimpleDomain {
            min: DATE_MIN,
            max: DATE_MIN,
        }
    } else {
        SimpleDomain {
            min: DATE_MIN,
            max: raw_max.min(DATE_MAX as i64) as i32,
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
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                let raw_min = ln.saturating_add(rn);
                let raw_max = lm.saturating_add(rm);
                Some(FunctionDomain::Domain(clamp_date_domain(raw_min, raw_max)))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date((a as i64).saturating_add(b)))
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
                let raw_min = ln.saturating_add(rn);
                let raw_max = lm.saturating_add(rm);
                Some(FunctionDomain::Domain(clamp_timestamp_domain(
                    raw_min, raw_max,
                )))
            }
            .unwrap_or(FunctionDomain::MayThrow)
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
            (|| {
                let lm: i64 = num_traits::cast::cast(lhs.max)?;
                let ln: i64 = num_traits::cast::cast(lhs.min)?;
                let rm = rhs.max;
                let rn = rhs.min;

                let raw_min = ln.saturating_sub(rm);
                let raw_max = lm.saturating_sub(rn);
                Some(FunctionDomain::Domain(clamp_date_domain(raw_min, raw_max)))
            })()
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|a, b, output, _| {
            output.push(clamp_date((a as i64).saturating_sub(b)));
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
                let raw_min = ln.saturating_sub(rm);
                let raw_max = lm.saturating_sub(rn);
                Some(FunctionDomain::Domain(clamp_timestamp_domain(
                    raw_min, raw_max,
                )))
            }
            .unwrap_or(FunctionDomain::MayThrow)
        },
        vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
            |a, b, output, _| {
                let mut minus = a.saturating_sub(b);
                clamp_timestamp(&mut minus);
                output.push(minus);
            },
        ),
    );
}
