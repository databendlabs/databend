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

use std::sync::LazyLock;

use databend_common_column::types::timestamp_tz;
use databend_common_exception::Result;
use databend_common_timezone::fast_components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;
use databend_common_timezone::DateTimeComponents;
use jiff::civil::date;
use jiff::civil::datetime;
use jiff::civil::Date;
use jiff::civil::DateTime;
use jiff::civil::Time;
use jiff::civil::Weekday;
use jiff::tz::TimeZone;
use jiff::SignedDuration;
use jiff::SpanRelativeTo;
use jiff::Timestamp;
use jiff::ToSpan;
use jiff::Unit;
use jiff::Zoned;
use num_traits::AsPrimitive;

use crate::types::date::clamp_date;
use crate::types::timestamp::clamp_timestamp;
use crate::types::timestamp::MICROS_PER_SEC;

// jiff's `Timestamp` only accepts UTC seconds in
// [-377705023201, 253402207200] so that any ±25:59:59 offset still
// yields a valid civil datetime. Clamp after splitting into seconds
// and sub-second nanoseconds to avoid constructing out-of-range values.
const JIFF_TIMESTAMP_MIN_SEC: i64 = -377705023201;
const JIFF_TIMESTAMP_MAX_SEC: i64 = 253402207200;

pub trait DateConverter {
    fn to_date(&self, tz: &TimeZone) -> Date;
    fn to_timestamp(&self, tz: &TimeZone) -> Zoned;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, _tz: &TimeZone) -> Date {
        let dur = SignedDuration::from_hours(self.as_() * 24);
        date(1970, 1, 1).checked_add(dur).unwrap()
    }

    fn to_timestamp(&self, tz: &TimeZone) -> Zoned {
        // Can't use `tz.timestamp_nanos(self.as_() * 1000)` directly, is may cause multiply with overflow.
        let micros = self.as_();
        let (mut secs, mut nanos) = (micros / MICROS_PER_SEC, (micros % MICROS_PER_SEC) * 1_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }
        if secs > JIFF_TIMESTAMP_MAX_SEC {
            secs = JIFF_TIMESTAMP_MAX_SEC;
            nanos = 0;
        } else if secs < JIFF_TIMESTAMP_MIN_SEC {
            secs = JIFF_TIMESTAMP_MIN_SEC;
            nanos = 0;
        }
        let ts = Timestamp::new(secs, nanos as i32).unwrap();
        ts.to_zoned(tz.clone())
    }
}

pub const MICROSECS_PER_DAY: i64 = 86_400_000_000;

// Timestamp arithmetic factors.
pub const FACTOR_HOUR: i64 = 3600;
pub const FACTOR_MINUTE: i64 = 60;
pub const FACTOR_SECOND: i64 = 1;
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
fn last_day_of_year_month(year: i16, month: i8) -> i8 {
    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    LAST_DAY_LUT[month as usize]
}

macro_rules! impl_interval_year_month {
    ($name: ident, $op: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl $name {
            pub fn eval_date(
                date: i32,
                tz: &TimeZone,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i32, String> {
                let date = date.to_date(tz);
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

            pub fn eval_timestamp(
                us: i64,
                tz: &TimeZone,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i64, String> {
                let ts = us.to_timestamp(tz);
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
impl_interval_year_month!(EvalMonthsImpl, eval_months_base);

/// Compare two `DateTimeComponents` by their time-of-day portion only.
fn components_time_less_than(a: &DateTimeComponents, b: &DateTimeComponents) -> bool {
    (a.hour, a.minute, a.second, a.micro) < (b.hour, b.minute, b.second, b.micro)
}

fn date_from_components(c: &DateTimeComponents) -> Option<Date> {
    Date::new(c.year as i16, c.month as i8, c.day as i8).ok()
}

#[inline]
pub fn timestamp_tz_local_micros(value: timestamp_tz) -> Option<i64> {
    let offset = value.micros_offset()?;
    value.timestamp().checked_add(offset)
}

#[inline]
pub fn timestamp_tz_components_via_lut(value: timestamp_tz) -> Option<DateTimeComponents> {
    let local = timestamp_tz_local_micros(value)?;
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
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32
    }

    pub fn eval_date_between(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start, tz);
        }

        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);

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

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            return (end.year as i64) - (start.year as i64);
        }
        let date_start = date_start.to_timestamp(tz);
        let date_end = date_end.to_timestamp(tz);
        date_end.year() as i64 - date_start.year() as i64
    }

    pub fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
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
        let start = date_start.to_timestamp(tz);
        let end = date_end.to_timestamp(tz);

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

pub struct EvalISOYearsImpl;
impl EvalISOYearsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);
        date_end.iso_week_date().year() as i32 - date_start.iso_week_date().year() as i32
    }

    pub fn eval_date_between(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start, tz);
        }
        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);
        let mut years = date_end.iso_week_date().year() - date_start.iso_week_date().year();
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let (start_year, _) = start.iso_year_week();
            let (end_year, _) = end.iso_year_week();
            return (end_year - start_year) as i64;
        }
        let date_start = date_start.to_timestamp(tz);
        let date_end = date_end.to_timestamp(tz);
        date_end.date().iso_week_date().year() as i64 - date_start.iso_week_date().year() as i64
    }

    pub fn eval_timestamp_between(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
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

        let start = date_start.to_timestamp(tz);
        let end = date_end.to_timestamp(tz);
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

pub struct EvalYearWeeksImpl;
impl EvalYearWeeksImpl {
    fn yearweek(date: Date) -> i32 {
        let iso_week = date.iso_week_date();
        (iso_week.year() as i32 * 100) + iso_week.week() as i32
    }

    fn yearweek_from_components(components: &DateTimeComponents) -> i32 {
        let (year, week) = components.iso_year_week();
        year * 100 + week as i32
    }

    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);
        let end = Self::yearweek(date_end);
        let start = Self::yearweek(date_start);

        end - start
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let start_yw = Self::yearweek_from_components(&start) as i64;
            let end_yw = Self::yearweek_from_components(&end) as i64;
            return end_yw - start_yw;
        }
        let date_start = date_start.to_timestamp(tz);
        let date_end = date_end.to_timestamp(tz);
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
    // let earlier = earlier.to_date(tz);
    // let later = later.to_date(tz);
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
    // let earlier = earlier.to_timestamp(tz);
    // let later = later.to_timestamp(tz);
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

pub struct EvalQuartersImpl;

impl EvalQuartersImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        EvalQuartersImpl::eval_timestamp_diff(
            date_start as i64 * MICROSECS_PER_DAY,
            date_end as i64 * MICROSECS_PER_DAY,
            tz,
        ) as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        if let (Some(start), Some(end)) = (
            fast_components_from_timestamp(date_start, tz),
            fast_components_from_timestamp(date_end, tz),
        ) {
            let start_quarter = ((start.month as i64 - 1) / 3) + 1;
            let end_quarter = ((end.month as i64 - 1) / 3) + 1;
            return (end.year as i64 - start.year as i64) * 4 + end_quarter - start_quarter;
        }
        let date_start = date_start.to_timestamp(tz);
        let date_end = date_end.to_timestamp(tz);
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
    // let earlier = earlier.to_date(tz);
    // let later = later.to_date(tz);
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
    // let earlier = earlier.to_timestamp(tz);
    // let later = later.to_timestamp(tz);
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
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: &TimeZone) -> i32 {
        let date_start = date_start.to_date(tz);
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32 * 12 + date_end.month() as i32
            - date_start.month() as i32
    }

    pub fn eval_date_between(start: i32, end: i32, tz: &TimeZone) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start, tz);
        }

        let start = start.to_date(tz);
        let end = end.to_date(tz);

        let year_diff = end.year() - start.year();
        let month_diff = end.month() as i32 - start.month() as i32;
        let mut months = year_diff as i32 * 12 + month_diff;

        if end.day() < start.day() {
            months -= 1;
        }

        months
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: &TimeZone) -> i64 {
        EvalMonthsImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
            tz,
        ) as i64
    }

    pub fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
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

        let start = start.to_timestamp(tz);
        let end = end.to_timestamp(tz);
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
    pub fn months_between_ts(ts_a: i64, ts_b: i64) -> f64 {
        EvalMonthsImpl::months_between(
            (ts_a / 86_400_000_000) as i32,
            (ts_b / 86_400_000_000) as i32,
        )
    }

    pub fn months_between(date_a: i32, date_b: i32) -> f64 {
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

pub struct EvalWeeksImpl;

impl EvalWeeksImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        // 1970-01-01 is ThursDay
        let date_start = date_start / 7 + (date_start % 7 >= 4) as i32;
        let date_end = date_end / 7 + (date_end % 7 >= 4) as i32;
        date_end - date_start
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
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

    pub fn eval_date_between(start: i32, end: i32, tz: &TimeZone) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start, tz);
        }

        let earlier = start.to_date(tz);
        let later = end.to_date(tz);
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

    pub fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
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

        let earlier = start.to_timestamp(tz);
        let later = end.to_timestamp(tz);

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

pub struct EvalDaysImpl;

impl EvalDaysImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> i32 {
        clamp_date((date as i64).wrapping_add(delta.as_()))
    }

    pub fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        date_end - date_start
    }

    pub fn eval_timestamp(date: i64, delta: impl AsPrimitive<i64>) -> i64 {
        let mut value = date.wrapping_add(delta.as_().wrapping_mul(MICROSECS_PER_DAY));
        clamp_timestamp(&mut value);
        value
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalDaysImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    pub fn eval_timestamp_between(start: i64, end: i64, tz: &TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let start = start.to_timestamp(tz);
        let end = end.to_timestamp(tz);
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

pub struct EvalTimesImpl;

impl EvalTimesImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>, factor: i64) -> i32 {
        clamp_date(
            (date as i64 * MICROSECS_PER_DAY)
                .wrapping_add(delta.as_().wrapping_mul(factor * MICROS_PER_SEC)),
        )
    }

    pub fn eval_timestamp(us: i64, delta: impl AsPrimitive<i64>, factor: i64) -> i64 {
        let mut ts = us.wrapping_add(delta.as_().wrapping_mul(factor * MICROS_PER_SEC));
        clamp_timestamp(&mut ts);
        ts
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, factor: i64) -> i64 {
        let date_start = date_start / (MICROS_PER_SEC * factor);
        let date_end = date_end / (MICROS_PER_SEC * factor);
        date_end - date_start
    }

    pub fn eval_timestamp_between(unit: &str, start: i64, end: i64) -> i64 {
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

#[inline]
pub fn today_date(now: &Zoned, tz: &TimeZone) -> i32 {
    let now = now.with_time_zone(tz.clone());
    now.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

// Summer Time in 1990 began at 2 a.m. (Beijing time) on Sunday, April 15th and ended at 2 a.m. (Beijing Daylight Saving Time) on Sunday, September 16th.
// During this period, the summer working hours will be implemented, namely from April 15th to September 16th.
// The working hours of all departments of The State Council are from 8 a.m. to 12 p.m. and from 1:30 p.m. to 5:30 p.m. The winter working hours will be implemented after September 17th.
pub fn calc_date_to_timestamp(val: i32, tz: &TimeZone) -> std::result::Result<i64, String> {
    let ts = (val as i64) * 24 * 3600 * MICROS_PER_SEC;
    let local_date = val.to_date(tz);
    let year = i32::from(local_date.year());
    let month = local_date.month() as u8;
    let day = local_date.day() as u8;

    if let Some(micros) = fast_utc_from_local(tz, year, month, day, 0, 0, 0, 0) {
        return Ok(micros);
    }

    let midnight = local_date.to_datetime(Time::midnight());
    match midnight.to_zoned(tz.clone()) {
        Ok(zoned) => Ok(zoned.timestamp().as_microsecond()),
        Err(_err) => {
            for minutes in 1..=1440 {
                let delta = SignedDuration::from_secs((minutes * 60) as i64);
                if let Ok(adj) = midnight.checked_add(delta) {
                    if let Ok(zoned) = adj.to_zoned(tz.clone()) {
                        return Ok(zoned.timestamp().as_microsecond());
                    }
                } else {
                    break;
                }
            }

            // The timezone database might not have explicit rules for extremely
            // old/new dates, so fall back to the legacy behavior that applies the
            // canonical offset we use for 1970-01-01.
            let tz_offset_micros = tz
                .to_timestamp(date(1970, 1, 1).at(0, 0, 0, 0))
                .unwrap()
                .as_microsecond();
            Ok(ts + tz_offset_micros)
        }
    }
}

pub trait ToNumber<N> {
    fn to_number(dt: &Zoned) -> N;

    fn from_components(_components: &DateTimeComponents) -> Option<N> {
        None
    }
}

pub struct ToNumberImpl;

impl ToNumberImpl {
    pub fn eval_timestamp<T: ToNumber<R>, R>(us: i64, tz: &TimeZone) -> R {
        if let Some(components) = fast_components_from_timestamp(us, tz) {
            if let Some(value) = T::from_components(&components) {
                return value;
            }
        }
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<R>, R>(date: i32, tz: &TimeZone) -> Result<R> {
        let dt = date
            .to_date(tz)
            .at(0, 0, 0, 0)
            .to_zoned(tz.clone())
            .unwrap();
        Ok(T::to_number(&dt))
    }
}

pub struct ToYYYYMM;
pub struct ToYYYYWW;
pub struct ToYYYYMMDD;
pub struct ToYYYYMMDDHH;
pub struct ToYYYYMMDDHHMMSS;
pub struct ToYear;
pub struct ToTimezoneHour;
pub struct ToTimezoneMinute;
pub struct ToMillennium;
pub struct ToISOYear;
pub struct ToQuarter;
pub struct ToMonth;
pub struct ToDayOfYear;
pub struct ToDayOfMonth;
pub struct ToDayOfWeek;
pub struct DayOfWeek;
pub struct ToHour;
pub struct ToMinute;
pub struct ToSecond;
pub struct ToUnixTimestamp;

pub struct ToWeekOfYear;

impl ToNumber<u32> for ToYYYYMM {
    fn to_number(dt: &Zoned) -> u32 {
        dt.year() as u32 * 100 + dt.month() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Option<u32> {
        Some(components.year as u32 * 100 + components.month as u32)
    }
}

impl ToNumber<u16> for ToMillennium {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16 / 1000 + 1
    }

    fn from_components(components: &DateTimeComponents) -> Option<u16> {
        Some(components.year as u16 / 1000 + 1)
    }
}

impl ToNumber<u32> for ToWeekOfYear {
    fn to_number(dt: &Zoned) -> u32 {
        dt.date().iso_week_date().week() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Option<u32> {
        Some(components.iso_year_week().1)
    }
}

impl ToNumber<u32> for ToYYYYMMDD {
    fn to_number(dt: &Zoned) -> u32 {
        dt.year() as u32 * 10_000 + dt.month() as u32 * 100 + dt.day() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Option<u32> {
        Some(
            components.year as u32 * 10_000 + components.month as u32 * 100 + components.day as u32,
        )
    }
}

impl ToNumber<u64> for ToYYYYMMDDHH {
    fn to_number(dt: &Zoned) -> u64 {
        dt.year() as u64 * 1_000_000
            + dt.month() as u64 * 10_000
            + dt.day() as u64 * 100
            + dt.hour() as u64
    }

    fn from_components(components: &DateTimeComponents) -> Option<u64> {
        Some(
            components.year as u64 * 1_000_000
                + components.month as u64 * 10_000
                + components.day as u64 * 100
                + components.hour as u64,
        )
    }
}

impl ToNumber<u64> for ToYYYYMMDDHHMMSS {
    fn to_number(dt: &Zoned) -> u64 {
        dt.year() as u64 * 10_000_000_000
            + dt.month() as u64 * 100_000_000
            + dt.day() as u64 * 1_000_000
            + dt.hour() as u64 * 10_000
            + dt.minute() as u64 * 100
            + dt.second() as u64
    }

    fn from_components(components: &DateTimeComponents) -> Option<u64> {
        Some(
            components.year as u64 * 10_000_000_000
                + components.month as u64 * 100_000_000
                + components.day as u64 * 1_000_000
                + components.hour as u64 * 10_000
                + components.minute as u64 * 100
                + components.second as u64,
        )
    }
}

impl ToNumber<u16> for ToYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16
    }

    fn from_components(components: &DateTimeComponents) -> Option<u16> {
        Some(components.year as u16)
    }
}

impl ToNumber<i16> for ToTimezoneHour {
    fn to_number(dt: &Zoned) -> i16 {
        dt.offset().seconds().div_ceil(3600) as i16
    }

    fn from_components(components: &DateTimeComponents) -> Option<i16> {
        Some(components.offset_seconds.div_ceil(3600) as i16)
    }
}

impl ToNumber<i16> for ToTimezoneMinute {
    fn to_number(dt: &Zoned) -> i16 {
        (dt.offset().seconds() % 3600).div_ceil(60) as i16
    }

    fn from_components(components: &DateTimeComponents) -> Option<i16> {
        Some((components.offset_seconds % 3600).div_ceil(60) as i16)
    }
}

impl ToNumber<u16> for ToISOYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.date().iso_week_date().year() as _
    }

    fn from_components(components: &DateTimeComponents) -> Option<u16> {
        Some(components.iso_year_week().0 as u16)
    }
}

impl ToNumber<u32> for ToYYYYWW {
    fn to_number(dt: &Zoned) -> u32 {
        let week_date = dt.date().iso_week_date();
        let year = week_date.year() as u32 * 100;
        year + dt.date().iso_week_date().week() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Option<u32> {
        let (iso_year, iso_week) = components.iso_year_week();
        Some(iso_year as u32 * 100 + iso_week)
    }
}

impl ToNumber<u8> for ToQuarter {
    fn to_number(dt: &Zoned) -> u8 {
        // begin with 0
        ((dt.month() - 1) / 3 + 1) as u8
    }

    fn from_components(components: &DateTimeComponents) -> Option<u8> {
        Some((components.month - 1) / 3 + 1)
    }
}

impl ToNumber<u8> for ToMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.month() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Option<u8> {
        Some(components.month)
    }
}

impl ToNumber<u16> for ToDayOfYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.day_of_year() as u16
    }

    fn from_components(components: &DateTimeComponents) -> Option<u16> {
        Some(components.day_of_year)
    }
}

impl ToNumber<u8> for ToDayOfMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.day() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Option<u8> {
        Some(components.day)
    }
}

impl ToNumber<u8> for ToDayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_monday_one_offset() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Option<u8> {
        Some(components.weekday.to_monday_one_offset() as u8)
    }
}

impl ToNumber<u8> for DayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_sunday_zero_offset() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Option<u8> {
        Some(components.weekday.to_sunday_zero_offset() as u8)
    }
}

impl ToNumber<i64> for ToUnixTimestamp {
    fn to_number(dt: &Zoned) -> i64 {
        dt.with_time_zone(TimeZone::UTC).timestamp().as_second()
    }

    fn from_components(components: &DateTimeComponents) -> Option<i64> {
        Some(components.unix_seconds)
    }
}

#[derive(Clone, Copy)]
pub enum Round {
    Second,
    Minute,
    FiveMinutes,
    TenMinutes,
    FifteenMinutes,
    TimeSlot,
    Hour,
    Day,
}

pub fn round_timestamp(ts: i64, tz: &TimeZone, round: Round) -> i64 {
    let dtz = ts.to_timestamp(tz);
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

#[derive(Debug, Clone, Copy)]
pub enum TimePart {
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
    pub fn from(s: &str) -> Self {
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

    pub fn date_part(&self) -> bool {
        matches!(
            self,
            Self::Year | Self::Quarter | Self::Month | Self::Week | Self::Day
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StartOrEnd {
    Start,
    End,
    None,
}

impl StartOrEnd {
    pub fn from(s: &str) -> Self {
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

pub fn time_slice_timestamp(
    ts: i64,
    slice_length: u64,
    part: TimePart,
    start_or_end: StartOrEnd,
    week_start: Weekday,
    tz: &TimeZone,
) -> i64 {
    let slice_length = slice_length as i64;

    let ts = ts.to_timestamp(tz);
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

pub fn time_slice_date(
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

pub struct DateRounder;

impl DateRounder {
    pub fn eval_timestamp<T: ToNumber<i32>>(us: i64, tz: &TimeZone) -> i32 {
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<i32>>(date: i32, tz: &TimeZone) -> Result<i32> {
        let naive_dt = date
            .to_date(tz)
            .at(0, 0, 0, 0)
            .to_zoned(tz.clone())
            .unwrap();
        Ok(T::to_number(&naive_dt))
    }
}

/// Convert `jiff::Zoned` to `i32` in `Scalar::Date(i32)` for `DateType`.
///
/// It's the days since 1970-01-01.
#[inline]
fn datetime_to_date_inner_number(date: &Zoned) -> i32 {
    date.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

pub struct ToLastMonday;
pub struct ToLastSunday;
pub struct ToStartOfMonth;
pub struct ToStartOfQuarter;
pub struct ToStartOfYear;
pub struct ToStartOfISOYear;

pub struct ToLastOfYear;
pub struct ToLastOfWeek;
pub struct ToLastOfMonth;
pub struct ToLastOfQuarter;
pub struct ToPreviousMonday;
pub struct ToPreviousTuesday;
pub struct ToPreviousWednesday;
pub struct ToPreviousThursday;
pub struct ToPreviousFriday;
pub struct ToPreviousSaturday;
pub struct ToPreviousSunday;
pub struct ToNextMonday;
pub struct ToNextTuesday;
pub struct ToNextWednesday;
pub struct ToNextThursday;
pub struct ToNextFriday;
pub struct ToNextSaturday;
pub struct ToNextSunday;

impl ToNumber<i32> for ToLastMonday {
    fn to_number(dt: &Zoned) -> i32 {
        // datetime_to_date_inner_number just calc naive_date, so weekday also need only calc naive_date
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_monday_zero_offset() as i32
    }
}

impl ToNumber<i32> for ToLastSunday {
    fn to_number(dt: &Zoned) -> i32 {
        // datetime_to_date_inner_number just calc naive_date, so weekday also need only calc naive_date
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_sunday_zero_offset() as i32
    }
}

impl ToNumber<i32> for ToStartOfMonth {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(&dt.first_of_month().unwrap())
    }
}

impl ToNumber<i32> for ToStartOfQuarter {
    fn to_number(dt: &Zoned) -> i32 {
        let new_month = (dt.month() - 1) / 3 * 3 + 1;
        let new_day = date(dt.year(), new_month, 1)
            .at(0, 0, 0, 0)
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&new_day)
    }
}

impl ToNumber<i32> for ToStartOfYear {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(&dt.first_of_year().unwrap())
    }
}

impl ToNumber<i32> for ToStartOfISOYear {
    fn to_number(dt: &Zoned) -> i32 {
        let iso_year = dt.date().iso_week_date().year();
        for i in 1..=7 {
            let new_dt = date(iso_year, 1, i)
                .at(0, 0, 0, 0)
                .to_zoned(dt.time_zone().clone())
                .unwrap();
            if new_dt.date().iso_week_date().weekday() == Weekday::Monday {
                return datetime_to_date_inner_number(&new_dt);
            }
        }
        // Never return 0
        0
    }
}

impl ToNumber<i32> for ToLastOfWeek {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_monday_zero_offset() as i32 + 6
    }
}

impl ToNumber<i32> for ToLastOfMonth {
    fn to_number(dt: &Zoned) -> i32 {
        let day = last_day_of_year_month(dt.year(), dt.month());
        let dt = date(dt.year(), dt.month(), day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToLastOfQuarter {
    fn to_number(dt: &Zoned) -> i32 {
        let new_month = (dt.month() - 1) / 3 * 3 + 3;
        let day = last_day_of_year_month(dt.year(), new_month);
        let dt = date(dt.year(), new_month, day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToLastOfYear {
    fn to_number(dt: &Zoned) -> i32 {
        let day = last_day_of_year_month(dt.year(), 12);
        let dt = date(dt.year(), 12, day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToPreviousMonday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Monday, true)
    }
}

impl ToNumber<i32> for ToPreviousTuesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Tuesday, true)
    }
}

impl ToNumber<i32> for ToPreviousWednesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Wednesday, true)
    }
}

impl ToNumber<i32> for ToPreviousThursday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Thursday, true)
    }
}

impl ToNumber<i32> for ToPreviousFriday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Friday, true)
    }
}

impl ToNumber<i32> for ToPreviousSaturday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Saturday, true)
    }
}

impl ToNumber<i32> for ToPreviousSunday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Sunday, true)
    }
}

impl ToNumber<i32> for ToNextMonday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Monday, false)
    }
}

impl ToNumber<i32> for ToNextTuesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Tuesday, false)
    }
}

impl ToNumber<i32> for ToNextWednesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Wednesday, false)
    }
}

impl ToNumber<i32> for ToNextThursday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Thursday, false)
    }
}

impl ToNumber<i32> for ToNextFriday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Friday, false)
    }
}

impl ToNumber<i32> for ToNextSaturday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Saturday, false)
    }
}

impl ToNumber<i32> for ToNextSunday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Sunday, false)
    }
}

pub fn previous_or_next_day(dt: &Zoned, target: Weekday, is_previous: bool) -> i32 {
    let dir = if is_previous { -1 } else { 1 };
    let mut days_diff = (dir
        * (target.to_monday_zero_offset() as i32
            - dt.date().weekday().to_monday_zero_offset() as i32)
        + 7)
        % 7;

    days_diff = if days_diff == 0 { 7 } else { days_diff };

    datetime_to_date_inner_number(dt) + dir * days_diff
}

/// PostgreSQL to strftime format specifier mappings
///
/// The vector contains tuples of (postgres_format, strftime_format):
/// - For case-insensitive PostgreSQL formats (e.g., "YYYY"), any case variation will match
/// - For case-sensitive strftime formats (prefixed with '%'), exact case matching is required
///
/// Note: The sort order (by descending key length) is critical for correct pattern matching
static PG_STRFTIME_MAPPINGS: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    let mut mappings = vec![
        // ==============================================
        // Case-insensitive PostgreSQL format specifiers
        // (will match regardless of letter case)
        // ==============================================
        // Date components
        ("YYYY", "%Y"), // 4-digit year
        ("YY", "%y"),   // 2-digit year
        ("MMMM", "%B"), // Full month name
        ("MON", "%b"),  // Abbreviated month name (special word boundary handling)
        ("MM", "%m"),   // Month number (01-12)
        ("DD", "%d"),   // Day of month (01-31)
        ("DY", "%a"),   // Abbreviated weekday name
        // Time components
        ("HH24", "%H"), // 24-hour format (00-23)
        ("HH12", "%I"), // 12-hour format (01-12)
        ("AM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("PM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("MI", "%M"),   // Minutes (00-59)
        ("SS", "%S"),   // Seconds (00-59)
        ("FF", "%f"),   // Fractional seconds
        // Special cases
        ("UUUU", "%G"),    // ISO week-numbering year
        ("TZHTZM", "%z"),  // Timezone as ±HHMM
        ("TZH:TZM", "%z"), // Timezone as ±HH:MM
        ("TZH", "%:::z"),  // Timezone hour only
        // ==============================================
        // Case-sensitive strftime format specifiers
        // (must match exactly including case)
        // ==============================================
        ("%Y", "%Y"), // Year aliases
        ("%y", "%y"),
        ("%B", "%B"), // Month aliases
        ("%b", "%b"),
        ("%m", "%m"),
        ("%d", "%d"), // Day aliases
        ("%a", "%a"), // Weekday alias
        ("%H", "%H"), // Hour aliases
        ("%I", "%I"),
        ("%p", "%p"),       // AM/PM indicator
        ("%M", "%M"),       // Minute alias
        ("%S", "%S"),       // Second alias
        ("%f", "%f"),       // Fractional second alias
        ("%G", "%G"),       // ISO year alias
        ("%z", "%z"),       // Timezone aliases
        ("%:::z", "%:::z"), // Timezone hour alias
    ];

    // Critical: Sort by descending key length to ensure longest possible matches are found first
    // This prevents shorter patterns from incorrectly matching parts of longer patterns
    mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    mappings
});

static PG_KEY_LENGTHS: LazyLock<Vec<usize>> =
    LazyLock::new(|| PG_STRFTIME_MAPPINGS.iter().map(|(k, _)| k.len()).collect());

fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    text.chars()
        .zip(prefix.chars())
        .all(|(c1, c2)| c1.to_lowercase().eq(c2.to_lowercase()))
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[inline]
pub fn pg_format_to_strftime(pg_format_string: &str) -> String {
    let mut result = String::with_capacity(pg_format_string.len() + 16);
    let mut current_byte_idx = 0;
    let format_len = pg_format_string.len();

    while current_byte_idx < format_len {
        let remaining_slice = &pg_format_string[current_byte_idx..];
        let mut matched = false;
        let first_char = remaining_slice.chars().next().unwrap_or('\0');

        for ((key, value), &key_len) in PG_STRFTIME_MAPPINGS.iter().zip(PG_KEY_LENGTHS.iter()) {
            if !key.is_empty() && !first_char.eq_ignore_ascii_case(&key.chars().next().unwrap()) {
                continue;
            }

            let is_case_sensitive_key = key.starts_with('%');
            let is_current_match = if is_case_sensitive_key {
                remaining_slice.starts_with(key)
            } else {
                starts_with_ignore_case(remaining_slice, key)
            };

            if is_current_match {
                let mut is_valid_match = true;
                if !is_case_sensitive_key && key.eq_ignore_ascii_case("MON") {
                    let next_byte_idx = current_byte_idx + key_len;

                    if current_byte_idx > 0 {
                        if let Some(prev_char) =
                            pg_format_string[..current_byte_idx].chars().next_back()
                        {
                            if is_word_char(prev_char) {
                                is_valid_match = false;
                            }
                        }
                    }

                    if is_valid_match && next_byte_idx < format_len {
                        if let Some(next_char) = pg_format_string[next_byte_idx..].chars().next() {
                            if is_word_char(next_char) {
                                is_valid_match = false;
                            }
                        }
                    }
                }

                if is_valid_match {
                    result.push_str(value);
                    current_byte_idx += key_len;
                    matched = true;
                    break;
                }
            }
        }

        if !matched {
            let c = first_char;
            result.push(c);
            current_byte_idx += c.len_utf8();
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;

    fn date_to_value(year: i32, month: u8, day: u8) -> i32 {
        let target = date(year.try_into().unwrap(), month as i8, day as i8);
        target
            .since((Unit::Day, date(1970, 1, 1)))
            .unwrap()
            .get_days()
    }

    #[test]
    fn test_calc_date_to_timestamp_handles_dst_gap() {
        let tz = TimeZone::get("Asia/Shanghai").unwrap();
        let val = date_to_value(1947, 4, 15);
        let micros = calc_date_to_timestamp(val, &tz).unwrap();
        let zoned = Timestamp::from_microsecond(micros)
            .unwrap()
            .to_zoned(tz.clone());
        assert_eq!(
            zoned.to_string(),
            "1947-04-15T01:00:00+09:00[Asia/Shanghai]"
        );
    }

    #[test]
    fn test_calc_date_to_timestamp_regular_midnight() {
        let tz = TimeZone::get("Asia/Shanghai").unwrap();
        let val = date_to_value(1947, 4, 16);
        let micros = calc_date_to_timestamp(val, &tz).unwrap();
        let zoned = Timestamp::from_microsecond(micros)
            .unwrap()
            .to_zoned(tz.clone());
        assert_eq!(
            zoned.to_string(),
            "1947-04-16T00:00:00+09:00[Asia/Shanghai]"
        );
    }
}
