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

use databend_common_exception::Result;
use jiff::civil::date;
use jiff::civil::datetime;
use jiff::civil::Date;
use jiff::civil::Weekday;
use jiff::tz::TimeZone;
use jiff::SignedDuration;
use jiff::Timestamp;
use jiff::Unit;
use jiff::Zoned;
use num_traits::AsPrimitive;

use crate::types::date::clamp_date;
use crate::types::timestamp::clamp_timestamp;
use crate::types::timestamp::MICROS_PER_SEC;

pub trait DateConverter {
    fn to_date(&self, tz: TimeZone) -> Date;
    fn to_timestamp(&self, tz: TimeZone) -> Zoned;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, _tz: TimeZone) -> Date {
        let dur = SignedDuration::from_hours(self.as_() * 24);
        date(1970, 1, 1).checked_add(dur).unwrap()
    }

    fn to_timestamp(&self, tz: TimeZone) -> Zoned {
        // Can't use `tz.timestamp_nanos(self.as_() * 1000)` directly, is may cause multiply with overflow.
        let micros = self.as_();
        let (mut secs, mut nanos) = (micros / MICROS_PER_SEC, (micros % MICROS_PER_SEC) * 1_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }

        if secs > 253402207200 {
            secs = 253402207200;
            nanos = 0;
        } else if secs < -377705023201 {
            secs = -377705023201;
            nanos = 0;
        }
        let ts = Timestamp::new(secs, nanos as i32).unwrap();
        ts.to_zoned(tz)
    }
}

pub const MICROSECS_PER_DAY: i64 = 86_400_000_000;

// Timestamp arithmetic factors.
pub const FACTOR_HOUR: i64 = 3600;
pub const FACTOR_MINUTE: i64 = 60;
pub const FACTOR_SECOND: i64 = 1;
const LAST_DAY_LUT: [i8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn eval_years_base(year: i16, month: i8, day: i8, delta: i64) -> std::result::Result<Date, String> {
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
) -> std::result::Result<Date, String> {
    let total_months = (month as i64 + delta - 1) as i16;
    let mut new_year = year + (total_months / 12);
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    let new_day = std::cmp::min::<u32>(
        day as u32,
        last_day_of_year_month(new_year, (new_month0 + 1) as i8) as u32,
    );

    match Date::new(new_year, (new_month0 + 1) as i8, new_day as i8) {
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
                tz: TimeZone,
                delta: impl AsPrimitive<i64>,
            ) -> std::result::Result<i32, String> {
                let date = date.to_date(tz);
                let new_date = $op(date.year(), date.month(), date.day(), delta.as_())?;

                Ok(clamp_date(
                    new_date
                        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                        .unwrap()
                        .get_days() as i64,
                ))
            }

            pub fn eval_timestamp(
                us: i64,
                tz: TimeZone,
                delta: impl AsPrimitive<i64>,
            ) -> std::result::Result<i64, String> {
                let ts = us.to_timestamp(tz.clone());
                let new_date = $op(ts.year(), ts.month(), ts.day(), delta.as_())?;

                let mut ts = new_date
                    .at(ts.hour(), ts.minute(), ts.second(), ts.subsec_nanosecond())
                    .to_zoned(tz)
                    .map_err(|e| format!("{}", e))?
                    .timestamp()
                    .as_microsecond();
                clamp_timestamp(&mut ts);
                Ok(ts)
            }
        }
    };
}

impl_interval_year_month!(EvalYearsImpl, eval_years_base);
impl_interval_year_month!(EvalMonthsImpl, eval_months_base);

impl EvalYearsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        date_end.year() as i64 - date_start.year() as i64
    }
}

pub struct EvalISOYearsImpl;
impl EvalISOYearsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        date_end.iso_week_date().year() as i32 - date_start.iso_week_date().year() as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        date_end.date().iso_week_date().year() as i64 - date_start.iso_week_date().year() as i64
    }
}

pub struct EvalYearWeeksImpl;
impl EvalYearWeeksImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        let week_date = date_end.iso_week_date();
        let year = week_date.year() as i32 * 100;
        let end = year + date_end.iso_week_date().week() as i32;

        let week_date = date_start.iso_week_date();
        let year = week_date.year() as i32 * 100;
        let start = year + date_start.iso_week_date().week() as i32;

        end - start
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        let week_date = date_end.date().iso_week_date();
        let year = week_date.year() as i64 * 100;
        let end = year + date_end.date().iso_week_date().week() as i64;

        let week_date = date_start.date().iso_week_date();
        let year = week_date.year() as i64 * 100;
        let start = year + date_start.date().iso_week_date().week() as i64;

        end - start
    }
}

pub struct EvalQuartersImpl;

impl EvalQuartersImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        EvalQuartersImpl::eval_timestamp_diff(
            date_start as i64 * MICROSECS_PER_DAY,
            date_end as i64 * MICROSECS_PER_DAY,
            tz,
        ) as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        (date_end.year() - date_start.year()) as i64 * 4 + ToQuarter::to_number(&date_end) as i64
            - ToQuarter::to_number(&date_start) as i64
    }
}

impl EvalMonthsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32 * 12 + date_end.month() as i32
            - date_start.month() as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        EvalMonthsImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
            tz,
        ) as i64
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
}

#[inline]
pub fn today_date(now: &Zoned, tz: &TimeZone) -> i32 {
    let now = now.with_time_zone(tz.clone());
    now.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

pub trait ToNumber<N> {
    fn to_number(dt: &Zoned) -> N;
}

pub struct ToNumberImpl;

impl ToNumberImpl {
    pub fn eval_timestamp<T: ToNumber<R>, R>(us: i64, tz: TimeZone) -> R {
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<R>, R>(date: i32, tz: TimeZone) -> Result<R> {
        let dt = date
            .to_date(tz.clone())
            .at(0, 0, 0, 0)
            .to_zoned(tz)
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
}

impl ToNumber<u16> for ToMillennium {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16 / 1000 + 1
    }
}

impl ToNumber<u32> for ToWeekOfYear {
    fn to_number(dt: &Zoned) -> u32 {
        dt.date().iso_week_date().week() as u32
    }
}

impl ToNumber<u32> for ToYYYYMMDD {
    fn to_number(dt: &Zoned) -> u32 {
        dt.year() as u32 * 10_000 + dt.month() as u32 * 100 + dt.day() as u32
    }
}

impl ToNumber<u64> for ToYYYYMMDDHH {
    fn to_number(dt: &Zoned) -> u64 {
        dt.year() as u64 * 1_000_000
            + dt.month() as u64 * 10_000
            + dt.day() as u64 * 100
            + dt.hour() as u64
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
}

impl ToNumber<u16> for ToYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16
    }
}

impl ToNumber<i16> for ToTimezoneHour {
    fn to_number(dt: &Zoned) -> i16 {
        dt.offset().seconds().div_ceil(3600) as i16
    }
}

impl ToNumber<i16> for ToTimezoneMinute {
    fn to_number(dt: &Zoned) -> i16 {
        (dt.offset().seconds() % 3600).div_ceil(60) as i16
    }
}

impl ToNumber<u16> for ToISOYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.date().iso_week_date().year() as _
    }
}

impl ToNumber<u32> for ToYYYYWW {
    fn to_number(dt: &Zoned) -> u32 {
        let week_date = dt.date().iso_week_date();
        let year = week_date.year() as u32 * 100;
        year + dt.date().iso_week_date().week() as u32
    }
}

impl ToNumber<u8> for ToQuarter {
    fn to_number(dt: &Zoned) -> u8 {
        // begin with 0
        ((dt.month() - 1) / 3 + 1) as u8
    }
}

impl ToNumber<u8> for ToMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.month() as u8
    }
}

impl ToNumber<u16> for ToDayOfYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.day_of_year() as u16
    }
}

impl ToNumber<u8> for ToDayOfMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.day() as u8
    }
}

impl ToNumber<u8> for ToDayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_monday_one_offset() as u8
    }
}

impl ToNumber<u8> for DayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_sunday_zero_offset() as u8
    }
}

impl ToNumber<i64> for ToUnixTimestamp {
    fn to_number(dt: &Zoned) -> i64 {
        dt.with_time_zone(TimeZone::UTC).timestamp().as_second()
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
    let dtz = ts.to_timestamp(tz.clone());
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

pub struct DateRounder;

impl DateRounder {
    pub fn eval_timestamp<T: ToNumber<i32>>(us: i64, tz: TimeZone) -> i32 {
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<i32>>(date: i32, tz: TimeZone) -> Result<i32> {
        let naive_dt = date
            .to_date(tz.clone())
            .at(0, 0, 0, 0)
            .to_zoned(tz)
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
