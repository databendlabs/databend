// Copyright 2022 Datafuse Labs.
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

use chrono::Date;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use chrono_tz::Tz;
use num_traits::AsPrimitive;

use crate::types::date::check_date;
use crate::types::timestamp::check_timestamp;

pub trait DateConverter {
    fn to_date(&self, tz: Tz) -> Date<Tz>;
    fn to_timestamp(&self, tz: Tz) -> DateTime<Tz>;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, tz: Tz) -> Date<Tz> {
        let mut dt = tz.ymd(1970, 1, 1);
        dt = dt.checked_add_signed(Duration::days(self.as_())).unwrap();
        dt
    }

    fn to_timestamp(&self, tz: Tz) -> DateTime<Tz> {
        // Can't use `tz.timestamp_nanos(self.as_() * 1000)` directly, is may cause multiply with overflow.
        let micros = self.as_();
        let (mut secs, mut nanos) = (micros / 1_000_000, (micros % 1_000_000) * 1_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }
        tz.timestamp_opt(secs, nanos as u32).unwrap()
    }
}

// Timestamp arithmetic factors.
pub const FACTOR_HOUR: i64 = 3600;
pub const FACTOR_MINUTE: i64 = 60;
pub const FACTOR_SECOND: i64 = 1;
const LAST_DAY_LUT: [u8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn add_years_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate, String> {
    let new_year = year + delta as i32;
    let mut new_day = day;
    if std::intrinsics::unlikely(month == 2 && day == 29) {
        new_day = last_day_of_year_month(new_year, month);
    }
    NaiveDate::from_ymd_opt(new_year, month, new_day).ok_or(format!(
        "Overflow on date YMD {}-{}-{}.",
        new_year, month, new_day
    ))
}

fn add_months_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate, String> {
    let total_months = month as i64 + delta - 1;
    let mut new_year = year + (total_months / 12) as i32;
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    let new_day = std::cmp::min::<u32>(
        day,
        last_day_of_year_month(new_year, (new_month0 + 1) as u32),
    );

    NaiveDate::from_ymd_opt(new_year, (new_month0 + 1) as u32, new_day).ok_or(format!(
        "Overflow on date YMD {}-{}-{}.",
        new_year,
        new_month0 + 1,
        new_day
    ))
}

// Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
fn last_day_of_year_month(year: i32, month: u32) -> u32 {
    let is_leap_year = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    LAST_DAY_LUT[month as usize] as u32
}

macro_rules! impl_interval_year_month {
    ($name: ident, $op: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl $name {
            pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> Result<i32, String> {
                let date = date.to_date(Tz::UTC);
                let new_date = $op(date.year(), date.month(), date.day(), delta.as_())?;
                check_date(
                    new_date
                        .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
                        .num_days(),
                )
            }

            pub fn eval_timestamp(ts: i64, delta: impl AsPrimitive<i64>) -> Result<i64, String> {
                let ts = ts.to_timestamp(Tz::UTC);
                let new_ts = $op(ts.year(), ts.month(), ts.day(), delta.as_())?;
                check_timestamp(NaiveDateTime::new(new_ts, ts.time()).timestamp_micros())
            }
        }
    };
}

impl_interval_year_month!(AddYearsImpl, add_years_base);
impl_interval_year_month!(AddMonthsImpl, add_months_base);

pub struct AddDaysImpl;

impl AddDaysImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> Result<i32, String> {
        check_date((date as i64).wrapping_add(delta.as_()))
    }

    pub fn eval_timestamp(date: i64, delta: impl AsPrimitive<i64>) -> Result<i64, String> {
        check_timestamp(date.wrapping_add(delta.as_() * 24 * 3600 * 1_000_000))
    }
}

pub struct AddTimesImpl;

impl AddTimesImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>, factor: i64) -> Result<i32, String> {
        check_date(
            (date as i64 * 3600 * 24 * 1_000_000).wrapping_add(delta.as_() * factor * 1_000_000),
        )
    }

    pub fn eval_timestamp(
        ts: i64,
        delta: impl AsPrimitive<i64>,
        factor: i64,
    ) -> Result<i64, String> {
        check_timestamp(ts.wrapping_add(delta.as_() * factor * 1_000_000))
    }
}

#[inline]
pub fn today_date() -> i32 {
    let now = Utc::now();
    NaiveDate::from_ymd(now.year(), now.month(), now.day())
        .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
        .num_days() as i32
}

pub trait ToNumber<N> {
    fn to_number(dt: &DateTime<Tz>) -> N;
}

pub struct ToNumberImpl;

impl ToNumberImpl {
    pub fn eval_timestamp<T: ToNumber<R>, R>(ts: i64, tz: Tz) -> R {
        let dt = ts.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<R>, R>(date: i32, tz: Tz) -> R {
        let dt = date.to_date(tz).and_hms(0, 0, 0);
        T::to_number(&dt)
    }
}

pub struct ToYYYYMM;
pub struct ToYYYYMMDD;
pub struct ToYYYYMMDDHHMMSS;
pub struct ToYear;
pub struct ToMonth;
pub struct ToDayOfYear;
pub struct ToDayOfMonth;
pub struct ToDayOfWeek;
pub struct ToHour;
pub struct ToMinute;
pub struct ToSecond;

impl ToNumber<u32> for ToYYYYMM {
    fn to_number(dt: &DateTime<Tz>) -> u32 {
        dt.year() as u32 * 100 + dt.month()
    }
}

impl ToNumber<u32> for ToYYYYMMDD {
    fn to_number(dt: &DateTime<Tz>) -> u32 {
        dt.year() as u32 * 10_000 + dt.month() * 100 + dt.day()
    }
}

impl ToNumber<u64> for ToYYYYMMDDHHMMSS {
    fn to_number(dt: &DateTime<Tz>) -> u64 {
        dt.year() as u64 * 10_000_000_000
            + dt.month() as u64 * 100_000_000
            + dt.day() as u64 * 1_000_000
            + dt.hour() as u64 * 10_000
            + dt.minute() as u64 * 100
            + dt.second() as u64
    }
}

impl ToNumber<u16> for ToYear {
    fn to_number(dt: &DateTime<Tz>) -> u16 {
        dt.year() as u16
    }
}

impl ToNumber<u8> for ToMonth {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.month() as u8
    }
}

impl ToNumber<u16> for ToDayOfYear {
    fn to_number(dt: &DateTime<Tz>) -> u16 {
        dt.ordinal() as u16
    }
}

impl ToNumber<u8> for ToDayOfMonth {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.day() as u8
    }
}

impl ToNumber<u8> for ToDayOfWeek {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.weekday().number_from_monday() as u8
    }
}

impl ToNumber<u8> for ToHour {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.hour() as u8
    }
}

impl ToNumber<u8> for ToMinute {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.minute() as u8
    }
}

impl ToNumber<u8> for ToSecond {
    fn to_number(dt: &DateTime<Tz>) -> u8 {
        dt.second() as u8
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

pub fn round_timestamp(ts: i64, tz: Tz, round: Round) -> i64 {
    let dt = tz.timestamp(ts / 1_000_000, 0_u32);
    let res = match round {
        Round::Second => dt,
        Round::Minute => {
            tz.ymd(dt.year(), dt.month(), dt.day())
                .and_hms_micro(dt.hour(), dt.minute(), 0, 0)
        }
        Round::FiveMinutes => tz.ymd(dt.year(), dt.month(), dt.day()).and_hms_micro(
            dt.hour(),
            dt.minute() / 5 * 5,
            0,
            0,
        ),
        Round::TenMinutes => tz.ymd(dt.year(), dt.month(), dt.day()).and_hms_micro(
            dt.hour(),
            dt.minute() / 10 * 10,
            0,
            0,
        ),
        Round::FifteenMinutes => tz.ymd(dt.year(), dt.month(), dt.day()).and_hms_micro(
            dt.hour(),
            dt.minute() / 15 * 15,
            0,
            0,
        ),
        Round::TimeSlot => tz.ymd(dt.year(), dt.month(), dt.day()).and_hms_micro(
            dt.hour(),
            dt.minute() / 30 * 30,
            0,
            0,
        ),
        Round::Hour => tz
            .ymd(dt.year(), dt.month(), dt.day())
            .and_hms_micro(dt.hour(), 0, 0, 0),
        Round::Day => tz
            .ymd(dt.year(), dt.month(), dt.day())
            .and_hms_micro(0, 0, 0, 0),
    };
    res.timestamp_micros()
}

pub struct DateRounder;

impl DateRounder {
    pub fn eval_timestamp<T: ToNumber<i32>>(ts: i64, tz: Tz) -> i32 {
        let dt = ts.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<i32>>(date: i32, tz: Tz) -> i32 {
        let dt = date.to_date(tz).and_hms(0, 0, 0);
        T::to_number(&dt)
    }
}

/// Convert `chrono::DateTime` to `i32` in `Scalar::Date(i32)` for `DateType`.
///
/// It's the days since 1970-01-01.
#[inline]
fn datetime_to_date_inner_number(date: &DateTime<Tz>) -> i32 {
    date.naive_local()
        .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0))
        .num_days() as i32
}

pub struct ToLastMonday;
pub struct ToLastSunday;
pub struct ToStartOfMonth;
pub struct ToStartOfQuarter;
pub struct ToStartOfYear;
pub struct ToStartOfISOYear;

impl ToNumber<i32> for ToLastMonday {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        datetime_to_date_inner_number(dt) - dt.weekday().num_days_from_monday() as i32
    }
}

impl ToNumber<i32> for ToLastSunday {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        datetime_to_date_inner_number(dt) - dt.weekday().num_days_from_sunday() as i32
    }
}

impl ToNumber<i32> for ToStartOfMonth {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        datetime_to_date_inner_number(&dt.with_day(1).unwrap())
    }
}

impl ToNumber<i32> for ToStartOfQuarter {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        let new_month = dt.month0() / 3 * 3 + 1;
        datetime_to_date_inner_number(&dt.with_month(new_month).unwrap().with_day(1).unwrap())
    }
}

impl ToNumber<i32> for ToStartOfYear {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        datetime_to_date_inner_number(&dt.with_month(1).unwrap().with_day(1).unwrap())
    }
}

impl ToNumber<i32> for ToStartOfISOYear {
    fn to_number(dt: &DateTime<Tz>) -> i32 {
        let iso_year = dt.iso_week().year();
        let iso_dt = dt
            .timezone()
            .isoywd(iso_year, 1, chrono::Weekday::Mon)
            .and_hms(0, 0, 0);
        datetime_to_date_inner_number(&iso_dt)
    }
}
