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
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::Timelike;
use chrono::Weekday;

#[derive(Debug, Clone)]
pub struct DateTimeComponents {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro: u32,
    pub weekday: Weekday,
    pub days_in_month: u8,
    pub day_of_year: u16,
    /// UTC offset in seconds that is active at `unix_seconds`.
    pub offset_seconds: i32,
    /// The instant these components were derived from, in whole seconds.
    pub unix_seconds: i64,
}

impl DateTimeComponents {
    pub(crate) fn from_naive(
        local: &NaiveDateTime,
        offset_seconds: i32,
        unix_seconds: i64,
        micro: u32,
    ) -> Self {
        let date = local.date();
        let year = date.year();
        let month = date.month() as u8;

        Self {
            year,
            month,
            day: date.day() as u8,
            hour: local.hour() as u8,
            minute: local.minute() as u8,
            second: local.second() as u8,
            micro,
            weekday: date.weekday(),
            days_in_month: last_day_of_month(year, month),
            day_of_year: date.ordinal() as u16,
            offset_seconds,
            unix_seconds,
        }
    }

    /// Weekday numbered `1..=7` with Monday as `1`.
    pub fn weekday_from_monday_one(&self) -> u8 {
        self.weekday.number_from_monday() as u8
    }

    /// Weekday numbered `0..=6` with Sunday as `0`.
    pub fn weekday_from_sunday_zero(&self) -> u8 {
        self.weekday.num_days_from_sunday() as u8
    }

    /// ISO 8601 week-numbering year and week.
    ///
    /// The ISO year can differ from the calendar year around New Year: for
    /// example 2019-12-30 belongs to ISO year 2020, week 1.
    pub fn iso_year_week(&self) -> (i32, u32) {
        let day = self.day_of_year as i32;
        let weekday = self.weekday_from_monday_one() as i32;
        let mut week = (day - weekday + 10).div_euclid(7);
        let mut year = self.year;

        if week < 1 {
            year -= 1;
            week = weeks_in_year(year) as i32;
        } else {
            let weeks_current = weeks_in_year(year) as i32;
            if week > weeks_current {
                year += 1;
                week = 1;
            }
        }

        (year, week as u32)
    }
}

/// Number of days from `0001-01-01` to `year-01-01`, exclusive.
pub(crate) fn days_before_year(year: i32) -> i64 {
    let y = (year - 1) as i64;
    365 * y + y / 4 - y / 100 + y / 400
}

pub(crate) fn days_between(start_year: i32, end_year: i32) -> usize {
    (days_before_year(end_year) - days_before_year(start_year)) as usize
}

const CUMULATIVE_DAYS: [u16; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

pub(crate) fn day_of_year(year: i32, month: u8, day: u8) -> u16 {
    let mut ordinal = CUMULATIVE_DAYS[(month - 1) as usize] + day as u16;
    if month > 2 && is_leap_year(year) {
        ordinal += 1;
    }
    ordinal
}

pub(crate) fn last_day_of_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => unreachable!("invalid month: {month}"),
    }
}

pub(crate) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn weeks_in_year(year: i32) -> u32 {
    let Some(first_day) = NaiveDate::from_ymd_opt(year, 1, 1) else {
        return 52;
    };
    match first_day.weekday() {
        Weekday::Thu => 53,
        Weekday::Wed if is_leap_year(year) => 53,
        _ => 52,
    }
}

/// Build a `NaiveDateTime` from calendar fields, rejecting invalid input.
pub(crate) fn naive_from_parts(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
) -> Option<NaiveDateTime> {
    if micro >= 1_000_000 {
        return None;
    }
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)?.and_hms_micro_opt(
        hour as u32,
        minute as u32,
        second as u32,
        micro,
    )
}
