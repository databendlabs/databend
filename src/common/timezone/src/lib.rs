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

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;

use jiff::civil::date;
use jiff::civil::Date;
use jiff::civil::Time;
use jiff::civil::Weekday;
use jiff::tz::TimeZone;
use jiff::SignedDuration;

const LUT_MIN_YEAR: i32 = 1900;
const LUT_MAX_YEAR: i32 = 2299;
const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_SEC: i64 = 1_000_000;

#[derive(Clone)]
struct DayEntry {
    /// UTC seconds for the local midnight that starts this day.
    /// When a fold occurs (e.g. `America/New_York` 2024-11-03),
    /// the “later” midnight is chosen so that every instant maps
    /// to exactly one entry.
    start_utc: i64,
    /// Calendar components for the local day.
    year: i32,
    month: u8,
    day: u8,
    weekday: Weekday,
    days_in_month: u8,
    day_of_year: u16,
    /// UTC offset (seconds) that is active at `start_utc`.
    offset_at_start: i32,
    /// When a DST transition occurs inside this day, this stores the UTC
    /// second at which the new offset starts (e.g. `America/Los_Angeles`
    /// jumps ahead at `2024-03-10T10:00:00Z`).
    transition_utc: Option<i64>,
    /// Local seconds since midnight when the transition takes place.
    /// For the 2024-03-10 DST gap in Los Angeles, this is `2*3600`.
    transition_elapsed: Option<i32>,
    /// Offset delta (seconds) introduced by the transition: +3600 for a DST
    /// gap, -3600 for a fold, 0 when no change occurs that day.
    offset_change: i32,
}

impl DayEntry {
    fn new(date: Date, tz: &TimeZone) -> Self {
        let midnight = date.to_datetime(Time::midnight());
        let ambiguous = tz.to_ambiguous_zoned(midnight);
        let needs_later = ambiguous.is_ambiguous();
        let zoned = match (needs_later, ambiguous) {
            (true, ambiguous) => ambiguous
                .later()
                .expect("construct timezone lut: disambiguate midnight via later transition"),
            (false, ambiguous) => ambiguous
                .compatible()
                .expect("construct timezone lut: convert midnight to zoned"),
        };

        let start_ts = zoned.timestamp();
        let start_utc = start_ts.as_second();
        let offset = zoned.offset().seconds();

        let mut transition_utc = None;
        let mut offset_change = 0;
        let mut transition_elapsed = None;

        let follow_start = start_ts
            .saturating_sub(SignedDuration::from_secs(1))
            .unwrap_or(start_ts);

        let mut transitions = tz.following(follow_start);
        for trans in &mut transitions {
            let trans_sec = trans.timestamp().as_second();
            if trans_sec < start_utc {
                continue;
            }
            if trans_sec >= start_utc + SECONDS_PER_DAY {
                break;
            }

            transition_utc = Some(trans_sec);
            transition_elapsed = Some((trans_sec - start_utc) as i32);
            let before_ts = trans
                .timestamp()
                .saturating_sub(SignedDuration::from_secs(1))
                .unwrap_or(trans.timestamp());
            let before_offset = tz.to_offset(before_ts).seconds();
            offset_change = trans.offset().seconds() - before_offset;
            break;
        }

        let year = date.year() as i32;
        let month = date.month() as u8;
        let day = date.day() as u8;
        let day_of_year = day_of_year(year, month, day);

        Self {
            start_utc,
            year,
            month,
            day,
            weekday: date.weekday(),
            days_in_month: last_day_of_month(year, month),
            day_of_year,
            offset_at_start: offset,
            transition_utc,
            transition_elapsed,
            offset_change,
        }
    }

    fn applies_to(&self, seconds: i64, next: &DayEntry) -> bool {
        seconds >= self.start_utc && seconds < next.start_utc
    }

    fn build_components(&self, seconds: i64, micros: u32) -> DateTimeComponents {
        let mut local_seconds = seconds - self.start_utc;
        let mut offset = self.offset_at_start;
        if let (Some(trans), Some(transition_elapsed)) =
            (self.transition_utc, self.transition_elapsed)
        {
            if seconds >= trans {
                if transition_elapsed == 0 {
                    if self.offset_change > 0 {
                        // Gap at the top of the day: skip the missing span but retain
                        // the post-transition offset that `offset_at_start` already has.
                        local_seconds += self.offset_change as i64;
                    }
                } else {
                    local_seconds += self.offset_change as i64;
                    offset += self.offset_change;
                }
            }
        }
        debug_assert!(local_seconds >= 0);

        let hour = (local_seconds / 3600) as u8;
        let minute = ((local_seconds % 3600) / 60) as u8;
        let second = (local_seconds % 60) as u8;

        DateTimeComponents {
            year: self.year,
            month: self.month,
            day: self.day,
            hour,
            minute,
            second,
            micro: micros,
            weekday: self.weekday,
            days_in_month: self.days_in_month,
            day_of_year: self.day_of_year,
            offset_seconds: offset,
            unix_seconds: seconds,
        }
    }

    fn local_elapsed_seconds(&self, local_seconds: i64) -> Option<i64> {
        let mut elapsed = local_seconds;
        if let (Some(_), Some(transition_elapsed)) = (self.transition_utc, self.transition_elapsed)
        {
            let offset_change = self.offset_change as i64;
            if offset_change == 0 {
                return Some(self.start_utc + elapsed);
            }

            if transition_elapsed == 0 && offset_change < 0 {
                // This is a fold that happens exactly at the day boundary. Since the
                // day itself is anchored to the later midnight, treat it as if no
                // transition occurs within the day.
            } else if offset_change > 0 {
                let gap_start = transition_elapsed as i64;
                let gap_end = gap_start + offset_change;
                if local_seconds < gap_start {
                    elapsed = local_seconds;
                } else if local_seconds >= gap_end {
                    elapsed = local_seconds - offset_change;
                } else {
                    return None;
                }
            } else if local_seconds >= transition_elapsed as i64 {
                elapsed = local_seconds - offset_change;
            } else {
                elapsed = local_seconds;
            }
        }
        Some(self.start_utc + elapsed)
    }
}

struct TimeZoneLut {
    daynum_offset: i64,
    entries: Vec<DayEntry>,
}

impl TimeZoneLut {
    fn new(tz: &TimeZone) -> Self {
        let mut entries = Vec::with_capacity(days_between(LUT_MIN_YEAR, LUT_MAX_YEAR + 2));
        let mut date = date(LUT_MIN_YEAR as i16, 1, 1);

        loop {
            entries.push(DayEntry::new(date, tz));
            if date.year() as i32 == LUT_MAX_YEAR + 1 && date.month() == 1 && date.day() == 1 {
                break;
            }
            date = date
                .checked_add(SignedDuration::from_hours(24))
                .expect("construct timezone lut: increment date");
        }

        let daynum_offset = days_before_year(1970) - days_before_year(LUT_MIN_YEAR);

        Self {
            daynum_offset,
            entries,
        }
    }

    fn entry_for_local_date(&self, year: i32, month: u8, day: u8) -> Option<&DayEntry> {
        let index = day_index_for_date(year, month, day)?;
        if index >= self.entries.len() - 1 {
            return None;
        }
        Some(&self.entries[index])
    }

    fn lookup(&self, seconds: i64) -> Option<&DayEntry> {
        let guess = seconds.div_euclid(SECONDS_PER_DAY) + self.daynum_offset;
        if guess < 0 {
            return None;
        }

        let last_index = self.entries.len() - 1;
        if guess as usize >= last_index {
            return None;
        }

        let mut index = guess as usize;

        if seconds < self.entries[index].start_utc {
            if index == 0 {
                return None;
            }
            index -= 1;
        } else if !self.entries[index].applies_to(seconds, &self.entries[index + 1]) {
            index += 1;
            if index >= last_index {
                return None;
            }
        }

        Some(&self.entries[index])
    }
}

type LutCache = RwLock<Vec<(TimeZone, Arc<TimeZoneLut>)>>;

static TZ_LUTS: LazyLock<LutCache> = LazyLock::new(|| RwLock::new(Vec::new()));

fn get_or_init_lut(tz: &TimeZone) -> Arc<TimeZoneLut> {
    {
        let guard = TZ_LUTS.read().unwrap();
        if let Some((_, lut)) = guard.iter().find(|(key, _)| key == tz) {
            return lut.clone();
        }
    }

    let mut guard = TZ_LUTS.write().unwrap();
    if let Some((_, lut)) = guard.iter().find(|(key, _)| key == tz) {
        return lut.clone();
    }

    let lut = Arc::new(TimeZoneLut::new(tz));
    guard.push((tz.clone(), lut.clone()));
    lut
}

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
    pub offset_seconds: i32,
    pub unix_seconds: i64,
}

impl DateTimeComponents {
    pub fn iso_year_week(&self) -> (i32, u32) {
        let day = self.day_of_year as i32;
        let weekday = self.weekday.to_monday_one_offset() as i32;
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

pub fn fast_components_from_timestamp(micros: i64, tz: &TimeZone) -> Option<DateTimeComponents> {
    let seconds = micros.div_euclid(1_000_000);
    let micros = micros.rem_euclid(1_000_000) as u32;
    let lut = get_or_init_lut(tz);
    let entry = lut.lookup(seconds)?;
    Some(entry.build_components(seconds, micros))
}

/// Convert a local calendar time into UTC microseconds using the cached LUT.
/// Returns `None` when the request lies outside the supported year range
/// (1900–2299) or when the local timestamp falls in a DST gap. For DST folds
/// the “later” instant is returned, matching how `fast_components_from_timestamp`
/// anchors each day.
pub fn fast_utc_from_local(
    tz: &TimeZone,
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
) -> Option<i64> {
    if hour >= 24 || minute >= 60 || second >= 60 || micro >= 1_000_000 {
        return None;
    }
    let local_seconds = (hour as i64) * 3600 + (minute as i64) * 60 + (second as i64);
    let lut = get_or_init_lut(tz);
    let entry = lut.entry_for_local_date(year, month, day)?;
    let utc_seconds = entry.local_elapsed_seconds(local_seconds)?;
    let total = (utc_seconds as i128) * (MICROS_PER_SEC as i128) + micro as i128;
    if total > i64::MAX as i128 || total < i64::MIN as i128 {
        return None;
    }
    Some(total as i64)
}

fn days_before_year(year: i32) -> i64 {
    let y = (year - 1) as i64;
    365 * y + y / 4 - y / 100 + y / 400
}

fn days_between(start_year: i32, end_year: i32) -> usize {
    (days_before_year(end_year) - days_before_year(start_year)) as usize
}

const CUMULATIVE_DAYS: [u16; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

fn day_of_year(year: i32, month: u8, day: u8) -> u16 {
    let mut ordinal = CUMULATIVE_DAYS[(month - 1) as usize] + day as u16;
    if month > 2 && is_leap_year(year) {
        ordinal += 1;
    }
    ordinal
}

fn day_index_for_date(year: i32, month: u8, day: u8) -> Option<usize> {
    if !(LUT_MIN_YEAR..=LUT_MAX_YEAR).contains(&year) {
        return None;
    }
    if month == 0 || month > 12 {
        return None;
    }
    let last = last_day_of_month(year, month);
    if day == 0 || day > last {
        return None;
    }
    let ordinal = day_of_year(year, month, day) as i64;
    let days_before = days_before_year(year);
    let offset = days_before - days_before_year(LUT_MIN_YEAR);
    Some((offset + ordinal - 1) as usize)
}

fn last_day_of_month(year: i32, month: u8) -> u8 {
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
        _ => unreachable!("invalid month"),
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn weeks_in_year(year: i32) -> u32 {
    let first_day = date(year as i16, 1, 1);
    match first_day.weekday() {
        Weekday::Thursday => 53,
        Weekday::Wednesday if is_leap_year(year) => 53,
        _ => 52,
    }
}
