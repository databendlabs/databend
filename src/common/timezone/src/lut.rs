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

//! Per-day timezone lookup cache.
//!
//! Ambiguous and missing local times always use the shared resolver.

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;

use chrono::Datelike;
use chrono::NaiveDate;
use chrono::Weekday;
use chrono_tz::Tz;

use crate::LUT_MAX_YEAR;
use crate::LUT_MIN_YEAR;
use crate::MICROS_PER_SEC;
use crate::SECONDS_PER_DAY;
use crate::civil::DateTimeComponents;
use crate::civil::day_of_year;
use crate::civil::days_before_year;
use crate::civil::days_between;
use crate::civil::last_day_of_month;
use crate::civil::naive_from_parts;
use crate::local_datetime_at;
use crate::offset_seconds_at;
use crate::resolve::LocalTimeResolution;
use crate::resolve::resolve_local_datetime;

#[derive(Clone)]
struct DayEntry {
    // `Later` resolution keeps cached day boundaries monotonic at midnight.
    start_utc: i64,
    // Wall-clock arithmetic is anchored independently of `start_utc`.
    day_epoch_local: i64,
    year: i32,
    month: u8,
    day: u8,
    weekday: Weekday,
    days_in_month: u8,
    day_of_year: u16,
    offset_at_start: i32,
    midnight_is_unique: bool,
    transition_utc: Option<i64>,
    offset_change: i32,
    reliable: bool,
}

const PROBE_STEP_SECONDS: i64 = 8 * 3600;

impl DayEntry {
    fn new(date: NaiveDate, tz: &Tz) -> Self {
        let midnight = date.and_hms_opt(0, 0, 0).expect("midnight is always valid");

        // Midnight folds and gaps must bypass the unique-local-time fast path.
        let unique_midnight =
            resolve_local_datetime(tz, midnight, LocalTimeResolution::Reject, None);
        let midnight_is_unique = unique_midnight.is_some();

        let resolved = match unique_midnight {
            Some(resolved) => resolved,
            None => resolve_local_datetime(tz, midnight, LocalTimeResolution::Later, None)
                .expect("timezone lut: resolve local midnight"),
        };

        let start_utc = resolved.unix_seconds;
        let offset_at_start = resolved.offset_seconds;

        let year = date.year();
        let month = date.month() as u8;
        let day = date.day() as u8;
        let day_of_year = day_of_year(year, month, day);
        let day_epoch_local =
            (days_before_year(year) + day_of_year as i64 - 1 - days_before_year(1970))
                * SECONDS_PER_DAY;

        let (transition_utc, offset_change, reliable) =
            detect_transition(tz, start_utc, offset_at_start);

        Self {
            start_utc,
            day_epoch_local,
            year,
            month,
            day,
            weekday: date.weekday(),
            days_in_month: last_day_of_month(year, month),
            day_of_year,
            offset_at_start,
            midnight_is_unique,
            transition_utc,
            offset_change,
            reliable,
        }
    }

    fn contains(&self, seconds: i64, next: &DayEntry) -> bool {
        seconds >= self.start_utc && seconds < next.start_utc
    }

    fn offset_at(&self, seconds: i64) -> i32 {
        match self.transition_utc {
            Some(transition) if seconds >= transition => self.offset_at_start + self.offset_change,
            _ => self.offset_at_start,
        }
    }

    fn build_components(&self, seconds: i64, micros: u32) -> Option<DateTimeComponents> {
        if !self.reliable {
            return None;
        }

        let offset = self.offset_at(seconds);
        let local_elapsed = seconds + offset as i64 - self.day_epoch_local;
        if !(0..SECONDS_PER_DAY).contains(&local_elapsed) {
            return None;
        }

        Some(DateTimeComponents {
            year: self.year,
            month: self.month,
            day: self.day,
            hour: (local_elapsed / 3600) as u8,
            minute: ((local_elapsed % 3600) / 60) as u8,
            second: (local_elapsed % 60) as u8,
            micro: micros,
            weekday: self.weekday,
            days_in_month: self.days_in_month,
            day_of_year: self.day_of_year,
            offset_seconds: offset,
            unix_seconds: seconds,
        })
    }

    fn unique_instant(&self, local_seconds: i64) -> Option<i64> {
        if !self.reliable || self.transition_utc.is_some() || !self.midnight_is_unique {
            return None;
        }
        Some(self.day_epoch_local + local_seconds - self.offset_at_start as i64)
    }
}

fn detect_transition(tz: &Tz, start_utc: i64, offset_at_start: i32) -> (Option<i64>, i32, bool) {
    let end_utc = start_utc + SECONDS_PER_DAY;

    let mut changed_window = None;
    let mut previous_offset = offset_at_start;
    let mut probe = start_utc;

    while probe < end_utc {
        let next_probe = (probe + PROBE_STEP_SECONDS).min(end_utc);
        let next_offset =
            offset_seconds_at(tz, next_probe).expect("timezone offset is representable");

        if next_offset != previous_offset {
            if changed_window.is_some() {
                // More than one change in a single day: refuse to cache it.
                return (None, 0, false);
            }
            changed_window = Some((probe, next_probe, previous_offset, next_offset));
        }

        previous_offset = next_offset;
        probe = next_probe;
    }

    match changed_window {
        None => (None, 0, true),
        Some((low, high, before, after)) => {
            let boundary = find_transition(tz, low, high, before);
            (Some(boundary), after - before, true)
        }
    }
}

fn find_transition(tz: &Tz, start: i64, end: i64, offset_at_start: i32) -> i64 {
    let mut low = start;
    let mut high = end;

    while low < high {
        let mid = low + (high - low) / 2;
        if offset_seconds_at(tz, mid).expect("timezone offset is representable") == offset_at_start
        {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

// Scan continuously: per-day windows can miss transitions at midnight folds.
fn collect_transitions(tz: &Tz, start: i64, end: i64) -> Vec<i64> {
    let mut transitions = Vec::new();
    let mut probe = start;
    let mut offset = offset_seconds_at(tz, probe).expect("timezone offset is representable");

    while probe < end {
        let next = (probe + PROBE_STEP_SECONDS).min(end);
        let next_offset = offset_seconds_at(tz, next).expect("timezone offset is representable");
        if next_offset != offset {
            transitions.push(find_transition(tz, probe, next, offset));
        }
        probe = next;
        offset = next_offset;
    }
    transitions
}

struct TimeZoneLut {
    daynum_offset: i64,
    entries: Vec<DayEntry>,
    transitions: Vec<i64>,
}

impl TimeZoneLut {
    fn new(tz: &Tz) -> Self {
        // One extra day past the end acts as the upper sentinel for lookups.
        let total_days = days_between(LUT_MIN_YEAR, LUT_MAX_YEAR + 1) + 1;
        let mut entries = Vec::with_capacity(total_days);

        let mut date =
            NaiveDate::from_ymd_opt(LUT_MIN_YEAR, 1, 1).expect("lut start date is valid");
        for _ in 0..total_days {
            let entry = DayEntry::new(date, tz);
            entries.push(entry);
            date = date
                .succ_opt()
                .expect("lut date stays inside the chrono range");
        }

        let transitions = collect_transitions(
            tz,
            entries[0].start_utc,
            entries[entries.len() - 1].start_utc,
        );

        Self {
            daynum_offset: days_before_year(1970) - days_before_year(LUT_MIN_YEAR),
            entries,
            transitions,
        }
    }

    fn covered_range(&self) -> (i64, i64) {
        (
            self.entries[0].start_utc,
            self.entries[self.entries.len() - 1].start_utc,
        )
    }

    fn has_transition_in(&self, start: i64, end: i64) -> bool {
        let index = self.transitions.partition_point(|at| *at <= start);
        matches!(self.transitions.get(index), Some(at) if *at <= end)
    }

    fn entry_for_local_date(&self, year: i32, month: u8, day: u8) -> Option<&DayEntry> {
        let index = day_index_for_date(year, month, day)?;
        // The sentinel entry is not a queryable day.
        if index + 1 >= self.entries.len() {
            return None;
        }
        Some(&self.entries[index])
    }

    fn lookup(&self, seconds: i64) -> Option<&DayEntry> {
        // Offsets shift a day by at most a few hours, so the guess is off by no
        // more than one entry.
        let guess = seconds.div_euclid(SECONDS_PER_DAY) + self.daynum_offset;
        if guess < 0 {
            return None;
        }

        let last_index = self.entries.len() - 1;
        let mut index = guess as usize;
        if index >= last_index {
            return None;
        }

        if seconds < self.entries[index].start_utc {
            if index == 0 {
                return None;
            }
            index -= 1;
        } else if !self.entries[index].contains(seconds, &self.entries[index + 1]) {
            index += 1;
            if index >= last_index {
                return None;
            }
        }

        let entry = &self.entries[index];
        debug_assert!(entry.contains(seconds, &self.entries[index + 1]));
        Some(entry)
    }
}

type LutCache = RwLock<Vec<(Tz, Arc<TimeZoneLut>)>>;

static TZ_LUTS: LazyLock<LutCache> = LazyLock::new(|| RwLock::new(Vec::new()));

fn get_or_init_lut(tz: &Tz) -> Arc<TimeZoneLut> {
    {
        let guard = TZ_LUTS.read().unwrap();
        if let Some((_, lut)) = guard.iter().find(|(key, _)| key == tz) {
            return lut.clone();
        }
    }

    let lut = Arc::new(TimeZoneLut::new(tz));

    let mut guard = TZ_LUTS.write().unwrap();
    // Another thread may have inserted the same timezone in the meantime.
    if let Some((_, existing)) = guard.iter().find(|(key, _)| key == tz) {
        return existing.clone();
    }
    guard.push((*tz, lut.clone()));
    lut
}

/// Local calendar components for an instant.
///
/// Uses the lookup table when the instant falls inside the cached year range and
/// `chrono-tz` otherwise; both paths return identical results.
///
/// Infallible for Databend's Date/Timestamp ranges. Callers must validate raw
/// values at SQL type boundaries; an invalid internal value fails explicitly
/// rather than being silently clamped to a different instant.
pub fn components_from_timestamp(micros: i64, tz: &Tz) -> DateTimeComponents {
    let seconds = micros.div_euclid(MICROS_PER_SEC);
    let subsec = micros.rem_euclid(MICROS_PER_SEC) as u32;

    if let Some(entry) = get_or_init_lut(tz).lookup(seconds)
        && let Some(components) = entry.build_components(seconds, subsec)
    {
        return components;
    }

    components_via_chrono(seconds, subsec, tz)
}

fn components_via_chrono(seconds: i64, subsec: u32, tz: &Tz) -> DateTimeComponents {
    let (local, offset) =
        local_datetime_at(tz, seconds).expect("timestamp is outside chrono's representable range");
    DateTimeComponents::from_naive(&local, offset, seconds, subsec)
}

/// UTC microseconds for a local calendar time.
///
/// Folds and gaps are delegated to [`resolve_local_datetime`], which applies
/// [`LocalTimeResolution::Compatible`]: folds take the earlier instant and gaps
/// move forward by the real width of the gap.
pub fn fast_utc_from_local(
    tz: &Tz,
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
) -> Option<i64> {
    if hour >= 24 || minute >= 60 || second >= 60 || micro >= MICROS_PER_SEC as u32 {
        return None;
    }

    let local_seconds = (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;

    let unix_seconds = get_or_init_lut(tz)
        .entry_for_local_date(year, month, day)
        .and_then(|entry| entry.unique_instant(local_seconds));

    let unix_seconds = match unix_seconds {
        Some(seconds) => seconds,
        None => {
            let local = naive_from_parts(year, month, day, hour, minute, second, micro)?;
            resolve_local_datetime(tz, local, LocalTimeResolution::Compatible, None)?.unix_seconds
        }
    };

    let total = (unix_seconds as i128) * (MICROS_PER_SEC as i128) + micro as i128;
    if total > i64::MAX as i128 || total < i64::MIN as i128 {
        return None;
    }
    Some(total as i64)
}

/// Whether the local wall clock runs monotonically across `[start_micros, end_micros]`.
///
/// True when no UTC offset transition happens inside the range, which lets
/// callers evaluate a monotonic function at the two end points only. UTC never
/// transitions; a range reaching outside the cached years is reported as
/// non-monotonic rather than guessed.
pub fn wall_clock_is_monotonic(tz: &Tz, start_micros: i64, end_micros: i64) -> bool {
    if *tz == Tz::UTC {
        return true;
    }

    if start_micros > end_micros {
        return false;
    }

    let start = start_micros.div_euclid(MICROS_PER_SEC);
    let end = end_micros.div_euclid(MICROS_PER_SEC);

    let lut = get_or_init_lut(tz);
    let (covered_start, covered_end) = lut.covered_range();
    if start < covered_start || end > covered_end {
        return false;
    }

    !lut.has_transition_in(start, end)
}

fn day_index_for_date(year: i32, month: u8, day: u8) -> Option<usize> {
    if !(LUT_MIN_YEAR..=LUT_MAX_YEAR).contains(&year) {
        return None;
    }
    if month == 0 || month > 12 {
        return None;
    }
    if day == 0 || day > last_day_of_month(year, month) {
        return None;
    }

    let ordinal = day_of_year(year, month, day) as i64;
    let offset = days_before_year(year) - days_before_year(LUT_MIN_YEAR);
    Some((offset + ordinal - 1) as usize)
}
