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

//! Calendar values of a timestamp column by integer arithmetic.
//!
//! Resolving each row's local calendar fields through the timezone costs tens
//! of nanoseconds. When the UTC offset cannot change across a column -- always
//! in UTC, and elsewhere whenever the column does not span a DST change -- local
//! time is the timestamp shifted by one offset, and day boundaries, dates and
//! truncations are integer divisions of it.

use chrono_tz::Tz;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_timezone::offset_seconds_at;
use databend_common_timezone::wall_clock_is_monotonic;

pub(crate) const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

/// The one UTC offset of a column's timestamps.
pub(crate) struct FixedOffset {
    pub(crate) min: i64,
    pub(crate) max: i64,
    /// Local time minus UTC, in microseconds.
    pub(crate) micros: i64,
}

impl FixedOffset {
    /// The offset of every active timestamp of `col`, and of every instant up
    /// to `reach` microseconds before them, which a truncation may land on.
    /// `active` is the evaluation's validity (`EvalContext::validity`): the
    /// values under NULLs are arbitrary and their results discarded, so they
    /// must not widen the range. None when the offset may change there, or
    /// when no row is active.
    pub(crate) fn of(
        col: &Buffer<i64>,
        active: Option<&Bitmap>,
        tz: &Tz,
        reach: i64,
    ) -> Option<Self> {
        let extend = |acc: Option<(i64, i64)>, ts: i64| match acc {
            None => Some((ts, ts)),
            Some((min, max)) => Some((i64::min(min, ts), i64::max(max, ts))),
        };
        let (min, max) = match active {
            None => col.iter().fold(None, |acc, &ts| extend(acc, ts)),
            Some(active) => col
                .iter()
                .zip(active.iter())
                .filter(|(_, active)| *active)
                .fold(None, |acc, (&ts, _)| extend(acc, ts)),
        }?;
        if !wall_clock_is_monotonic(tz, min.checked_sub(reach)?, max) {
            return None;
        }
        let micros = offset_seconds_at(tz, min.div_euclid(MICROS_PER_SEC))? as i64 * MICROS_PER_SEC;
        Some(FixedOffset { min, max, micros })
    }

    // Inactive rows may hold any value, so the arithmetic below wraps rather
    // than overflows; for active rows, bounded by min and max, it never wraps.

    /// Days since 1970-01-01 of the local date of `ts`.
    pub(crate) fn local_day(&self, ts: i64) -> i64 {
        ts.wrapping_add(self.micros).div_euclid(MICROS_PER_DAY)
    }

    /// The instant of the local wall-clock time of `ts` truncated to a
    /// multiple of `unit` microseconds; `reach` passed to [`Self::of`] must be
    /// at least `unit`.
    pub(crate) fn truncate(&self, ts: i64, unit: i64) -> i64 {
        let local = ts.wrapping_add(self.micros);
        local
            .wrapping_sub(local.rem_euclid(unit))
            .wrapping_sub(self.micros)
    }
}

/// The local dates of `col` as days since 1970-01-01, when its offset is
/// fixed and every active date is a valid `Date`. Truncation is monotonic, so
/// valid dates at the extremes bound every active row.
pub(crate) fn local_dates(
    col: &Buffer<i64>,
    active: Option<&Bitmap>,
    tz: &Tz,
) -> Option<Buffer<i32>> {
    let offset = FixedOffset::of(col, active, tz, 0)?;
    check_date(offset.local_day(offset.min)).ok()?;
    check_date(offset.local_day(offset.max)).ok()?;
    Some(col.iter().map(|&ts| offset.local_day(ts) as i32).collect())
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::NaiveDate;
    use chrono::NaiveDateTime;
    use chrono::TimeZone;
    use chrono::Timelike;
    use chrono_tz::Tz;
    use databend_common_column::buffer::Buffer;
    use databend_common_expression::types::timestamp::TIMESTAMP_MIN;

    use super::*;

    const ZONES: [Tz; 6] = [
        Tz::UTC,
        Tz::Europe__Paris,
        Tz::Asia__Kathmandu,
        Tz::America__St_Johns,
        Tz::Australia__Sydney,
        Tz::Pacific__Kiritimati,
    ];

    fn column(start: i64, rows: i64, step: i64) -> Buffer<i64> {
        (0..rows).map(|i| start + i * step).collect()
    }

    // 2024-09-01 00:00 UTC, rows every 7 minutes 13.000017 seconds for ~20 days:
    // no zone above changes offset in it.
    fn september() -> Buffer<i64> {
        column(1_725_148_800_000_000, 4000, 433_000_017)
    }

    fn local(ts: i64, tz: &Tz) -> NaiveDateTime {
        DateTime::from_timestamp_micros(ts)
            .unwrap()
            .with_timezone(tz)
            .naive_local()
    }

    #[test]
    fn test_local_dates_match_chrono() {
        let col = september();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        for tz in ZONES {
            let days = local_dates(&col, None, &tz).unwrap();
            for (&ts, &day) in col.iter().zip(days.iter()) {
                let want = (local(ts, &tz).date() - epoch).num_days() as i32;
                assert_eq!(day, want, "{tz} at {ts}");
            }
        }
    }

    // Each truncation is the instant of the local wall-clock time with its
    // finer fields cleared, as chrono's calendar computes it.
    #[test]
    fn test_truncate_matches_chrono() {
        let col = september();
        for (minutes, unit) in [(1u32, 60), (15, 15 * 60), (30, 30 * 60), (60, 3600)] {
            for tz in ZONES {
                let offset = FixedOffset::of(&col, None, &tz, unit * MICROS_PER_SEC).unwrap();
                for &ts in col.iter() {
                    let wall = local(ts, &tz);
                    let total = wall.hour() * 60 + wall.minute();
                    let floor = total / minutes * minutes;
                    let want = wall.date().and_hms_opt(floor / 60, floor % 60, 0).unwrap();
                    let want = tz.from_local_datetime(&want).unwrap().timestamp_micros();
                    let got = offset.truncate(ts, unit * MICROS_PER_SEC);
                    assert_eq!(got, want, "{tz} {minutes}min at {ts}");
                }
            }
        }
        for tz in ZONES {
            let offset = FixedOffset::of(&col, None, &tz, MICROS_PER_DAY).unwrap();
            for &ts in col.iter() {
                let midnight = local(ts, &tz).date().and_hms_opt(0, 0, 0).unwrap();
                let want = tz
                    .from_local_datetime(&midnight)
                    .unwrap()
                    .timestamp_micros();
                assert_eq!(
                    offset.truncate(ts, MICROS_PER_DAY),
                    want,
                    "{tz} day at {ts}"
                );
            }
        }
    }

    // Columns the offset changes in, or whose truncations reach back across
    // a change, are left to the per-row path.
    #[test]
    fn test_offset_changes_decline() {
        let paris = Tz::Europe__Paris;
        // Spans the Paris DST change at 2024-10-27 01:00 UTC.
        let spans = column(1_729_900_800_000_000, 100, 3_600_000_000);
        assert!(FixedOffset::of(&spans, None, &paris, 0).is_none());
        assert!(FixedOffset::of(&spans, None, &Tz::UTC, 0).is_some());

        // 03:00-04:00 UTC that day is after the change, but local midnight
        // was before it.
        let after = column(1_729_998_000_000_000, 60, 60_000_000);
        assert!(FixedOffset::of(&after, None, &paris, 3600 * MICROS_PER_SEC).is_some());
        assert!(FixedOffset::of(&after, None, &paris, MICROS_PER_DAY).is_none());

        // Outside the lookup table's years (1900-2099) no offset is known to be
        // constant, so even a zone without DST takes the per-row path.
        let early = column(TIMESTAMP_MIN, 2, 1);
        assert!(local_dates(&early, None, &Tz::Etc__GMTPlus5).is_none());

        assert!(local_dates(&Buffer::from(vec![]), None, &Tz::UTC).is_none());
    }

    // Values under NULLs neither widen the range nor overflow the arithmetic,
    // and leave the active rows' results unchanged.
    #[test]
    fn test_inactive_rows_are_ignored() {
        let paris = Tz::Europe__Paris;
        let reference = september();
        let mut col: Vec<i64> = reference.iter().copied().collect();
        let mut active = vec![true; col.len()];
        for (i, value) in [(0, 0), (1, i64::MIN), (2, i64::MAX)] {
            col[i] = value;
            active[i] = false;
        }
        let col = Buffer::from(col);
        let active = Bitmap::from(active);

        // With every row counted, 1970 and the extremes span DST changes.
        assert!(local_dates(&col, None, &paris).is_none());

        let days = local_dates(&col, Some(&active), &paris).unwrap();
        let want_days = local_dates(&reference, None, &paris).unwrap();
        let offset = FixedOffset::of(&col, Some(&active), &paris, MICROS_PER_DAY).unwrap();
        let want = FixedOffset::of(&reference, None, &paris, MICROS_PER_DAY).unwrap();
        for i in 0..col.len() {
            let truncated = offset.truncate(col[i], MICROS_PER_DAY);
            if active.get_bit(i) {
                assert_eq!(days[i], want_days[i]);
                assert_eq!(truncated, want.truncate(reference[i], MICROS_PER_DAY));
            }
        }

        let none = Bitmap::new_zeroed(col.len());
        assert!(local_dates(&col, Some(&none), &paris).is_none());
    }
}
