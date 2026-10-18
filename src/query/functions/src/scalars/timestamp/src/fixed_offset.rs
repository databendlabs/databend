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
        // Plain accumulators keep the all-active loop vectorized; min > max
        // afterwards means no row was active.
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        match active.filter(|active| active.null_count() > 0) {
            None => {
                for &ts in col.iter() {
                    min = min.min(ts);
                    max = max.max(ts);
                }
            }
            Some(active) => {
                for (&ts, active) in col.iter().zip(active.iter()) {
                    if active {
                        min = min.min(ts);
                        max = max.max(ts);
                    }
                }
            }
        }
        if min > max {
            return None;
        }
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
