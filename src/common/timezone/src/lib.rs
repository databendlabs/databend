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

//! Timezone conversion helpers.
//!
//! Local-to-instant conversion shares one fold/gap policy. Instant-to-local
//! conversion also supports local year 10000 at the timestamp upper bound.

mod civil;
mod lut;
mod resolve;

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Offset;
use chrono::TimeDelta;
use chrono::TimeZone as _;
use chrono::Utc;
pub use chrono_tz::Tz;
pub use civil::DateTimeComponents;
pub use lut::components_from_timestamp;
pub use lut::fast_utc_from_local;
pub use lut::wall_clock_is_monotonic;
pub use resolve::LocalTimeResolution;
pub use resolve::ResolvedLocalTime;
pub use resolve::resolve_local_datetime;

pub fn offset_seconds_at(tz: &Tz, unix_seconds: i64) -> Option<i32> {
    let utc = DateTime::<Utc>::from_timestamp(unix_seconds, 0)?;
    Some(
        tz.offset_from_utc_datetime(&utc.naive_utc())
            .fix()
            .local_minus_utc(),
    )
}

/// Manual offset application keeps local year 10000 representable.
pub fn local_datetime_at(tz: &Tz, unix_seconds: i64) -> Option<(NaiveDateTime, i32)> {
    let utc = DateTime::<Utc>::from_timestamp(unix_seconds, 0)?;
    let offset = offset_seconds_at(tz, unix_seconds)?;
    let local = utc
        .naive_utc()
        .checked_add_signed(TimeDelta::seconds(i64::from(offset)))?;
    Some((local, offset))
}

/// Lowest year covered by the per-day lookup table.
pub const LUT_MIN_YEAR: i32 = 1900;
/// Highest year covered by the per-day lookup table.
///
/// `chrono-tz` expands recurring DST rules into explicit transitions only up to
/// 2099, so the table stops there and later years fall back to `chrono-tz`
/// directly. Keeping both paths on the same data avoids fast/slow path skew.
pub const LUT_MAX_YEAR: i32 = 2099;

pub(crate) const SECONDS_PER_DAY: i64 = 86_400;
pub(crate) const MICROS_PER_SEC: i64 = 1_000_000;
