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

use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_timezone::fast_utc_from_local;
use jiff::Timestamp;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::TimeZone;

use crate::serialize::uniform_date;
use crate::types::date::clamp_date;
use crate::types::date::string_to_date;
use crate::types::timestamp::MICROS_PER_MILLI;
use crate::types::timestamp::MICROS_PER_SEC;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::timestamp::clamp_timestamp;
use crate::types::timestamp::string_to_timestamp;
use crate::types::timestamp_tz::string_to_timestamp_tz;

// ---------------------------------------------------------------------------
// AUTO datetime format detection
// ---------------------------------------------------------------------------

const AUTO_DATE_FORMATS: &[&str] = &["%d-%b-%Y", "%m/%d/%Y"];

const AUTO_TS_FORMATS: &[&str] = &[
    // DD-MON-YYYY
    "%d-%b-%Y %H:%M:%S%.f",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y",
    // MM/DD/YYYY
    "%m/%d/%Y %H:%M:%S%.f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    // RFC 2822 (24h)
    "%a, %d %b %Y %H:%M:%S%.f %z",
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S%.f",
    "%a, %d %b %Y %H:%M:%S",
    // RFC 2822 (12h)
    "%a, %d %b %Y %I:%M:%S%.f %p %z",
    "%a, %d %b %Y %I:%M:%S %p %z",
    "%a, %d %b %Y %I:%M:%S%.f %p",
    "%a, %d %b %Y %I:%M:%S %p",
    // Unix date
    "%a %b %d %H:%M:%S %z %Y",
];

/// Check if timestamp is within range, and return the timestamp in micros.
#[inline]
pub fn int64_to_timestamp(mut n: i64) -> i64 {
    if -31536000000 < n && n < 31536000000 {
        n * MICROS_PER_SEC
    } else if -31536000000000 < n && n < 31536000000000 {
        n * MICROS_PER_MILLI
    } else {
        clamp_timestamp(&mut n);
        n
    }
}

/// calc int64 domain to timestamp domain
#[inline]
pub fn calc_int64_to_timestamp_domain(n: i64) -> i64 {
    if -31536000000 < n && n < 31536000000 {
        n * MICROS_PER_SEC
    } else if -31536000000000 < n && n < 31536000000000 {
        n * MICROS_PER_MILLI
    } else {
        n.clamp(TIMESTAMP_MIN, TIMESTAMP_MAX)
    }
}

/// Try to parse a string as an epoch number and convert to microseconds.
/// Reuses the same rules as `int64_to_timestamp` / `to_timestamp(number)`.
pub fn parse_epoch_str(val: &str) -> Option<i64> {
    let n: i64 = val.parse().ok()?;
    Some(int64_to_timestamp(n))
}

/// Core format-matching loop: tries each format, returns `(micros, offset_seconds)`.
fn try_parse_formats(val: &str, tz: &TimeZone, formats: &[&str]) -> Option<(i64, i32)> {
    for fmt in formats {
        let (tm, consumed) = match BrokenDownTime::parse_prefix(fmt, val) {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        if consumed != val.len() {
            continue;
        }
        match tm.offset() {
            Some(_) => {
                let zoned = match tm.to_zoned() {
                    Ok(z) => z,
                    Err(_) => continue,
                };
                return Some((zoned.timestamp().as_microsecond(), zoned.offset().seconds()));
            }
            None => {
                let micros = match fast_timestamp_from_tm(&tm, tz) {
                    Some(m) => m,
                    None => continue,
                };
                let ts = match Timestamp::from_microsecond(micros) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let zoned = ts.to_zoned(tz.clone());
                return Some((micros, zoned.offset().seconds()));
            }
        }
    }
    None
}

pub fn fast_timestamp_from_tm(tm: &BrokenDownTime, tz: &TimeZone) -> Option<i64> {
    let year = i32::from(tm.year()?);
    let month: u8 = tm.month()?.try_into().ok()?;
    let day: u8 = tm.day()?.try_into().ok()?;
    let hour: u8 = tm.hour().unwrap_or(0).try_into().ok()?;
    let minute: u8 = tm.minute().unwrap_or(0).try_into().ok()?;
    let second: u8 = tm.second().unwrap_or(0).try_into().ok()?;
    let nanos = tm.subsec_nanosecond().unwrap_or(0);
    let micro = (nanos / 1_000).max(0) as u32;
    fast_utc_from_local(tz, year, month, day, hour, minute, second, micro)
}

pub fn auto_detect_timestamp(val: &str, tz: &TimeZone) -> Option<i64> {
    let (mut micros, _) = try_parse_formats(val, tz, AUTO_TS_FORMATS)?;
    clamp_timestamp(&mut micros);
    Some(micros)
}

pub fn auto_detect_date(val: &str) -> Option<i32> {
    for fmt in AUTO_DATE_FORMATS {
        let (tm, consumed) = match BrokenDownTime::parse_prefix(fmt, val) {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        if consumed != val.len() {
            continue;
        }
        let dt = match tm.to_datetime() {
            Ok(dt) => dt,
            Err(_) => continue,
        };
        return Some(clamp_date(uniform_date(dt.date()) as i64));
    }
    None
}

pub fn auto_detect_timestamp_tz(val: &str, tz: &TimeZone) -> Option<timestamp_tz> {
    let (mut micros, offset) = try_parse_formats(val, tz, AUTO_TS_FORMATS)?;
    clamp_timestamp(&mut micros);
    Some(timestamp_tz::new(micros, offset))
}

/// Parse a date string with optional auto-detect fallback.
/// Chain: ISO -> numeric-day -> auto (no dtparse).
#[allow(clippy::result_large_err)]
pub fn parse_date_with_auto(val: &str, tz: &TimeZone, enable_auto: bool) -> Result<i32, ErrorCode> {
    match string_to_date(val, tz) {
        Ok(d) => Ok(uniform_date(d)),
        Err(e) => {
            if enable_auto {
                if let Ok(days) = val.parse::<i64>() {
                    return Ok(clamp_date(days));
                }
                if let Some(days) = auto_detect_date(val) {
                    return Ok(days);
                }
            }
            Err(e)
        }
    }
}

/// Parse a timestamp string with optional auto-detect fallback.
/// Chain: ISO -> epoch -> auto (no dtparse).
#[allow(clippy::result_large_err)]
pub fn parse_timestamp_with_auto(
    val: &str,
    tz: &TimeZone,
    enable_auto: bool,
) -> Result<i64, ErrorCode> {
    match string_to_timestamp(val, tz) {
        Ok(ts) => Ok(ts.timestamp().as_microsecond()),
        Err(e) => {
            if enable_auto {
                if let Some(mut micros) = parse_epoch_str(val) {
                    clamp_timestamp(&mut micros);
                    return Ok(micros);
                }
                if let Some(micros) = auto_detect_timestamp(val, tz) {
                    return Ok(micros);
                }
            }
            Err(e)
        }
    }
}

/// Parse a timestamp_tz string with optional auto-detect fallback.
/// Chain: ISO -> epoch -> auto (no dtparse).
#[allow(clippy::result_large_err)]
pub fn parse_timestamp_tz_with_auto(
    val: &str,
    tz: &TimeZone,
    enable_auto: bool,
) -> Result<timestamp_tz, ErrorCode> {
    match string_to_timestamp_tz(val.as_bytes(), || tz) {
        Ok(ts_tz) => Ok(ts_tz),
        Err(e) => {
            if enable_auto {
                if let Some(mut micros) = parse_epoch_str(val) {
                    clamp_timestamp(&mut micros);
                    if let Ok(ts) = Timestamp::from_microsecond(micros) {
                        let offset = tz.to_offset(ts).seconds();
                        return Ok(timestamp_tz::new(micros, offset));
                    }
                }
                if let Some(ts_tz) = auto_detect_timestamp_tz(val, tz) {
                    return Ok(ts_tz);
                }
            }
            Err(e)
        }
    }
}
