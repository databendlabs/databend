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

use chrono::NaiveDate;
use chrono::format::Parsed;
use chrono::format::StrftimeItems;
use chrono::format::parse_and_remainder;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_timezone::Tz;
use databend_common_timezone::fast_utc_from_local;
use databend_common_timezone::offset_seconds_at;

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

const AUTO_DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"];

const AUTO_TS_FORMATS: &[&str] = &[
    // YYYY-MM-DD, including single-digit month/day when auto-detect is enabled.
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
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

pub fn parse_epoch_str(val: &str) -> Option<i64> {
    let n: i64 = val.parse().ok()?;
    Some(int64_to_timestamp(n))
}

/// Parsed fields; omitted clock fields default to midnight and a missing offset
/// uses the session timezone.
pub struct ParsedDateTime {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro: u32,
    pub offset_seconds: Option<i32>,
}

impl ParsedDateTime {
    pub fn parse(format: &str, val: &str) -> Option<Self> {
        let mut parsed = Parsed::new();
        let remainder = parse_and_remainder(&mut parsed, val, StrftimeItems::new(format)).ok()?;
        if !remainder.is_empty() {
            return None;
        }

        let date = parsed.to_naive_date().ok()?;
        let hour = match (parsed.hour_div_12(), parsed.hour_mod_12()) {
            (Some(half), Some(hour)) => half * 12 + hour,
            _ => 0,
        };

        Some(Self {
            year: chrono::Datelike::year(&date),
            month: chrono::Datelike::month(&date) as u8,
            day: chrono::Datelike::day(&date) as u8,
            hour: hour as u8,
            minute: parsed.minute().unwrap_or(0) as u8,
            second: parsed.second().unwrap_or(0) as u8,
            micro: parsed.nanosecond().unwrap_or(0) / 1_000,
            offset_seconds: parsed.offset(),
        })
    }

    fn naive_date(&self) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(self.year, self.month as u32, self.day as u32)
    }
}

fn try_parse_formats(val: &str, tz: &Tz, formats: &[&str]) -> Option<(i64, i32)> {
    for format in formats {
        let Some(parsed) = ParsedDateTime::parse(format, val) else {
            continue;
        };

        match parsed.offset_seconds {
            Some(offset) => {
                let Some(date) = parsed.naive_date() else {
                    continue;
                };
                let Some(local) = date.and_hms_opt(
                    parsed.hour as u32,
                    parsed.minute as u32,
                    parsed.second as u32,
                ) else {
                    continue;
                };
                let micros = local.and_utc().timestamp() * MICROS_PER_SEC + parsed.micro as i64
                    - offset as i64 * MICROS_PER_SEC;
                return Some((micros, offset));
            }
            None => {
                let Some(micros) = fast_timestamp_from_parsed(&parsed, tz) else {
                    continue;
                };
                let offset = offset_seconds_at(tz, micros.div_euclid(MICROS_PER_SEC))?;
                return Some((micros, offset));
            }
        }
    }
    None
}

pub fn fast_timestamp_from_parsed(parsed: &ParsedDateTime, tz: &Tz) -> Option<i64> {
    fast_utc_from_local(
        tz,
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour,
        parsed.minute,
        parsed.second,
        parsed.micro,
    )
}

pub fn auto_detect_timestamp(val: &str, tz: &Tz) -> Option<i64> {
    let (mut micros, _) = try_parse_formats(val, tz, AUTO_TS_FORMATS)?;
    clamp_timestamp(&mut micros);
    Some(micros)
}

pub fn auto_detect_date(val: &str) -> Option<i32> {
    for format in AUTO_DATE_FORMATS {
        let Some(parsed) = ParsedDateTime::parse(format, val) else {
            continue;
        };
        let Some(date) = parsed.naive_date() else {
            continue;
        };
        return Some(clamp_date(crate::serialize::uniform_date(date) as i64));
    }
    None
}

pub fn auto_detect_timestamp_tz(val: &str, tz: &Tz) -> Option<timestamp_tz> {
    let (mut micros, offset) = try_parse_formats(val, tz, AUTO_TS_FORMATS)?;
    clamp_timestamp(&mut micros);
    Some(timestamp_tz::new(micros, offset))
}

/// Parse a date string with optional auto-detect fallback.
/// Chain: ISO -> numeric-day -> auto (no dtparse).
#[allow(clippy::result_large_err)]
pub fn parse_date_with_auto(val: &str, tz: &Tz, enable_auto: bool) -> Result<i32, ErrorCode> {
    match string_to_date(val, tz) {
        Ok(days) => Ok(days),
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
pub fn parse_timestamp_with_auto(val: &str, tz: &Tz, enable_auto: bool) -> Result<i64, ErrorCode> {
    match string_to_timestamp(val, tz) {
        Ok(micros) => Ok(micros),
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
    tz: &Tz,
    enable_auto: bool,
) -> Result<timestamp_tz, ErrorCode> {
    match string_to_timestamp_tz(val.as_bytes(), || tz) {
        Ok(ts_tz) => Ok(ts_tz),
        Err(e) => {
            if enable_auto {
                if let Some(mut micros) = parse_epoch_str(val) {
                    clamp_timestamp(&mut micros);
                    let offset = offset_seconds_at(tz, micros.div_euclid(MICROS_PER_SEC))
                        .expect("clamped Databend timestamp has a timezone offset");
                    return Ok(timestamp_tz::new(micros, offset));
                }
                if let Some(ts_tz) = auto_detect_timestamp_tz(val, tz) {
                    return Ok(ts_tz);
                }
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_non_padded_iso_date_with_auto_detect() {
        let tz = Tz::UTC;
        let expected = parse_date_with_auto("2027-01-01", &tz, false).unwrap();

        for val in ["2027-1-1", "2027-01-1", "2027-1-01"] {
            assert_eq!(parse_date_with_auto(val, &tz, true).unwrap(), expected);
            assert!(parse_date_with_auto(val, &tz, false).is_err());
        }
    }

    #[test]
    fn test_parse_non_padded_iso_timestamp_with_auto_detect() {
        let tz = Tz::UTC;
        let expected = parse_timestamp_with_auto("2027-01-01 02:03:04", &tz, false).unwrap();

        for val in [
            "2027-1-1 02:03:04",
            "2027-01-1 02:03:04",
            "2027-1-01 02:03:04",
        ] {
            assert_eq!(parse_timestamp_with_auto(val, &tz, true).unwrap(), expected);
            assert!(parse_timestamp_with_auto(val, &tz, false).is_err());
        }
    }
}
