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

use std::io::Cursor;
use std::io::Read;

use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::Timelike;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_timezone::Tz;
use databend_common_timezone::fast_utc_from_local;
use databend_common_timezone::offset_seconds_at;

use crate::cursor_ext::cursor_read_bytes_ext::ReadBytesExt;
use crate::datetime::parse_standard_timestamp as parse_iso_timestamp;

pub enum DateTimeResType {
    Datetime(i64),
    Date(i32),
}

pub trait BufferReadDateTimeExt {
    fn read_date_text(&mut self, tz: &Tz) -> Result<i32>;
    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTimeResType>;
    fn read_text_to_datetime(&mut self, tz: &Tz, need_date: bool) -> Result<DateTimeResType>;
}

const DATE_LEN: usize = 10;
const MICROS_PER_SEC: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

// ISO 8601 maximum offset.
const MAX_OFFSET_HOURS: i32 = 14;

fn parse_time_part(buf: &[u8], size: usize) -> Result<u32> {
    if size > 0 && size < 3 {
        Ok(lexical_core::FromLexical::from_lexical(buf)
            .map_err_to_code(ErrorCode::BadBytes, || "time part parse error".to_string())?)
    } else {
        let msg = format!(
            "err with parse time part. Format like this:[03:00:00], got {} digits",
            size
        );
        Err(ErrorCode::BadBytes(msg))
    }
}

fn days_from_epoch(date: &NaiveDate) -> i32 {
    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"))
        .num_days() as i32
}

fn local_to_micros(tz: &Tz, local: &NaiveDateTime, micro: u32) -> Option<i64> {
    fast_utc_from_local(
        tz,
        local.year(),
        local.month() as u8,
        local.day() as u8,
        local.hour() as u8,
        local.minute() as u8,
        local.second() as u8,
        micro,
    )
}

fn local_to_micros_checked(tz: &Tz, local: &NaiveDateTime, micro: u32) -> Result<i64> {
    local_to_micros(tz, local, micro).ok_or_else(|| {
        ErrorCode::BadBytes(format!(
            "Invalid local datetime {} for timezone {tz}",
            local.format("%Y-%m-%d %H:%M:%S")
        ))
    })
}

// Replace the session offset used during civil-time resolution.
fn apply_explicit_offset(micros: i64, tz: &Tz, provided_offset: i32) -> Result<i64> {
    let session_offset = offset_seconds_at(tz, micros.div_euclid(MICROS_PER_SEC))
        .ok_or_else(|| ErrorCode::BadBytes("Datetime is out of range".to_string()))?;
    let delta = i64::from(session_offset - provided_offset);

    micros
        .checked_add(delta * MICROS_PER_SEC)
        .ok_or_else(|| ErrorCode::BadBytes("Datetime offset adjustment overflowed".to_string()))
}

fn try_read_standard_timestamp<T: AsRef<[u8]>>(
    cursor: &mut Cursor<T>,
    tz: &Tz,
    need_date: bool,
) -> Result<Option<DateTimeResType>> {
    let pos = cursor.position() as usize;
    let data = cursor.get_ref().as_ref();
    if pos >= data.len() {
        return Ok(None);
    }

    match parse_standard_timestamp(&data[pos..], tz, need_date) {
        Some(Ok((consumed, value))) => {
            cursor.set_position((pos + consumed) as u64);
            Ok(Some(value))
        }
        Some(Err(err)) => Err(err),
        None => Ok(None),
    }
}

fn parse_standard_timestamp(
    input: &[u8],
    tz: &Tz,
    need_date: bool,
) -> Option<Result<(usize, DateTimeResType)>> {
    parse_iso_timestamp(input).map(|parsed_result| {
        parsed_result.and_then(|parsed| {
            let value = build_best_effort_result(
                tz,
                parsed.year,
                parsed.month,
                parsed.day,
                parsed.hour,
                parsed.minute,
                parsed.second,
                parsed.micro,
                need_date,
            )?;

            let value = match (parsed.provided_offset, value) {
                (Some(offset), DateTimeResType::Datetime(micros)) => {
                    DateTimeResType::Datetime(apply_explicit_offset(micros, tz, offset)?)
                }
                (_, other) => other,
            };

            Ok((parsed.consumed, value))
        })
    })
}

fn build_best_effort_result(
    tz: &Tz,
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
    need_date: bool,
) -> Result<DateTimeResType> {
    let (year, month, day) = if year == 0 && month == 0 && day == 0 {
        (1970, 1, 1)
    } else {
        (year, month, day)
    };

    let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| {
        ErrorCode::BadBytes(format!(
            "Invalid date value {:04}-{:02}-{:02}",
            year, month, day
        ))
    })?;

    if need_date {
        return Ok(DateTimeResType::Date(days_from_epoch(&date)));
    }

    let local = date
        .and_hms_opt(hour as u32, minute as u32, second as u32)
        .ok_or_else(|| {
            ErrorCode::BadBytes(format!(
                "Invalid time value {:02}:{:02}:{:02}",
                hour, minute, second
            ))
        })?;

    Ok(DateTimeResType::Datetime(local_to_micros_checked(
        tz, &local, micro,
    )?))
}

fn read_offset_seconds<T: AsRef<[u8]>>(
    cursor: &mut Cursor<T>,
    buf: &mut Vec<u8>,
    west_tz: bool,
) -> Result<i32> {
    fn validated(hour_offset: i32, minute_offset: i32, west_tz: bool) -> Result<i32> {
        let in_range = (hour_offset == MAX_OFFSET_HOURS && minute_offset == 0)
            || ((0..60).contains(&minute_offset) && hour_offset < MAX_OFFSET_HOURS);

        if !in_range {
            return Err(ErrorCode::BadBytes(format!(
                "Invalid Timezone Offset: The minute offset '{}' is outside the valid range. Expected range is [00-59] within a timezone gap of [-14:00, +14:00]",
                minute_offset
            )));
        }

        let seconds = hour_offset * 3600 + minute_offset * 60;
        Ok(if west_tz { -seconds } else { seconds })
    }

    let n = cursor.keep_read(buf, |f| f.is_ascii_digit());
    match n {
        2 => {
            let hour_offset: i32 = lexical_core::FromLexical::from_lexical(buf.as_slice())
                .map_err_to_code(ErrorCode::BadBytes, || {
                    "hour offset parse error".to_string()
                })?;
            if !(0..=MAX_OFFSET_HOURS).contains(&hour_offset) {
                return Err(ErrorCode::BadBytes(format!(
                    "Invalid Timezone Offset: The hour offset '{}' is outside the valid range. Expected range is [00-14] within a timezone gap of [-14:00, +14:00]",
                    hour_offset
                )));
            }

            buf.clear();
            if !cursor.ignore_byte(b':') {
                return validated(hour_offset, 0, west_tz);
            }

            if cursor.keep_read(buf, |f| f.is_ascii_digit()) != 2 {
                return Err(ErrorCode::BadBytes(
                    "Timezone Parsing Error: Incorrect format in hour part. The time zone format must conform to the ISO 8601 standard",
                ));
            }
            let minute_offset: i32 = lexical_core::FromLexical::from_lexical(buf.as_slice())
                .map_err_to_code(ErrorCode::BadBytes, || {
                    "minute offset parse error".to_string()
                })?;
            validated(hour_offset, minute_offset, west_tz)
        }
        4 => {
            let hour_offset: i32 = lexical_core::FromLexical::from_lexical(&buf.as_slice()[..2])
                .map_err_to_code(ErrorCode::BadBytes, || {
                    "hour offset parse error".to_string()
                })?;
            let minute_offset: i32 = lexical_core::FromLexical::from_lexical(&buf.as_slice()[2..])
                .map_err_to_code(ErrorCode::BadBytes, || {
                    "minute offset parse error".to_string()
                })?;
            buf.clear();

            if !(0..=MAX_OFFSET_HOURS).contains(&hour_offset) {
                return Err(ErrorCode::BadBytes(format!(
                    "Invalid Timezone Offset: The hour offset '{}' is outside the valid range. Expected range is [00-14] within a timezone gap of [-14:00, +14:00]",
                    hour_offset
                )));
            }
            validated(hour_offset, minute_offset, west_tz)
        }
        _ => Err(ErrorCode::BadBytes(
            "Timezone Parsing Error: Incorrect format. The time zone format must conform to the ISO 8601 standard",
        )),
    }
}

impl<T> BufferReadDateTimeExt for Cursor<T>
where T: AsRef<[u8]>
{
    fn read_date_text(&mut self, tz: &Tz) -> Result<i32> {
        // TODO support YYYYMMDD format
        self.read_text_to_datetime(tz, true).map(|res| match res {
            DateTimeResType::Date(days) => days,
            DateTimeResType::Datetime(micros) => {
                // Truncate towards the epoch-relative day containing the instant.
                micros
                    .div_euclid(MICROS_PER_SEC)
                    .div_euclid(SECONDS_PER_DAY) as i32
            }
        })
    }

    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTimeResType> {
        self.read_text_to_datetime(tz, false)
    }

    fn read_text_to_datetime(&mut self, tz: &Tz, need_date: bool) -> Result<DateTimeResType> {
        if let Some(value) = try_read_standard_timestamp(self, tz, need_date)? {
            return Ok(value);
        }

        let mut buf = vec![0; DATE_LEN];
        self.read_exact(buf.as_mut_slice())?;
        let mut v =
            std::str::from_utf8(buf.as_slice()).map_err_to_code(ErrorCode::BadBytes, || {
                format!(
                    "UTF-8 Conversion Failed: Unable to convert value {:?} to UTF-8",
                    buf
                )
            })?;

        // The all-zero date is accepted and means the epoch.
        if v == "0000-00-00" {
            v = "1970-01-01";
        }

        let d = NaiveDate::parse_from_str(v, "%Y-%m-%d").map_err_to_code(
            ErrorCode::BadBytes,
            || {
                format!(
                    "Date Parsing Error: The value '{}' could not be parsed into a valid Date",
                    v
                )
            },
        )?;

        buf.clear();
        if !self.ignore(|b| b == b' ' || b == b'T') {
            // Date with no time part.
            if need_date {
                return Ok(DateTimeResType::Date(days_from_epoch(&d)));
            }
            let midnight = d.and_hms_opt(0, 0, 0).expect("midnight is valid");
            return Ok(DateTimeResType::Datetime(local_to_micros_checked(
                tz, &midnight, 0,
            )?));
        }

        let mut buf = Vec::with_capacity(2);
        let mut times = Vec::with_capacity(3);
        loop {
            buf.clear();
            let size = self.keep_read(&mut buf, |f| f.is_ascii_digit());
            if size == 0 {
                break;
            }
            times.push(parse_time_part(&buf, size)?);
            if times.len() == 3 {
                break;
            }
            self.ignore_byte(b':');
        }

        // Missing time fields default to zero.
        let partial_time = times.len() < 3;
        times.resize(3, 0);

        let local = d.and_hms_opt(times[0], times[1], times[2]).ok_or_else(|| {
            ErrorCode::BadBytes(format!(
                "Invalid time {:02}:{:02}:{:02}",
                times[0], times[1], times[2]
            ))
        })?;

        if partial_time {
            if need_date {
                return Ok(DateTimeResType::Date(days_from_epoch(&d)));
            }
            return Ok(DateTimeResType::Datetime(local_to_micros_checked(
                tz, &local, 0,
            )?));
        }

        let mut micro = 0_u32;
        if self.ignore_byte(b'.') {
            buf.clear();
            let size = self.keep_read(&mut buf, |f| f.is_ascii_digit());
            if size == 0 {
                return Err(ErrorCode::BadBytes(
                    "Microsecond Parsing Error: Expecting a format like [.123456] for microseconds part",
                ));
            }
            let mut scales: u64 = lexical_core::FromLexical::from_lexical(buf.as_slice())
                .map_err_to_code(ErrorCode::BadBytes, || {
                    "datetime scales parse error".to_string()
                })?;
            // Preserve the legacy long-fraction truncation.
            if size <= 9 {
                scales *= 10_u64.pow(9 - size as u32)
            } else {
                scales /= (size as u64 - 9) * 10
            }
            micro = (scales / 1_000) as u32;
        }

        let micros = local_to_micros_checked(tz, &local, micro)?;

        buf.clear();
        let explicit_offset = if self.ignore(|b| b == b'z' || b == b'Z') {
            Some(0)
        } else if self.ignore_byte(b'+') {
            Some(read_offset_seconds(self, &mut buf, false)?)
        } else if self.ignore_byte(b'-') {
            Some(read_offset_seconds(self, &mut buf, true)?)
        } else {
            None
        };

        if need_date {
            // Parsing the offset still validates the input suffix.
            return Ok(DateTimeResType::Date(days_from_epoch(&d)));
        }

        match explicit_offset {
            Some(offset) => Ok(DateTimeResType::Datetime(apply_explicit_offset(
                micros, tz, offset,
            )?)),
            None => Ok(DateTimeResType::Datetime(micros)),
        }
    }
}
