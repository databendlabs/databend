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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub trait BufferReadIntervalExt {
    fn read_interval_text(&mut self) -> Result<Interval>;
}

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buffer = [0u8; 70];
        let len = IntervalToStringCast::format(*self, &mut buffer);
        write!(f, "{}", String::from_utf8_lossy(&buffer[..len]))
    }
}

struct IntervalToStringCast;

impl IntervalToStringCast {
    fn format_signed_number(value: i64, buffer: &mut [u8], length: &mut usize) {
        let s = value.to_string();
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();
    }

    fn format_two_digits(value: i64, buffer: &mut [u8], length: &mut usize) {
        let s = format!("{:02}", value.abs());
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();
    }

    fn format_interval_value(value: i32, buffer: &mut [u8], length: &mut usize, name: &str) {
        if value == 0 {
            return;
        }
        if *length != 0 {
            buffer[*length] = b' ';
            *length += 1;
        }
        Self::format_signed_number(value as i64, buffer, length);
        let name_bytes = name.as_bytes();
        buffer[*length..*length + name_bytes.len()].copy_from_slice(name_bytes);
        *length += name_bytes.len();
        if value != 1 && value != -1 {
            buffer[*length] = b's';
            *length += 1;
        }
    }

    fn format_micros(mut micros: i64, buffer: &mut [u8], length: &mut usize) {
        if micros < 0 {
            micros = -micros;
        }
        let s = format!("{:06}", micros);
        let bytes = s.as_bytes();
        buffer[*length..*length + bytes.len()].copy_from_slice(bytes);
        *length += bytes.len();

        while *length > 0 && buffer[*length - 1] == b'0' {
            *length -= 1;
        }
    }

    pub fn format(interval: Interval, buffer: &mut [u8]) -> usize {
        let mut length = 0;
        if interval.months != 0 {
            let years = interval.months / 12;
            let months = interval.months - years * 12;
            Self::format_interval_value(years, buffer, &mut length, " year");
            Self::format_interval_value(months, buffer, &mut length, " month");
        }
        if interval.days != 0 {
            Self::format_interval_value(interval.days, buffer, &mut length, " day");
        }
        if interval.micros != 0 {
            if length != 0 {
                buffer[length] = b' ';
                length += 1;
            }
            let mut micros = interval.micros;
            if micros < 0 {
                buffer[length] = b'-';
                length += 1;
                micros = -micros;
            }
            let hour = micros / MICROS_PER_HOUR;
            micros -= hour * MICROS_PER_HOUR;
            let min = micros / MICROS_PER_MINUTE;
            micros -= min * MICROS_PER_MINUTE;
            let sec = micros / MICROS_PER_SEC;
            micros -= sec * MICROS_PER_SEC;

            Self::format_signed_number(hour, buffer, &mut length);
            buffer[length] = b':';
            length += 1;
            Self::format_two_digits(min, buffer, &mut length);
            buffer[length] = b':';
            length += 1;
            Self::format_two_digits(sec, buffer, &mut length);
            if micros != 0 {
                buffer[length] = b'.';
                length += 1;
                Self::format_micros(micros, buffer, &mut length);
            }
        } else if length == 0 {
            buffer[..8].copy_from_slice(b"00:00:00");
            return 8;
        }
        length
    }
}

impl Interval {
    pub fn from_string(str: &str) -> Result<Self> {
        Self::from_cstring(str.as_bytes())
    }
    pub fn from_cstring(str: &[u8]) -> Result<Self> {
        let mut result = Interval::default();
        let mut pos = 0;
        let len = str.len();
        let mut found_any = false;

        if len == 0 {
            return Err(ErrorCode::BadArguments("Empty string".to_string()));
        }
        match str[pos] {
            b'@' => {
                pos += 1;
            }
            b'P' | b'p' => {
                return Err(ErrorCode::BadArguments(
                    "Posix intervals not supported yet".to_string(),
                ));
            }
            _ => {}
        }

        while pos < len {
            match str[pos] {
                b' ' | b'\t' | b'\n' => {
                    pos += 1;
                    continue;
                }
                b'0'..=b'9' => {
                    let (number, fraction, next_pos) = parse_number(&str[pos..])?;
                    pos += next_pos;
                    let (specifier, next_pos) = parse_identifier(&str[pos..]);

                    pos += next_pos;
                    let _ = apply_specifier(&mut result, number, fraction, &specifier);
                    found_any = true;
                }
                b'-' => {
                    pos += 1;
                    let (number, fraction, next_pos) = parse_number(&str[pos..])?;
                    let number = -number;
                    let fraction = -fraction;

                    pos += next_pos;

                    let (specifier, next_pos) = parse_identifier(&str[pos..]);

                    pos += next_pos;
                    let _ = apply_specifier(&mut result, number, fraction, &specifier);
                    found_any = true;
                }
                b'a' | b'A' => {
                    if len - pos < 3
                        || str[pos + 1] != b'g' && str[pos + 1] != b'G'
                        || str[pos + 2] != b'o' && str[pos + 2] != b'O'
                    {
                        return Err(ErrorCode::BadArguments(
                            "Invalid 'ago' specifier".to_string(),
                        ));
                    }
                    pos += 3;
                    while pos < len {
                        match str[pos] {
                            b' ' | b'\t' | b'\n' => {
                                pos += 1;
                            }
                            _ => {
                                return Err(ErrorCode::BadArguments(
                                    "Trailing characters after 'ago'".to_string(),
                                ));
                            }
                        }
                    }
                    result.months = -result.months;
                    result.days = -result.days;
                    result.micros = -result.micros;
                    return Ok(result);
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Unexpected character at position {}",
                        pos
                    )));
                }
            }
        }

        if !found_any {
            return Err(ErrorCode::BadArguments(
                "No interval specifiers found".to_string(),
            ));
        }
        Ok(result)
    }
}

fn parse_number(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut fraction: i64 = 0;
    let mut pos = 0;

    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(ErrorCode::BadArguments("Number too large"))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }

    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let mut mult: i64 = 100000;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            if mult > 0 {
                fraction += (bytes[pos] - b'0') as i64 * mult;
            }
            mult /= 10;
            pos += 1;
        }
    }
    if pos < bytes.len() && bytes[pos] == b':' {
        let time_bytes = &bytes[pos..];
        let mut time_pos = 0;
        let mut total_micros: i64 = number * 60 * 60 * MICROS_PER_SEC;
        let mut colon_count = 0;

        while colon_count < 2 && time_bytes.len() > time_pos {
            let (minute, _, next_pos) = parse_time_part(&time_bytes[time_pos..])?;
            let minute_nanos = minute * 60 * MICROS_PER_SEC;
            total_micros += minute_nanos;
            time_pos += next_pos;

            if time_bytes.len() > time_pos && time_bytes[time_pos] == b':' {
                time_pos += 1;
                colon_count += 1;
            } else {
                break;
            }
        }
        if time_bytes.len() > time_pos {
            let (seconds, micros, next_pos) = parse_time_part_with_micros(&time_bytes[time_pos..])?;
            total_micros += seconds * MICROS_PER_SEC + micros;
            time_pos += next_pos;
        }
        return Ok((total_micros, 0, pos + time_pos));
    }

    if pos == 0 {
        return Err(ErrorCode::BadArguments("Expected number".to_string()));
    }

    Ok((number, fraction, pos))
}

fn parse_time_part(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut pos = 0;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(ErrorCode::BadArguments("Number too large"))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }
    Ok((number, 0, pos))
}

fn parse_time_part_with_micros(bytes: &[u8]) -> Result<(i64, i64, usize)> {
    let mut number: i64 = 0;
    let mut fraction: i64 = 0;
    let mut pos = 0;

    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        number = number
            .checked_mul(10)
            .ok_or(ErrorCode::BadArguments("Number too large"))?
            + (bytes[pos] - b'0') as i64;
        pos += 1;
    }

    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let mut mult: i64 = 100000;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            if mult > 0 {
                fraction += (bytes[pos] - b'0') as i64 * mult;
            }
            mult /= 10;
            pos += 1;
        }
    }

    Ok((number, fraction, pos))
}

fn parse_identifier(s: &[u8]) -> (String, usize) {
    let mut pos = 0;
    while pos < s.len() && (s[pos] == b' ' || s[pos] == b'\t' || s[pos] == b'\n') {
        pos += 1;
    }
    let start_pos = pos;
    while pos < s.len() && (s[pos].is_ascii_alphabetic()) {
        pos += 1;
    }

    if pos == start_pos {
        return ("".to_string(), pos);
    }

    let identifier = String::from_utf8_lossy(&s[start_pos..pos]).to_string();
    (identifier, pos)
}

#[derive(Debug, PartialEq, Eq)]
enum DatePartSpecifier {
    Millennium,
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Day,
    Week,
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
}

fn try_get_date_part_specifier(specifier_str: &str) -> Result<DatePartSpecifier> {
    match specifier_str.to_lowercase().as_str() {
        "millennium" | "millennia" => Ok(DatePartSpecifier::Millennium),
        "century" | "centuries" => Ok(DatePartSpecifier::Century),
        "decade" | "decades" => Ok(DatePartSpecifier::Decade),
        "year" | "years" | "y" => Ok(DatePartSpecifier::Year),
        "quarter" | "quarters" => Ok(DatePartSpecifier::Quarter),
        "month" | "months" | "mon" => Ok(DatePartSpecifier::Month),
        "day" | "days" | "d" => Ok(DatePartSpecifier::Day),
        "week" | "weeks" | "w" => Ok(DatePartSpecifier::Week),
        "microsecond" | "microseconds" | "us" => Ok(DatePartSpecifier::Microseconds),
        "millisecond" | "milliseconds" | "ms" => Ok(DatePartSpecifier::Milliseconds),
        "second" | "seconds" | "s" => Ok(DatePartSpecifier::Second),
        "minute" | "minutes" | "m" => Ok(DatePartSpecifier::Minute),
        "hour" | "hours" | "h" => Ok(DatePartSpecifier::Hour),
        _ => Err(ErrorCode::BadArguments(format!(
            "Invalid date part specifier: {}",
            specifier_str
        ))),
    }
}

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MSEC: i64 = 1_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SEC;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const DAYS_PER_WEEK: i32 = 7;
const MONTHS_PER_QUARTER: i32 = 3;
const MONTHS_PER_YEAR: i32 = 12;
const MONTHS_PER_DECADE: i32 = 120;
const MONTHS_PER_CENTURY: i32 = 1200;
const MONTHS_PER_MILLENNIUM: i32 = 12000;

fn apply_specifier(
    result: &mut Interval,
    number: i64,
    fraction: i64,
    specifier_str: &str,
) -> Result<()> {
    if specifier_str.is_empty() {
        result.micros = result
            .micros
            .checked_add(number)
            .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        result.micros = result
            .micros
            .checked_add(fraction)
            .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        return Ok(());
    }

    let specifier = try_get_date_part_specifier(specifier_str)?;
    match specifier {
        DatePartSpecifier::Millennium => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_MILLENNIUM as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Century => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_CENTURY as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Decade => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_DECADE as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Year => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_YEAR as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Quarter => {
            result.months = result
                .months
                .checked_add(
                    number
                        .checked_mul(MONTHS_PER_QUARTER as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Month => {
            result.months = result
                .months
                .checked_add(
                    number
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Day => {
            result.days = result
                .days
                .checked_add(
                    number
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Week => {
            result.days = result
                .days
                .checked_add(
                    number
                        .checked_mul(DAYS_PER_WEEK as i64)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?
                        .try_into()
                        .map_err(|_| ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Microseconds => {
            result.micros = result
                .micros
                .checked_add(number)
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Milliseconds => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_MSEC)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Second => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_SEC)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Minute => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_MINUTE)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
        DatePartSpecifier::Hour => {
            result.micros = result
                .micros
                .checked_add(
                    number
                        .checked_mul(MICROS_PER_HOUR)
                        .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?,
                )
                .ok_or(ErrorCode::BadArguments("Overflow".to_string()))?;
        }
    }
    Ok(())
}
