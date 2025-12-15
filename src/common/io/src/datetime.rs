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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTimestamp {
    pub consumed: usize,
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro: u32,
    pub provided_offset: Option<i32>,
}

#[inline(always)]
pub fn parse_two_digits(bytes: &[u8]) -> Option<u8> {
    if bytes.len() != 2 {
        return None;
    }
    let d0 = bytes[0].wrapping_sub(b'0');
    let d1 = bytes[1].wrapping_sub(b'0');
    if d0 < 10 && d1 < 10 {
        Some(d0 * 10 + d1)
    } else {
        None
    }
}

#[inline(always)]
pub fn parse_four_digits(bytes: &[u8]) -> Option<i32> {
    if bytes.len() != 4 {
        return None;
    }
    let d0 = bytes[0].wrapping_sub(b'0');
    let d1 = bytes[1].wrapping_sub(b'0');
    let d2 = bytes[2].wrapping_sub(b'0');
    let d3 = bytes[3].wrapping_sub(b'0');
    if d0 < 10 && d1 < 10 && d2 < 10 && d3 < 10 {
        Some(((d0 as i32) * 1000) + ((d1 as i32) * 100) + ((d2 as i32) * 10) + (d3 as i32))
    } else {
        None
    }
}

/// Parse ISO-8601-like timestamps: `YYYY-MM-DD HH:MM:SS[.ffffff][Z|(+|-)hh[:mm]]`.
/// Returning `None` indicates that the input is not in the supported format.
#[inline(always)]
pub fn parse_standard_timestamp(input: &[u8]) -> Option<Result<ParsedTimestamp>> {
    if input.len() < 19 {
        return None;
    }

    if !(input[0..4].iter().all(|b| b.is_ascii_digit())
        && input[4] == b'-'
        && input[7] == b'-'
        && input[5..7].iter().all(|b| b.is_ascii_digit())
        && input[8..10].iter().all(|b| b.is_ascii_digit()))
    {
        return None;
    }

    let separator = input[10];
    if separator != b' ' && separator != b'T' {
        return None;
    }

    if input.len() < 19 || input[13] != b':' || input[16] != b':' {
        return None;
    }

    let year = parse_four_digits(&input[0..4])?;
    let month = parse_two_digits(&input[5..7])?;
    let day = parse_two_digits(&input[8..10])?;
    let hour = parse_two_digits(&input[11..13])?;
    let minute = parse_two_digits(&input[14..16])?;
    let second = parse_two_digits(&input[17..19])?;

    let mut idx = 19;
    let mut micro = 0_u32;
    if idx < input.len() && input[idx] == b'.' {
        idx += 1;
        let mut digits = 0_u32;
        while idx < input.len() && input[idx].is_ascii_digit() {
            if digits < 6 {
                micro = micro * 10 + u32::from(input[idx] - b'0');
            }
            digits += 1;
            idx += 1;
        }

        if digits == 0 {
            return Some(Err(ErrorCode::BadBytes(
                "Microsecond Parsing Error: Expecting digits after '.'".to_string(),
            )));
        }

        if digits < 6 {
            micro *= 10_u32.pow(6 - digits);
        }
    }

    while idx < input.len() && input[idx] == b' ' {
        idx += 1;
    }

    let mut provided_offset = None;
    if idx < input.len() {
        match input[idx] {
            b'Z' | b'z' => {
                provided_offset = Some(0);
                idx += 1;
            }
            b'+' | b'-' => {
                let sign = if input[idx] == b'-' { -1 } else { 1 };
                idx += 1;
                if idx + 2 > input.len() {
                    return Some(Err(ErrorCode::BadBytes(
                        "Invalid timezone offset: missing hour component".to_string(),
                    )));
                }
                let hour_offset = match parse_two_digits(&input[idx..idx + 2]) {
                    Some(v) => v as i32,
                    None => {
                        return Some(Err(ErrorCode::BadBytes(
                            "Invalid timezone offset: hour part must be digits".to_string(),
                        )));
                    }
                };
                idx += 2;
                let mut minute_offset = 0_i32;
                if idx < input.len() && input[idx] == b':' {
                    idx += 1;
                    if idx + 2 > input.len() {
                        return Some(Err(ErrorCode::BadBytes(
                            "Invalid timezone offset: missing minute component".to_string(),
                        )));
                    }
                    minute_offset = match parse_two_digits(&input[idx..idx + 2]) {
                        Some(v) => v as i32,
                        None => {
                            return Some(Err(ErrorCode::BadBytes(
                                "Invalid timezone offset minute part".to_string(),
                            )));
                        }
                    };
                    idx += 2;
                } else if idx + 1 < input.len()
                    && input[idx].is_ascii_digit()
                    && input[idx + 1].is_ascii_digit()
                {
                    minute_offset = match parse_two_digits(&input[idx..idx + 2]) {
                        Some(v) => v as i32,
                        None => {
                            return Some(Err(ErrorCode::BadBytes(
                                "Invalid timezone offset minute part".to_string(),
                            )));
                        }
                    };
                    idx += 2;
                }

                if hour_offset > 14
                    || minute_offset >= 60
                    || (hour_offset == 14 && minute_offset != 0)
                {
                    return Some(Err(ErrorCode::BadBytes(
                        "Timezone offset out of range".to_string(),
                    )));
                }

                provided_offset = Some(sign * (hour_offset * 3600 + minute_offset * 60));
            }
            _ => return None,
        }
    }

    while idx < input.len() && input[idx].is_ascii_whitespace() {
        idx += 1;
    }

    if idx != input.len() {
        return None;
    }

    Some(Ok(ParsedTimestamp {
        consumed: idx,
        year,
        month,
        day,
        hour,
        minute,
        second,
        micro,
        provided_offset,
    }))
}
