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

use std::result::Result;

use chrono::NaiveDate;

use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalSize;

pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Day offset of `date` from 1970-01-01.
#[inline]
pub fn uniform_date(date: NaiveDate) -> i32 {
    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"))
        .num_days() as i32
}

/// Parse a decimal at a known precision and scale, optionally rounding half away from zero.
/// In prefix mode (`exact = false`), return the number of bytes consumed before the delimiter.
/// For compatibility, an exponent marker with no digits (after an optional sign) means
/// exponent zero; the marker and sign are consumed. Explicit exponents must fit in `i32`,
/// including for zero mantissas or values that would otherwise round to zero. This is
/// intentional: out-of-range exponents are treated as malformed rather than saturated.
// Used in function, so we don't want to return ErrorCode with backtrace
pub fn read_decimal_with_size<T: Decimal>(
    buf: &[u8],
    size: DecimalSize,
    exact: bool,
    rounding_mode: bool,
) -> Result<(T, usize), String> {
    if buf.is_empty() {
        return Err(decimal_parse_error("empty"));
    }

    let negative = buf[0] == b'-';
    let start = usize::from(matches!(buf[0], b'+' | b'-'));
    let mut pos = start;
    let mut has_point = false;
    let mut significant_digits = 0usize;
    let mut fractional_digits = 0usize;
    // Locate the decimal point and exponent before accumulating a coefficient. A
    // long mantissa can still fit after applying its exponent, e.g. 10000e-4.
    while pos < buf.len() {
        match buf[pos] {
            b'0'..=b'9' => {
                if has_point {
                    fractional_digits += 1;
                }
                if significant_digits > 0 || buf[pos] != b'0' {
                    significant_digits += 1;
                }
            }
            b'.' if !has_point => has_point = true,
            b'e' | b'E' => break,
            _ if exact => return Err(decimal_parse_error("unexpected char")),
            _ => break,
        }
        pos += 1;
    }
    // The scan consumed only digits and at most one decimal point.
    let has_digits = pos - start > usize::from(has_point);
    if !has_digits {
        return Err(decimal_parse_error("no digits"));
    }
    let mantissa = &buf[start..pos];
    let mut exponent = 0i32;
    if pos < buf.len() && matches!(buf[pos], b'e' | b'E') {
        pos += 1;
        let mut exponent_sign = 1;
        if pos < buf.len() && matches!(buf[pos], b'+' | b'-') {
            if buf[pos] == b'-' {
                exponent_sign = -1;
            }
            pos += 1;
        }
        while pos < buf.len() && buf[pos].is_ascii_digit() {
            exponent = exponent
                .checked_mul(10)
                .and_then(|e| e.checked_add((buf[pos] - b'0') as i32 * exponent_sign))
                .ok_or_else(decimal_overflow_error)?;
            pos += 1;
        }
    }
    if exact && pos != buf.len() {
        return Err(decimal_parse_error("unexpected char"));
    }
    if significant_digits == 0 {
        return Ok((T::zero(), pos));
    }

    // Number of significant digits to the left of the *scaled* decimal point.
    // It may be negative for tiny inputs, or exceed the mantissa length when
    // padding is needed. Leading fractional zeros never enter the coefficient.
    let coefficient_digits = (significant_digits as i64 - fractional_digits as i64)
        .checked_add(exponent as i64)
        .and_then(|e| e.checked_add(size.scale() as i64))
        .ok_or_else(decimal_overflow_error)?;
    if coefficient_digits > size.precision() as i64 {
        return Err(decimal_overflow_error());
    }
    if coefficient_digits < 0 {
        return Ok((T::zero(), pos));
    }

    let mut significant = mantissa
        .iter()
        .copied()
        .filter(u8::is_ascii_digit)
        .skip_while(|&digit| digit == b'0');
    let mut n = T::zero();
    let mut retained = 0u8;
    #[inline(always)]
    fn append_chunk<D: Decimal>(n: D, retained: u8, chunk: i64, digits: u8) -> Result<D, String> {
        if retained == 0 {
            Ok(D::from_i64(chunk))
        } else {
            n.checked_mul(D::e(digits))
                .and_then(|n| n.checked_add(D::from_i64(chunk)))
                .ok_or_else(decimal_overflow_error)
        }
    }
    let mut chunk = 0i64;
    let mut chunk_digits = 0u8;
    for digit in significant.by_ref().take(coefficient_digits as usize) {
        // Any 18 decimal digits fit in i64. Accumulate there to avoid a checked
        // Decimal128/256 multiply and add for every digit.
        chunk = chunk * 10 + (digit - b'0') as i64;
        chunk_digits += 1;
        if chunk_digits == 18 {
            n = append_chunk(n, retained, chunk, chunk_digits)?;
            retained += chunk_digits;
            chunk = 0;
            chunk_digits = 0;
        }
    }
    if chunk_digits != 0 {
        n = append_chunk(n, retained, chunk, chunk_digits)?;
        retained += chunk_digits;
    }
    // This power is bounded by the target precision, not the input's exponent
    // or leading zeros. Only the final coefficient is accumulated in T.
    let padding = coefficient_digits as u8 - retained;
    if padding != 0 {
        n = n
            .checked_mul(T::e(padding))
            .ok_or_else(decimal_overflow_error)?;
    }
    // Keep the rounding digit separate: precision + 1 digits may not fit in T.
    if rounding_mode && significant.next().is_some_and(|digit| digit >= b'5') {
        n = n.checked_add(T::one()).ok_or_else(decimal_overflow_error)?;
    }
    // Rounding can carry into an extra digit (e.g. 99.995 into DECIMAL(4, 2)).
    if n > T::max_for_precision(size.precision()) {
        return Err(decimal_overflow_error());
    }
    if negative {
        n = n
            .checked_mul(T::minus_one())
            .ok_or_else(decimal_overflow_error)?;
    }
    Ok((n, pos))
}

pub fn read_decimal_from_json<T: Decimal>(
    value: &serde_json::Value,
    size: DecimalSize,
    rounding_mode: bool,
) -> Result<T, String> {
    let text = match value {
        serde_json::Value::Number(n) => {
            // With arbitrary_precision, Number stores text. Choose the integer parser
            // by sign so decimals do not fail both as_i64() and as_u64(), while still
            // keeping integer input out of the more expensive decimal parser.
            let text = n.as_str();
            let integer = if text.starts_with('-') {
                text.parse::<i64>().ok().map(T::from_i64)
            } else {
                text.parse::<u64>().ok().and_then(T::from_i128)
            };
            if let Some(n) = integer {
                return n.with_size(size).ok_or_else(decimal_overflow_error);
            }
            text
        }
        serde_json::Value::String(s) => s.as_str(),
        _ => return Err("Incorrect json value for decimal".to_string()),
    };
    // Borrow the exact number text, without allocating or losing digits through f64.
    let (n, _) = read_decimal_with_size::<T>(text.as_bytes(), size, true, rounding_mode)?;
    Ok(n)
}

fn decimal_parse_error(msg: &str) -> String {
    format!("bad decimal literal: {msg}")
}

fn decimal_overflow_error() -> String {
    "Decimal overflow".to_string()
}
