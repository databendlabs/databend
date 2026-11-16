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

use bstr::ByteSlice;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::Number;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_expression::utils::auto_detect_datetime::auto_detect_timestamp;
use databend_common_expression::utils::auto_detect_datetime::int64_to_timestamp;
use databend_common_expression::utils::auto_detect_datetime::parse_date_with_auto;
use databend_common_expression::utils::auto_detect_datetime::parse_epoch_str;
use databend_common_expression::utils::auto_detect_datetime::parse_timestamp_tz_with_auto;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::cursor_ext::read_num_text_exact;
use databend_common_io::datetime::check_input_year;
use databend_common_io::datetime::parse_date_prefix;

use crate::InputCommonSettings;

pub(crate) fn read_date(
    column: &mut Vec<i32>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    let s = std::str::from_utf8(data).map_err(|e| ErrorCode::BadBytes(format!("{e}")))?;
    let days = parse_date_with_auto(
        s,
        &settings.settings.timezone,
        settings.settings.enable_auto_detect_datetime_format,
    )?;
    column.push(days);
    Ok(())
}

pub(crate) fn read_timestamp(
    column: &mut Vec<i64>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    // Epoch path: no '-' means numeric input.
    if !data.contains(&b'-') {
        // When auto-detect is off, preserve original behavior: epoch parse
        // failure is an immediate error (no fallthrough to ISO).
        if !settings.settings.enable_auto_detect_datetime_format {
            let n: i64 = read_num_text_exact(data)?;
            column.push(int64_to_timestamp(n));
            return Ok(());
        }
        if let Ok(n) = read_num_text_exact::<i64>(data) {
            column.push(int64_to_timestamp(n));
            return Ok(());
        }
    }

    // Try ISO/standard timestamp text.
    let mut buffer_readr = Cursor::new(&data);
    let t = buffer_readr.read_timestamp_text(&settings.settings.timezone);
    match t {
        Ok(DateTimeResType::Datetime(t)) => {
            if !buffer_readr.eof() {
                let data = data.to_str().unwrap_or("not utf8");
                let msg = format!(
                    "fail to deserialize timestamp, unexpected end at pos {} of {}",
                    buffer_readr.position(),
                    data
                );
                return Err(ErrorCode::BadBytes(msg));
            }
            let mut ts = t;
            if let Some((year, _, _, _)) = parse_date_prefix(data)
                && check_input_year(year).is_err()
            {
                // Extended years are accepted only for valid UTC round trips.
                // Do not turn formerly rejected text into a clamped timestamp.
                check_timestamp(ts).map_err(ErrorCode::BadArguments)?;
            } else {
                clamp_timestamp(&mut ts);
            }
            column.push(ts);
            Ok(())
        }
        Ok(_) => unreachable!(),
        Err(e) if e.code() == ErrorCode::INVALID_TIMEZONE => Err(e),
        Err(e) => {
            if settings.settings.enable_auto_detect_datetime_format
                && let Ok(s) = std::str::from_utf8(data)
            {
                if let Some(micros) = parse_epoch_str(s) {
                    column.push(micros);
                    return Ok(());
                }
                if let Some(micros) = auto_detect_timestamp(s, &settings.settings.timezone)? {
                    column.push(micros);
                    return Ok(());
                }
            }
            Err(e)
        }
    }
}

pub(crate) fn read_timestamp_tz(
    column: &mut Vec<timestamp_tz>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    let s = std::str::from_utf8(data).map_err(|e| ErrorCode::BadBytes(format!("{e}")))?;
    let ts_tz = parse_timestamp_tz_with_auto(
        s,
        &settings.settings.timezone,
        settings.settings.enable_auto_detect_datetime_format,
    )?;
    column.push(ts_tz);
    Ok(())
}

/// Parse numeric text that is not a plain integer literal, such as `1.5` or
/// `1.76e18`, into the integer type `T`.
///
/// The text is parsed as an exact decimal rather than through `f64`: `f64`
/// only has 53 bits of mantissa, so values beyond 2^53 would be silently
/// altered (e.g. `1760000000123456789.4` -> `1760000000123456768`) or pushed
/// into range (e.g. `-9223372036854775809` -> `i64::MIN`). This matches
/// `CAST(<string> AS <int>)`.
///
/// The fraction is rounded half away from zero when `rounding_mode` is set,
/// otherwise truncated toward zero.
pub(crate) fn read_int_from_decimal_text<T: Number>(data: &[u8], rounding_mode: bool) -> Result<T> {
    let data_type = T::data_type();
    let size = data_type
        .get_decimal_properties()
        .ok_or_else(|| ErrorCode::BadBytes(format!("{data_type} is not an integer type")))?;
    let (n, _) = read_decimal_with_size::<i128>(data, size, true, rounding_mode).map_err(|e| {
        ErrorCode::BadBytes(format!(
            "Cannot parse value:{:?} to {data_type}, cause: {e}",
            String::from_utf8_lossy(data)
        ))
    })?;
    // The decimal precision only bounds the number of digits, so the value must
    // still be checked against the exact range of `T`.
    num_traits::cast::cast(n).ok_or_else(|| number_overflow_error::<T>(data))
}

/// Error for numeric text whose value does not fit the integer type `T`.
pub(crate) fn number_overflow_error<T: Number>(data: &[u8]) -> ErrorCode {
    ErrorCode::BadBytes(format!(
        "number {} is overflowed for {}",
        String::from_utf8_lossy(data),
        T::data_type()
    ))
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use databend_common_expression::types::Number;

    use super::read_int_from_decimal_text;

    fn check<T: Number>(text: &str, rounding: bool, expected: T) {
        let got = read_int_from_decimal_text::<T>(text.as_bytes(), rounding);
        assert_eq!(
            got.ok(),
            Some(expected),
            "text: {text}, rounding: {rounding}"
        );
    }

    fn check_err<T: Number + Debug>(text: &str, rounding: bool) {
        let got = read_int_from_decimal_text::<T>(text.as_bytes(), rounding);
        assert!(
            got.is_err(),
            "text: {text}, rounding: {rounding}, got: {got:?}"
        );
    }

    #[test]
    fn test_read_int_from_decimal_text() {
        // Rounding is half away from zero, truncating is toward zero.
        check::<i32>("1.5", true, 2);
        check::<i32>("1.4", true, 1);
        check::<i32>("-1.5", true, -2);
        check::<i32>("1.9", false, 1);
        check::<i32>("-1.9", false, -1);
        check::<i32>("0.5", true, 1);
        check::<i32>("0.05", true, 0);
        check::<i32>("1.5e1", true, 15);
        check::<i32>("15e-1", true, 2);
        check::<i32>("1e-50", true, 0);
        check::<i32>("0e300", true, 0);

        // Values beyond 2^53 keep every digit instead of going through f64.
        check::<i64>("1760000000123456789.4", true, 1760000000123456789);
        check::<i64>("1760000000123456789.5", true, 1760000000123456790);
        check::<i64>("1760000000123456789.9", false, 1760000000123456789);
        check::<i64>("9007199254740993.4", true, 9007199254740993);
        check::<i64>("3.698114021686886E+16", true, 36981140216868860);
        check::<i64>("1.760000000123456789e18", true, 1760000000123456789);
        check::<i64>("9223372036854775807.4", true, i64::MAX);
        check::<i64>("-9223372036854775808.4", true, i64::MIN);
        check::<u64>("18446744073709551615.4", true, u64::MAX);
        check::<u64>("1e19", true, 10_000_000_000_000_000_000);

        // Out of range for the target type must be rejected, not clamped.
        check_err::<i64>("-9223372036854775809", true);
        check_err::<i64>("-9223372036854776000", true);
        check_err::<i64>("9223372036854775808", true);
        check_err::<i64>("9223372036854775807.5", true);
        check_err::<i64>("-9223372036854775808.5", true);
        check_err::<i64>("1e19", true);
        check_err::<u64>("18446744073709551616", true);
        check_err::<u64>("-1.5", true);
        check_err::<u8>("255.5", true);
        check_err::<i8>("-128.6", true);
        check::<u8>("255.5", false, 255);
        check::<i8>("-128.6", false, -128);

        // Invalid text.
        check_err::<i32>("1.2.3", true);
        check_err::<i32>("abc", true);
    }
}
