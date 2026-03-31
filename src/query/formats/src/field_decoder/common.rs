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
use databend_common_expression::types::timestamp::clamp_timestamp;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::cursor_ext::read_num_text_exact;
use databend_functions_scalar_datetime::datetime::auto_detect_timestamp;
use databend_functions_scalar_datetime::datetime::int64_to_timestamp;
use databend_functions_scalar_datetime::datetime::parse_date_with_auto;
use databend_functions_scalar_datetime::datetime::parse_epoch_str;
use databend_functions_scalar_datetime::datetime::parse_timestamp_tz_with_auto;

use crate::InputCommonSettings;

pub(crate) fn read_date(
    column: &mut Vec<i32>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    let s = std::str::from_utf8(data).map_err(|e| ErrorCode::BadBytes(format!("{e}")))?;
    let days = parse_date_with_auto(
        s,
        &settings.settings.jiff_timezone,
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
    let t = buffer_readr.read_timestamp_text(&settings.settings.jiff_timezone);
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
            let mut ts = t.timestamp().as_microsecond();
            clamp_timestamp(&mut ts);
            column.push(ts);
            Ok(())
        }
        Ok(_) => unreachable!(),
        Err(e) => {
            if settings.settings.enable_auto_detect_datetime_format
                && let Ok(s) = std::str::from_utf8(data)
            {
                if let Some(micros) = parse_epoch_str(s) {
                    column.push(micros);
                    return Ok(());
                }
                if let Some(micros) = auto_detect_timestamp(s, &settings.settings.jiff_timezone) {
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
        &settings.settings.jiff_timezone,
        settings.settings.enable_auto_detect_datetime_format,
    )?;
    column.push(ts_tz);
    Ok(())
}
