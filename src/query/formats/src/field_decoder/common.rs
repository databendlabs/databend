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
use databend_common_expression::types::timestamp_tz::string_to_timestamp_tz;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::cursor_ext::read_num_text_exact;
use databend_functions_scalar_datetime::datetime::int64_to_timestamp;

use crate::InputCommonSettings;

pub(crate) fn read_timestamp(
    column: &mut Vec<i64>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    let ts = if !data.contains(&b'-') {
        int64_to_timestamp(read_num_text_exact(data)?)
    } else {
        let mut buffer_readr = Cursor::new(&data);
        let t = buffer_readr.read_timestamp_text(&settings.jiff_timezone)?;
        match t {
            DateTimeResType::Datetime(t) => {
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
                ts
            }
            _ => unreachable!(),
        }
    };
    column.push(ts);
    Ok(())
}

pub(crate) fn read_timestamp_tz(
    column: &mut Vec<timestamp_tz>,
    data: &[u8],
    settings: &InputCommonSettings,
) -> Result<()> {
    let ts_tz = string_to_timestamp_tz(data, || &settings.jiff_timezone)?;
    column.push(ts_tz);
    Ok(())
}
