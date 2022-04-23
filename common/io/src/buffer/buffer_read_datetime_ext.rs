// Copyright 2021 Datafuse Labs.
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

use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::TimeZone;
use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;

use super::BufferRead;
use super::BufferReadExt;

pub trait BufferReadDateTimeExt: BufferRead {
    fn read_date_text(&mut self) -> Result<NaiveDate>;
    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTime<Tz>>;
}

const DATE_LEN: usize = 10;
const DATE_TIME_LEN: usize = 19;

impl<R> BufferReadDateTimeExt for R
where R: BufferRead
{
    fn read_date_text(&mut self) -> Result<NaiveDate> {
        // TODO support YYYYMMDD format
        let mut buf = vec![0; DATE_LEN];
        self.read_exact(buf.as_mut_slice())?;

        let v = std::str::from_utf8(buf.as_slice())
            .map_err_to_code(ErrorCode::BadBytes, || "Cannot convert value to utf8")?;
        v.parse::<NaiveDate>()
            .map_err_to_code(ErrorCode::BadBytes, || "Cannot parse value to Date type")
    }

    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTime<Tz>> {
        let mut buf = vec![0; DATE_TIME_LEN];
        self.read_exact(buf.as_mut_slice())?;

        let v = std::str::from_utf8(buf.as_slice())
            .map_err_to_code(ErrorCode::BadBytes, || "Cannot convert value to utf8")?;
        let res = tz
            .datetime_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
            .map_err_to_code(ErrorCode::BadBytes, || {
                format!("Cannot parse value:{:?} to DateTime type", v)
            })?;

        if self.ignore_byte(b'.')? {
            buf.clear();
            let size = self.keep_read(&mut buf, |f| (b'0'..=b'9').contains(&f))?;
            let scales: i64 = lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();

            let res = if size >= 9 {
                res.checked_add_signed(Duration::nanoseconds(scales))
            } else if size >= 6 {
                res.checked_add_signed(Duration::microseconds(scales))
            } else if size >= 3 {
                res.checked_add_signed(Duration::milliseconds(scales))
            } else {
                Some(res)
            };
            Ok(res.unwrap())
        } else {
            Ok(res)
        }
    }
}
