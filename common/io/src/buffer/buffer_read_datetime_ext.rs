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
use chrono::Datelike;
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

        let v = std::str::from_utf8(buf.as_slice()).map_err_to_code(ErrorCode::BadBytes, || {
            format!("Cannot convert value:{:?} to utf8", buf)
        })?;
        // convert zero date to `1970-01-01`
        if v == "0000-00-00" {
            return Ok(NaiveDate::from_ymd(1970, 1, 1));
        }
        let d = v
            .parse::<NaiveDate>()
            .map_err_to_code(ErrorCode::BadBytes, || {
                format!("Cannot parse value:{} to Date type", v)
            })?;
        // convert date less than `1000-01-01` to `1000-01-01`
        if d.year() < 1000 {
            return Ok(NaiveDate::from_ymd(1000, 1, 1));
        }
        Ok(d)
    }

    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTime<Tz>> {
        let mut buf = vec![0; DATE_TIME_LEN];
        self.read_exact(buf.as_mut_slice())?;

        let v = std::str::from_utf8(buf.as_slice())
            .map_err_to_code(ErrorCode::BadBytes, || "Cannot convert value to utf8")?;
        // convert zero timestamp to `1970-01-01 00:00:00`
        let res = if v == "0000-00-00 00:00:00" {
            tz.from_utc_datetime(&NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0))
        } else {
            tz.datetime_from_str(v, "%Y-%m-%d %H:%M:%S")
                .map_err_to_code(ErrorCode::BadBytes, || {
                    format!("Cannot parse value:{:?} to DateTime type", v)
                })?
        };

        self.ignore(|b| b == b'z' || b == b'Z')?;
        let dt = if self.ignore_byte(b'.')? {
            buf.clear();
            let size = self.keep_read(&mut buf, |f| (b'0'..=b'9').contains(&f))?;
            let scales: i64 = lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();

            if size >= 9 {
                res.checked_add_signed(Duration::nanoseconds(scales))
                    .unwrap()
            } else if size >= 6 {
                res.checked_add_signed(Duration::microseconds(scales))
                    .unwrap()
            } else if size >= 3 {
                res.checked_add_signed(Duration::milliseconds(scales))
                    .unwrap()
            } else {
                res
            }
        } else {
            res
        };
        // convert timestamp less than `1000-01-01 00:00:00` to `1000-01-01 00:00:00`
        if dt.year() < 1000 {
            Ok(tz.from_utc_datetime(&NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0)))
        } else {
            Ok(dt)
        }
    }
}
