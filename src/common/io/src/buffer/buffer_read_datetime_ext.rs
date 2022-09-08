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
use chrono::FixedOffset;
use chrono::NaiveDate;
use chrono::Offset;
use chrono::TimeZone;
use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;

use super::BufferRead;
use super::BufferReadExt;

pub trait BufferReadDateTimeExt: BufferRead {
    fn read_date_text(&mut self, tz: &Tz) -> Result<NaiveDate>;
    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTime<Tz>>;
    fn parse_time_offset(
        &mut self,
        tz: &Tz,
        buf: &mut Vec<u8>,
        dt: &DateTime<Tz>,
        west_tz: bool,
        calc_offset: impl Fn(i64, i64, &DateTime<Tz>) -> Result<DateTime<Tz>>,
    ) -> Result<DateTime<Tz>>;
}

const DATE_LEN: usize = 10;

impl<R> BufferReadDateTimeExt for R
where R: BufferRead
{
    fn read_date_text(&mut self, tz: &Tz) -> Result<NaiveDate> {
        // TODO support YYYYMMDD format
        self.read_timestamp_text(tz).map(|dt| dt.naive_utc().date())
    }

    fn read_timestamp_text(&mut self, tz: &Tz) -> Result<DateTime<Tz>> {
        // Date Part YYYY-MM-DD
        let mut buf = vec![0; DATE_LEN];
        self.read_exact(buf.as_mut_slice())?;
        let mut v = std::str::from_utf8(buf.as_slice())
            .map_err_to_code(ErrorCode::BadBytes, || {
                format!("Cannot convert value:{:?} to utf8", buf)
            })?;

        // convert zero date to `1970-01-01`
        if v == "0000-00-00" {
            v = "1970-01-01";
        }
        let d = v
            .parse::<NaiveDate>()
            .map_err_to_code(ErrorCode::BadBytes, || {
                format!("Cannot parse value:{} to Date type", v)
            })?;
        let mut dt = tz.from_local_datetime(&d.and_hms(0, 0, 0)).unwrap();

        let less_1000 = |dt: DateTime<Tz>| {
            // convert timestamp less than `1000-01-01 00:00:00` to `1000-01-01 00:00:00`
            if dt.year() < 1000 {
                Ok(tz.from_utc_datetime(&NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0)))
            } else {
                Ok(dt)
            }
        };

        // Time Part
        buf.clear();
        if self.ignore(|b| b == b' ' || b == b'T').unwrap() {
            // HH:mm:ss
            let get_time = |buf: &mut Vec<u8>, size| {
                if size > 0 && size < 3 {
                    Ok(lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap())
                } else {
                    Err(ErrorCode::BadBytes(
                        "err with parse time part. Format like this:[03:00:00]",
                    ))
                }
            };
            let mut buf = Vec::with_capacity(2);
            let mut times = Vec::with_capacity(3);
            loop {
                buf.clear();
                let size = self.keep_read(&mut buf, |f| (b'0'..=b'9').contains(&f))?;
                if size == 0 {
                    break;
                } else {
                    let time = get_time(&mut buf, size)?;
                    times.push(time);
                    if times.len() == 3 {
                        break;
                    }
                    self.ignore_byte(b':')?;
                }
            }
            // Time part is HH:MM or HH or empty
            // Examples: '2022-02-02T', '2022-02-02 ', '2022-02-02T02', '2022-02-02T3:', '2022-02-03T03:13', '2022-02-03T03:13:'
            if times.len() < 3 {
                times.resize(3, 0);
                dt = tz
                    .from_local_datetime(&d.and_hms(times[0], times[1], times[2]))
                    .unwrap();
                return less_1000(dt);
            }

            dt = tz
                .from_local_datetime(&d.and_hms(times[0], times[1], times[2]))
                .unwrap();

            // ms .microseconds
            let dt = if self.ignore_byte(b'.')? {
                buf.clear();
                let size = self.keep_read(&mut buf, |f| (b'0'..=b'9').contains(&f))?;
                if size == 0 {
                    return Err(ErrorCode::BadBytes(
                        "err with parse micros second, format like this:[.123456]",
                    ));
                }
                let scales: i64 = lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();

                if size >= 9 {
                    dt.checked_add_signed(Duration::nanoseconds(scales))
                        .unwrap()
                } else if size >= 6 {
                    dt.checked_add_signed(Duration::microseconds(scales))
                        .unwrap()
                } else if size >= 3 {
                    dt.checked_add_signed(Duration::milliseconds(scales))
                        .unwrap()
                } else {
                    dt
                }
            } else {
                dt
            };

            // Timezone 2022-02-02T03:00:03.123[z/Z[+/-08:00]]
            buf.clear();
            let calc_offset = |current_tz_sec: i64, val_tz_sec: i64, dt: &DateTime<Tz>| {
                let offset = (current_tz_sec - val_tz_sec) * 1000 * 1000;
                let mut ts = dt.timestamp_micros();
                ts += offset;
                // TODO: need support timestamp_micros in chrono-0.4.22/src/offset/mod.rs
                // use like tz.timestamp_nanos()
                let (mut secs, mut micros) = (ts / 1_000_000, ts % 1_000_000);
                if ts < 0 {
                    secs -= 1;
                    micros += 1_000_000;
                }
                Ok(tz.timestamp_opt(secs, (micros as u32) * 1000).unwrap())
            };
            if self.ignore(|b| b == b'z' || b == b'Z')? {
                // ISO 8601 The Z on the end means UTC (that is, an offset-from-UTC of zero hours-minutes-seconds).
                if dt.year() < 1000 {
                    Ok(tz.from_utc_datetime(&NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0)))
                } else {
                    let current_tz = dt.offset().fix().local_minus_utc();
                    calc_offset(current_tz.into(), 0, &dt)
                }
            } else if self.ignore_byte(b'+')? {
                self.parse_time_offset(tz, &mut buf, &dt, false, calc_offset)
            } else if self.ignore_byte(b'-')? {
                self.parse_time_offset(tz, &mut buf, &dt, true, calc_offset)
            } else {
                // only datetime part
                less_1000(dt)
            }
        } else {
            // only date part
            less_1000(dt)
        }
    }

    // Only support HH:mm format
    fn parse_time_offset(
        &mut self,
        tz: &Tz,
        buf: &mut Vec<u8>,
        dt: &DateTime<Tz>,
        west_tz: bool,
        calc_offset: impl Fn(i64, i64, &DateTime<Tz>) -> Result<DateTime<Tz>>,
    ) -> Result<DateTime<Tz>> {
        if self.keep_read(buf, |f| (b'0'..=b'9').contains(&f))? != 2 {
            // +0800 will err in there
            return Err(ErrorCode::BadBytes(
                "err with parse timezone, format like this:[+08:00]",
            ));
        }
        let hour_offset: i32 = lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();
        if (0..15).contains(&hour_offset) {
            buf.clear();
            self.ignore_byte(b':')?;
            if self.keep_read(buf, |f| (b'0'..=b'9').contains(&f))? != 2 {
                // +08[other byte]00 will err in there, e.g. +08-00
                return Err(ErrorCode::BadBytes(
                    "err with parse timezone, format like this:[+08:00]",
                ));
            }
            let minute_offset: i32 =
                lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();
            // max utc: 14:00, min utc: 00:00
            if (hour_offset == 14 && minute_offset == 0)
                || ((0..60).contains(&minute_offset) && hour_offset < 14)
            {
                if dt.year() < 1970 {
                    Ok(tz.from_utc_datetime(&NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0)))
                } else {
                    let current_tz_sec = dt.offset().fix().local_minus_utc();
                    let mut val_tz_sec = FixedOffset::east(hour_offset * 3600 + minute_offset * 60)
                        .local_minus_utc();
                    if west_tz {
                        val_tz_sec = -val_tz_sec;
                    }
                    calc_offset(current_tz_sec.into(), val_tz_sec.into(), dt)
                }
            } else {
                Err(ErrorCode::BadBytes(format!(
                    "err with parse minute_offset:[{:?}], timezone gap: [-14:00,+14:00]",
                    minute_offset
                )))
            }
        } else {
            Err(ErrorCode::BadBytes(format!(
                "err with parse hour_offset:[{:?}], timezone gap: [-14:00,+14:00]",
                hour_offset
            )))
        }
    }
}
