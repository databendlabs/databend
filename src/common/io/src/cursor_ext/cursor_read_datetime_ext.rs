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

use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::FixedOffset;
use chrono::LocalResult;
use chrono::MappedLocalTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::Offset;
use chrono::TimeZone;
use chrono_tz::Tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;

use crate::cursor_ext::cursor_read_bytes_ext::ReadBytesExt;

pub enum DateTimeResType {
    Datetime(DateTime<Tz>),
    Date(NaiveDate),
}

pub trait BufferReadDateTimeExt {
    fn read_date_text(&mut self, tz: &Tz, enable_dst_hour_fix: bool) -> Result<NaiveDate>;
    fn read_timestamp_text(
        &mut self,
        tz: &Tz,
        only_date_text: bool,
        enable_dst_hour_fix: bool,
    ) -> Result<DateTimeResType>;
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

fn parse_time_part(buf: &[u8], size: usize) -> Result<u32> {
    if size > 0 && size < 3 {
        Ok(lexical_core::FromLexical::from_lexical(buf).unwrap())
    } else {
        let msg = format!(
            "err with parse time part. Format like this:[03:00:00], got {} digits",
            size
        );
        Err(ErrorCode::BadBytes(msg))
    }
}

impl<T> BufferReadDateTimeExt for Cursor<T>
where T: AsRef<[u8]>
{
    fn read_date_text(&mut self, tz: &Tz, enable_dst_hour_fix: bool) -> Result<NaiveDate> {
        // TODO support YYYYMMDD format
        // Also need to consider enable_dst_hour_fix, to_date('1947-04-15 00:00:00')
        self.read_timestamp_text(tz, true, enable_dst_hour_fix)
            .map(|dt| match dt {
                DateTimeResType::Datetime(dt) => dt.naive_local().date(),
                DateTimeResType::Date(nd) => nd,
            })
    }

    fn read_timestamp_text(
        &mut self,
        tz: &Tz,
        only_date_text: bool,
        enable_dst_hour_fix: bool,
    ) -> Result<DateTimeResType> {
        // Date Part YYYY-MM-DD
        let mut buf = vec![0; DATE_LEN];
        self.read_exact(buf.as_mut_slice())?;
        let mut v =
            std::str::from_utf8(buf.as_slice()).map_err_to_code(ErrorCode::BadBytes, || {
                format!(
                    "UTF-8 Conversion Failed: Unable to convert value {:?} to UTF-8",
                    buf
                )
            })?;

        // convert zero date to `1970-01-01`
        if v == "0000-00-00" {
            v = "1970-01-01";
        }
        let d = v
            .parse::<NaiveDate>()
            .map_err_to_code(ErrorCode::BadBytes, || {
                format!(
                    "Date Parsing Error: The value '{}' could not be parsed into a valid Date",
                    v
                )
            })?;

        let less_1000 = |dt: DateTime<Tz>| {
            // convert timestamp less than `1000-01-01 00:00:00` to `1000-01-01 00:00:00`
            if dt.year() < 1000 {
                tz.from_utc_datetime(
                    &NaiveDate::from_ymd_opt(1000, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                )
            } else {
                dt
            }
        };

        // Time Part
        buf.clear();
        if self.ignore(|b| b == b' ' || b == b'T') {
            // HH:mm:ss
            let mut buf = Vec::with_capacity(2);
            let mut times = Vec::with_capacity(3);
            loop {
                buf.clear();
                let size = self.keep_read(&mut buf, |f| f.is_ascii_digit());
                if size == 0 {
                    break;
                } else {
                    let time = parse_time_part(&buf, size)?;
                    times.push(time);
                    if times.len() == 3 {
                        break;
                    }
                    self.ignore_byte(b':');
                }
            }
            // Time part is HH:MM or HH or empty
            // Examples: '2022-02-02T', '2022-02-02 ', '2022-02-02T02', '2022-02-02T3:', '2022-02-03T03:13', '2022-02-03T03:13:'
            if times.len() < 3 {
                times.resize(3, 0);
                let dt = get_local_time(tz, &d, &mut times, enable_dst_hour_fix)?;
                return Ok(DateTimeResType::Datetime(less_1000(dt)));
            }

            let dt = get_local_time(tz, &d, &mut times, enable_dst_hour_fix)?;

            // ms .microseconds
            let dt = if self.ignore_byte(b'.') {
                buf.clear();
                let size = self.keep_read(&mut buf, |f| f.is_ascii_digit());
                if size == 0 {
                    return Err(ErrorCode::BadBytes(
                        "Microsecond Parsing Error: Expecting a format like [.123456] for microseconds part",
                    ));
                }
                let mut scales: i64 =
                    lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();
                if size <= 9 {
                    scales *= 10_i64.pow(9 - size as u32)
                } else {
                    scales /= (size as i64 - 9) * 10
                }
                dt.checked_add_signed(Duration::nanoseconds(scales))
                    .unwrap()
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
            if self.ignore(|b| b == b'z' || b == b'Z') {
                // ISO 8601 The Z on the end means UTC (that is, an offset-from-UTC of zero hours-minutes-seconds).
                if dt.year() < 1000 {
                    Ok(DateTimeResType::Datetime(
                        tz.from_utc_datetime(
                            &NaiveDate::from_ymd_opt(1000, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        ),
                    ))
                } else {
                    let current_tz = dt.offset().fix().local_minus_utc();
                    Ok(DateTimeResType::Datetime(calc_offset(
                        current_tz.into(),
                        0,
                        &dt,
                    )?))
                }
            } else if self.ignore_byte(b'+') {
                Ok(DateTimeResType::Datetime(self.parse_time_offset(
                    tz,
                    &mut buf,
                    &dt,
                    false,
                    calc_offset,
                )?))
            } else if self.ignore_byte(b'-') {
                Ok(DateTimeResType::Datetime(self.parse_time_offset(
                    tz,
                    &mut buf,
                    &dt,
                    true,
                    calc_offset,
                )?))
            } else {
                // only datetime part
                Ok(DateTimeResType::Datetime(less_1000(dt)))
            }
        } else {
            // only date part
            if d.year() < 1000 {
                Ok(DateTimeResType::Datetime(
                    tz.from_utc_datetime(
                        &NaiveDate::from_ymd_opt(1000, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    ),
                ))
            } else {
                let res = tz.from_local_datetime(&d.and_hms_opt(0, 0, 0).unwrap());
                match res {
                    LocalResult::Single(t) => Ok(DateTimeResType::Datetime(t)),
                    LocalResult::Ambiguous(t1, t2) => Err(ErrorCode::BadBytes(format!(
                        "Ambiguous Local Time: The local time is ambiguous, with possible times ranging from {:?} to {:?}",
                        t1, t2
                    ))),
                    LocalResult::None => {
                        // like to_date('1941-03-15') => 1941-03-15 00:00:00 in Asia/Shanghai is not exists
                        // but if just convert to Date, it should can return NaiveDate
                        if only_date_text {
                            Ok(DateTimeResType::Date(d))
                        } else {
                            // Now add a setting enable_dst_hour_fix to control this behavior. If true, try to add a hour.
                            if let Some(naive_datetime) = &d.and_hms_opt(1, 0, 0) {
                                return Ok(DateTimeResType::Datetime(unwrap_local_time(
                                    tz,
                                    enable_dst_hour_fix,
                                    naive_datetime,
                                )?));
                            }
                            Err(ErrorCode::BadBytes(format!(
                                "Local Time Error: No valid local time found for timezone '{:?}' with date '{}'",
                                tz, d
                            )))
                        }
                    }
                }
            }
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
        fn get_hour_minute_offset(
            tz: &Tz,
            dt: &DateTime<Tz>,
            west_tz: bool,
            calc_offset: &impl Fn(i64, i64, &DateTime<Tz>) -> Result<DateTime<Tz>>,
            hour_offset: i32,
            minute_offset: i32,
        ) -> Result<DateTime<Tz>, ErrorCode> {
            if (hour_offset == 14 && minute_offset == 0)
                || ((0..60).contains(&minute_offset) && hour_offset < 14)
            {
                if dt.year() < 1970 {
                    Ok(tz.from_utc_datetime(
                        &NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    ))
                } else {
                    let current_tz_sec = dt.offset().fix().local_minus_utc();
                    let mut val_tz_sec =
                        FixedOffset::east_opt(hour_offset * 3600 + minute_offset * 60)
                            .unwrap()
                            .local_minus_utc();
                    if west_tz {
                        val_tz_sec = -val_tz_sec;
                    }
                    calc_offset(current_tz_sec.into(), val_tz_sec.into(), dt)
                }
            } else {
                Err(ErrorCode::BadBytes(format!(
                    "Invalid Timezone Offset: The minute offset '{}' is outside the valid range. Expected range is [00-59] within a timezone gap of [-14:00, +14:00]",
                    minute_offset
                )))
            }
        }
        let n = self.keep_read(buf, |f| f.is_ascii_digit());
        match n {
            2 => {
                let hour_offset: i32 =
                    lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();
                if (0..15).contains(&hour_offset) {
                    buf.clear();
                    if self.ignore_byte(b':') {
                        if self.keep_read(buf, |f| f.is_ascii_digit()) != 2 {
                            // +08[other byte]00 will err in there, e.g. +08-00
                            return Err(ErrorCode::BadBytes(
                                "Timezone Parsing Error: Incorrect format in hour part. The time zone format must conform to the ISO 8601 standard",
                            ));
                        }
                        let minute_offset: i32 =
                            lexical_core::FromLexical::from_lexical(buf.as_slice()).unwrap();
                        // max utc: 14:00, min utc: 00:00
                        get_hour_minute_offset(
                            tz,
                            dt,
                            west_tz,
                            &calc_offset,
                            hour_offset,
                            minute_offset,
                        )
                    } else {
                        get_hour_minute_offset(tz, dt, west_tz, &calc_offset, hour_offset, 0)
                    }
                } else {
                    Err(ErrorCode::BadBytes(format!(
                        "Invalid Timezone Offset: The hour offset '{}' is outside the valid range. Expected range is [00-14] within a timezone gap of [-14:00, +14:00]",
                        hour_offset
                    )))
                }
            }
            4 => {
                let hour_offset = &buf.as_slice()[..2];
                let hour_offset: i32 =
                    lexical_core::FromLexical::from_lexical(hour_offset).unwrap();
                let minute_offset = &buf.as_slice()[2..];
                let minute_offset: i32 =
                    lexical_core::FromLexical::from_lexical(minute_offset).unwrap();
                buf.clear();
                // max utc: 14:00, min utc: 00:00
                if (0..15).contains(&hour_offset) {
                    get_hour_minute_offset(
                        tz,
                        dt,
                        west_tz,
                        &calc_offset,
                        hour_offset,
                        minute_offset,
                    )
                } else {
                    Err(ErrorCode::BadBytes(format!(
                        "Invalid Timezone Offset: The hour offset '{}' is outside the valid range. Expected range is [00-14] within a timezone gap of [-14:00, +14:00]",
                        hour_offset
                    )))
                }
            }
            _ => Err(ErrorCode::BadBytes(
                "Timezone Parsing Error: Incorrect format. The time zone format must conform to the ISO 8601 standard",
            )),
        }
    }
}

// Can not directly unwrap, because of DST.
// e.g.
// set timezone='Europe/London';
// -- if unwrap() will cause session panic.
// -- https://github.com/chronotope/chrono/blob/v0.4.24/src/offset/mod.rs#L186
// select to_date(to_timestamp('2021-03-28 01:00:00'));
// Now add a setting enable_dst_hour_fix to control this behavior. If true, try to add a hour.
fn get_local_time(
    tz: &Tz,
    d: &NaiveDate,
    times: &mut Vec<u32>,
    enable_dst_hour_fix: bool,
) -> Result<DateTime<Tz>> {
    d.and_hms_opt(times[0], times[1], times[2])
        .map(|naive_datetime| unwrap_local_time(tz, enable_dst_hour_fix, &naive_datetime))
        .transpose()?
        .ok_or_else(|| ErrorCode::BadBytes(format!("Invalid time provided in times: {:?}", times)))
}

fn unwrap_local_time(
    tz: &Tz,
    enable_dst_hour_fix: bool,
    naive_datetime: &NaiveDateTime,
) -> Result<DateTime<Tz>> {
    match tz.from_local_datetime(naive_datetime) {
        LocalResult::Single(t) => Ok(t),
        LocalResult::Ambiguous(t1, t2) => Err(ErrorCode::BadBytes(format!(
            "Ambiguous Local Time: The local time is ambiguous {}, {} with timezone {}",
            t1, t2, tz
        ))),
        LocalResult::None => {
            if enable_dst_hour_fix {
                if let Some(res2) = naive_datetime.checked_add_signed(Duration::seconds(3600)) {
                    return match tz.from_local_datetime(&res2) {
                        MappedLocalTime::Single(t) => Ok(t),
                        MappedLocalTime::Ambiguous(t1, t2) => {
                            Err(ErrorCode::BadArguments(format!(
                                "Ambiguous Local Time: The local time is ambiguous {}, {} with timezone {}",
                                t1, t2, tz
                            )))
                        }
                        MappedLocalTime::None => Err(ErrorCode::BadArguments(format!(
                            "Local Time Error: The local time {}, {} can not map to a single unique result with timezone {}",
                            naive_datetime, res2, tz
                        ))),
                    };
                }
            }
            Err(ErrorCode::BadArguments(format!(
                "The time {} can not map to a single unique result with timezone {}",
                naive_datetime, tz
            )))
        }
    }
}
