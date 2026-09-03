// Copyright 2022 Datafuse Labs.
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

use chrono::NaiveDate;
use databend_common_io::cursor_ext::*;
use databend_common_timezone::Tz;
use databend_common_timezone::local_datetime_at;

/// Render microseconds as a local datetime in `tz`, for comparison against the
/// instants the parser is expected to produce.
fn format_micros(micros: i64, tz: &Tz) -> String {
    let (local, offset) = local_datetime_at(tz, micros.div_euclid(1_000_000)).unwrap();
    let subsec = micros.rem_euclid(1_000_000);
    let sign = if offset < 0 { '-' } else { '+' };
    let offset = offset.abs();
    format!(
        "{}.{:06}{sign}{:02}:{:02}",
        local.format("%Y-%m-%dT%H:%M:%S"),
        subsec,
        offset / 3600,
        (offset % 3600) / 60
    )
}

fn format_days(days: i32) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (epoch + chrono::TimeDelta::days(days as i64))
        .format("%Y-%m-%d")
        .to_string()
}

#[test]
fn test_read_timestamp_text() -> anyhow::Result<()> {
    let mut reader = Cursor::new(
        "2023-12-25T02:31:07.485281+0545,2023-12-25T02:31:07.485281-0545,2023-12-25T02:31:07.485281+05,2023-12-25T02:31:07.485281-05,2009-01-01 00:00:00.12,2009-01-01 00:00:00.1234,2009-01-01 00:00:00.1234567891,2022-02-02T,2022-02-02 12,2022-02-02T13:4:,2022-02-02 12:03,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00".as_bytes(),
    );
    let tz = Tz::UTC;

    // Databend timestamps carry microsecond precision, so fractions longer than
    // six digits are truncated rather than kept.
    let expected = [
        "2023-12-24T20:46:07.485281+00:00",
        "2023-12-25T08:16:07.485281+00:00",
        "2023-12-24T21:31:07.485281+00:00",
        "2023-12-25T07:31:07.485281+00:00",
        "2009-01-01T00:00:00.120000+00:00",
        "2009-01-01T00:00:00.123400+00:00",
        "2009-01-01T00:00:00.123456+00:00",
        "2022-02-02T00:00:00.000000+00:00",
        "2022-02-02T12:00:00.000000+00:00",
        "2022-02-02T13:04:00.000000+00:00",
        "2022-02-02T12:03:00.000000+00:00",
        "2023-03-03T00:00:00.000000+00:00",
        "2022-02-02T00:00:00.000000+00:00",
        "2009-01-01T03:02:01.123000+00:00",
        "2009-01-01T00:00:00.000000+00:00",
        "2009-01-01T00:00:00.123000+00:00",
        "2009-01-01T00:00:00.123456+00:00",
        "0002-03-03T00:01:02.000000+00:00",
        "2022-03-03T16:01:02.000000+00:00",
        "2022-03-04T08:01:02.000000+00:00",
        "1970-01-01T00:00:00.000000+00:00",
        "1970-01-01T00:00:00.000000+00:00",
        "0001-01-01T00:00:00.000000+00:00",
        "2020-01-01T11:11:11.000000+00:00",
        "2009-01-03T00:00:00.000000+00:00",
        "2020-01-01T11:11:11.123000+00:00",
        "2055-02-03T02:00:20.234000+00:00",
        "2055-02-03T18:00:20.234000+00:00",
        "1022-05-15T19:25:02.000000+00:00",
    ];

    let mut res = vec![];
    for _ in 0..expected.len() {
        if let DateTimeResType::Datetime(micros) = reader.read_timestamp_text(&tz)? {
            res.push(format_micros(micros, &tz));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected);
    Ok(())
}

#[test]
fn test_read_dst_timestamp_text() -> anyhow::Result<()> {
    let mut reader = Cursor::new("1947-04-15 01:00:00,1990-09-16 01:00:00".as_bytes());
    let tz = "Asia/Shanghai".parse::<Tz>().unwrap();

    // Both readings fall in a historical period when Shanghai observed DST.
    let expected = [
        "1947-04-15T01:00:00.000000+09:00",
        "1990-09-16T01:00:00.000000+09:00",
    ];
    let mut res = vec![];
    for _ in 0..expected.len() {
        if let DateTimeResType::Datetime(micros) = reader.read_timestamp_text(&tz)? {
            res.push(format_micros(micros, &tz));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected);

    let mut reader = Cursor::new("1990-09-16 01:00:00".as_bytes());
    let DateTimeResType::Datetime(micros) = reader.read_timestamp_text(&tz)? else {
        panic!("expected a timestamp");
    };
    assert_eq!(
        format_micros(micros, &tz),
        "1990-09-16T01:00:00.000000+09:00"
    );
    Ok(())
}

#[test]
fn test_read_date_text() -> anyhow::Result<()> {
    let mut reader = Cursor::new("2009-01-01,1000-01-01,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00,2055-01-01".as_bytes());
    let tz = "UTC".parse::<Tz>().unwrap();
    let expected = [
        "2009-01-01",
        "1000-01-01",
        "2023-03-03",
        "2022-02-02",
        "2009-01-01",
        "2009-01-01",
        "2009-01-01",
        "2009-01-01",
        "0002-03-03",
        "2022-03-04",
        "2022-03-04",
        "1970-01-01",
        "1970-01-01",
        "0001-01-01",
        "2020-01-01",
        "2009-01-03",
        "2020-01-01",
        "2055-02-03",
        "2055-02-03",
        "1022-05-16",
        "2055-01-01",
    ];

    let mut res = vec![];
    for _ in 0..expected.len() {
        let days = reader.read_date_text(&tz)?;
        res.push(format_days(days));
        let _ = reader.ignore_byte(b',');
    }
    assert_eq!(res, expected);
    Ok(())
}
