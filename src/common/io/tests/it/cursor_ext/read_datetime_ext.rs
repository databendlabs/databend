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

use databend_common_exception::Result;
use databend_common_io::cursor_ext::*;
use jiff::tz::TimeZone;

#[test]
fn test_read_timestamp_text() -> Result<()> {
    let mut reader = Cursor::new(
        "2023-12-25T02:31:07.485281+0545,2023-12-25T02:31:07.485281-0545,2023-12-25T02:31:07.485281+05,2023-12-25T02:31:07.485281-05,2009-01-01 00:00:00.12,2009-01-01 00:00:00.1234,2009-01-01 00:00:00.1234567891,2022-02-02T,2022-02-02 12,2022-02-02T13:4:,2022-02-02 12:03,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00".as_bytes(),
    );
    let tz = TimeZone::UTC;
    let expected = [
        "2023-12-24T20:46:07.485281+00:00[UTC]",
        "2023-12-25T08:16:07.485281+00:00[UTC]",
        "2023-12-24T21:31:07.485281+00:00[UTC]",
        "2023-12-25T07:31:07.485281+00:00[UTC]",
        "2009-01-01T00:00:00.12+00:00[UTC]",
        "2009-01-01T00:00:00.1234+00:00[UTC]",
        "2009-01-01T00:00:00.123456789+00:00[UTC]",
        "2022-02-02T00:00:00+00:00[UTC]",
        "2022-02-02T12:00:00+00:00[UTC]",
        "2022-02-02T13:04:00+00:00[UTC]",
        "2022-02-02T12:03:00+00:00[UTC]",
        "2023-03-03T00:00:00+00:00[UTC]",
        "2022-02-02T00:00:00+00:00[UTC]",
        "2009-01-01T03:02:01.123+00:00[UTC]",
        "2009-01-01T00:00:00+00:00[UTC]",
        "2009-01-01T00:00:00.123+00:00[UTC]",
        "2009-01-01T00:00:00.123456+00:00[UTC]",
        "0002-03-03T00:01:02+00:00[UTC]",
        "2022-03-03T16:01:02+00:00[UTC]",
        "2022-03-04T08:01:02+00:00[UTC]",
        "1970-01-01T00:00:00+00:00[UTC]",
        "1970-01-01T00:00:00+00:00[UTC]",
        "0001-01-01T00:00:00+00:00[UTC]",
        "2020-01-01T11:11:11+00:00[UTC]",
        "2009-01-03T00:00:00+00:00[UTC]",
        "2020-01-01T11:11:11.123+00:00[UTC]",
        "2055-02-03T02:00:20.234+00:00[UTC]",
        "2055-02-03T18:00:20.234+00:00[UTC]",
        "1022-05-15T19:25:02+00:00[UTC]",
    ];
    let mut res = vec![];
    for _ in 0..expected.len() {
        let time = reader.read_timestamp_text(&tz)?;
        if let DateTimeResType::Datetime(time) = time {
            res.push(format!("{:?}", time));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected);
    Ok(())
}

#[test]
fn test_read_dst_timestamp_text() -> Result<()> {
    let mut reader = Cursor::new("1947-04-15 01:00:00,1990-09-16 01:00:00".as_bytes());
    let tz = TimeZone::get("Asia/Shanghai").unwrap();
    let expected = [
        "\"1947-04-15T01:00:00+09:00[Asia/Shanghai]\"",
        "\"1990-09-16T01:00:00+09:00[Asia/Shanghai]\"",
    ];
    let mut res = vec![];
    for _ in 0..expected.len() {
        let time = reader.read_timestamp_text(&tz)?;
        if let DateTimeResType::Datetime(time) = time {
            res.push(format!("{:?}", time.to_string()));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected);

    reader = Cursor::new("1990-09-16 01:00:00".as_bytes());
    let expected2 = ["\"1990-09-16T01:00:00+09:00[Asia/Shanghai]\""];
    let mut res = vec![];
    for _ in 0..expected2.len() {
        let time = reader.read_timestamp_text(&tz)?;
        if let DateTimeResType::Datetime(time) = time {
            res.push(format!("{:?}", time.to_string()));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected2);
    Ok(())
}

#[test]
fn test_read_date_text() -> Result<()> {
    let mut reader = Cursor::new("2009-01-01,1000-01-01,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00,2055-01-01".as_bytes());
    let tz = TimeZone::get("UTC").unwrap();
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
        let date = reader.read_date_text(&tz)?;
        res.push(format!("{:?}", date));
        let _ = reader.ignore_byte(b',');
    }
    assert_eq!(res, expected);
    Ok(())
}
