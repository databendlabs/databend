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

use chrono_tz::Tz;
use databend_common_exception::Result;
use databend_common_io::cursor_ext::*;

#[test]
fn test_read_timestamp_text() -> Result<()> {
    let mut reader = Cursor::new(
        "2023-12-25T02:31:07.485281+0545,2023-12-25T02:31:07.485281-0545,2023-12-25T02:31:07.485281+05,2023-12-25T02:31:07.485281-05,2009-01-01 00:00:00.12,2009-01-01 00:00:00.1234,2009-01-01 00:00:00.1234567891,2022-02-02T,2022-02-02 12,2022-02-02T13:4:,2022-02-02 12:03,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00".as_bytes(),
    );
    let tz = Tz::UTC;
    let expected = vec![
        "2023-12-24T20:46:07.485281UTC",
        "2023-12-25T08:16:07.485281UTC",
        "2023-12-24T21:31:07.485281UTC",
        "2023-12-25T07:31:07.485281UTC",
        "2009-01-01T00:00:00.120UTC",
        "2009-01-01T00:00:00.123400UTC",
        "2009-01-01T00:00:00.123456789UTC",
        "2022-02-02T00:00:00UTC",
        "2022-02-02T12:00:00UTC",
        "2022-02-02T13:04:00UTC",
        "2022-02-02T12:03:00UTC",
        "2023-03-03T00:00:00UTC",
        "2022-02-02T00:00:00UTC",
        "2009-01-01T03:02:01.123UTC",
        "2009-01-01T00:00:00UTC",
        "2009-01-01T00:00:00.123UTC",
        "2009-01-01T00:00:00.123456UTC",
        "1000-01-01T00:00:00UTC",
        "2022-03-03T16:01:02UTC",
        "2022-03-04T08:01:02UTC",
        "1970-01-01T00:00:00UTC",
        "1970-01-01T00:00:00UTC",
        "1000-01-01T00:00:00UTC",
        "2020-01-01T11:11:11UTC",
        "2009-01-03T00:00:00UTC",
        "2020-01-01T11:11:11.123UTC",
        "2055-02-03T02:00:20.234UTC",
        "2055-02-03T18:00:20.234UTC",
        "1970-01-01T00:00:00UTC",
    ];
    let mut res = vec![];
    for _ in 0..expected.len() {
        let time = reader.read_timestamp_text(&tz, false, false)?;
        if let DateTimeResType::Datetime(time) = time {
            res.push(format!("{:?}", time));
            reader.ignore_byte(b',');
        }
    }
    assert_eq!(res, expected);
    Ok(())
}

#[test]
fn test_read_date_text() -> Result<()> {
    let mut reader = Cursor::new("2009-01-01,1000-01-01,2023-03-03,2022-02-02,2009-01-01 3:2:1.123,2009-01-01 0:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456,0002-03-03T00:01:02,2022-03-04T00:01:02+08:00,2022-03-04T00:01:02-08:00,0000-00-00,0000-00-00 00:00:00,0001-01-01 00:00:00,2020-01-01T11:11:11Z,2009-01-03 00:00:00,2020-01-01T11:11:11.123Z,2055-02-03 10:00:20.234+08:00,2055-02-03 10:00:20.234-08:00,1022-05-16T03:25:02.000000+08:00,2055-01-01".as_bytes());
    let tz = Tz::UTC;

    let expected = vec![
        "2009-01-01",
        "1000-01-01",
        "2023-03-03",
        "2022-02-02",
        "2009-01-01",
        "2009-01-01",
        "2009-01-01",
        "2009-01-01",
        "1000-01-01",
        "2022-03-03",
        "2022-03-04",
        "1970-01-01",
        "1970-01-01",
        "1000-01-01",
        "2020-01-01",
        "2009-01-03",
        "2020-01-01",
        "2055-02-03",
        "2055-02-03",
        "1970-01-01",
        "2055-01-01",
    ];

    let mut res = vec![];
    for _ in 0..expected.len() {
        let date = reader.read_date_text(&tz, false)?;
        res.push(format!("{:?}", date));
        let _ = reader.ignore_byte(b',');
    }
    assert_eq!(res, expected);
    Ok(())
}
