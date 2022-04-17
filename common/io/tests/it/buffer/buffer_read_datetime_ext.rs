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

use chrono_tz::Tz;
use common_exception::Result;
use common_io::prelude::*;

#[test]
fn test_read_datetime_ext() -> Result<()> {
    let mut reader = BufferReader::new(
        "2009-01-01 00:00:00,2009-01-01 00:00:00.123,2009-01-01 00:00:00.123456".as_bytes(),
    );
    let tz = Tz::UTC;
    let expected = vec![
        "2009-01-01T00:00:00UTC",
        "2009-01-01T00:00:00.123UTC",
        "2009-01-01T00:00:00.123456UTC",
    ];
    let mut res = vec![];
    for _ in 0..expected.len() {
        let time = reader.read_datetime_text(&tz)?;
        res.push(format!("{:?}", time));
        let _ = reader.ignore_byte(b',')?;
    }
    assert_eq!(res, expected);

    let mut reader = BufferReader::new("2009-01-01,1000-01-01".as_bytes());

    let expected = vec!["2009-01-01", "1000-01-01"];

    let mut res = vec![];
    for _ in 0..expected.len() {
        let date = reader.read_date_text()?;
        res.push(format!("{:?}", date));
        let _ = reader.ignore_byte(b',')?;
    }
    assert_eq!(res, expected);
    Ok(())
}
