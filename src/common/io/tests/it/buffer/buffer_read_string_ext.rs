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

use common_exception::Result;
use common_io::prelude::*;

#[test]
fn test_read_csv_string() -> Result<()> {
    let settings = FormatSettings {
        record_delimiter: Vec::new(),
        ..FormatSettings::default()
    };
    let s = "1,a,2\n3\r\n\"4\",\"\"\"5\",\"{\\\"\"second\\\"\" : 33}\",";
    let mut reader = BufferReader::new(s.as_bytes());

    let mut buf = Vec::new();

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "1".as_bytes());
    reader.must_ignore_byte(b',')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "a".as_bytes());
    reader.must_ignore_byte(b',')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "2".as_bytes());
    reader.must_ignore_byte(b'\n')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "3".as_bytes());
    reader.must_ignore_byte(b'\r')?;
    reader.must_ignore_byte(b'\n')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "4".as_bytes());
    reader.must_ignore_byte(b',')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "\"5".as_bytes());
    reader.must_ignore_byte(b',')?;

    buf.clear();
    reader.read_csv_string(&mut buf, &settings)?;
    assert_eq!(&buf, "{\\\"second\\\" : 33}".as_bytes());
    reader.must_ignore_byte(b',')?;

    assert!(reader.eof()?);
    Ok(())
}
