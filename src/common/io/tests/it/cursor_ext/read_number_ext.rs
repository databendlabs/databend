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

use common_exception::Result;
use common_io::cursor_ext::*;

#[test]
fn test_read_int() -> Result<()> {
    let mut reader = Cursor::new("3,032,+2,-23,".as_bytes());
    let expected = vec![3, 32, 2, -23];
    let mut res = vec![];
    for _ in 0..expected.len() {
        println!("{}", String::from_utf8_lossy(reader.remaining_slice()));
        res.push(reader.read_int_text::<i32>()?);
        println!("{}", String::from_utf8_lossy(reader.remaining_slice()));
        assert!(reader.ignore_byte(b','));
    }
    assert_eq!(res, expected);

    let mut reader = Cursor::new("00000789.0,".as_bytes());
    assert!(reader.read_int_text::<i32>().is_err());
    Ok(())
}

#[test]
fn test_read_float() -> Result<()> {
    let mut reader = Cursor::new(
        "3,32,789.2,+2,-2.33333,-23,1.903583017e+9,1.903583017e9,1.903583017e-9".as_bytes(),
    );
    let expected = vec![
        3.0,
        32.0,
        789.2,
        2.0,
        -2.33333,
        -23.0,
        1903583017.0,
        1903583017.0,
        1.903583017e-9,
    ];
    let mut res = vec![];
    for _ in 0..9 {
        res.push(reader.read_float_text::<f64>()?);
        let _ = reader.ignore_byte(b',');
    }

    assert_eq!(res, expected);
    Ok(())
}
