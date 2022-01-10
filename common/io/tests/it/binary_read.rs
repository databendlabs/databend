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

use std::io::Cursor;

use common_exception::Result;
use common_io::prelude::*;

#[test]
fn test_write_and_read() -> Result<()> {
    //impl Write for Cursor<&mut [u8]>
    let mut buffer = vec![0u8; 1024];
    let mut buff: Cursor<&mut [u8]> = Cursor::new(buffer.as_mut());

    buff.write_scalar(&8u8)?;
    buff.write_scalar(&16u16)?;
    buff.write_scalar(&32u32)?;
    buff.write_scalar(&'🐳')?;
    buff.write_string("33")?;
    buff.write_opt_scalar(&Some(64i64))?;
    buff.write_opt_scalar::<u8>(&None)?;
    buff.write_uvarint(1024u64)?;

    let mut read = Cursor::new(buffer);
    let res: u8 = read.read_scalar().unwrap();
    assert_eq!(res, 8);

    let res: u16 = read.read_scalar().unwrap();
    assert_eq!(res, 16);

    let res: u32 = read.read_scalar().unwrap();
    assert_eq!(res, 32);

    let res: char = read.read_scalar().unwrap();
    assert_eq!(res, '🐳');

    let res = read.read_string().unwrap();
    assert_eq!(res, "33");

    let res: Option<i64> = read.read_opt_scalar().unwrap();
    assert_eq!(res, Some(64));

    let res: Option<u8> = read.read_opt_scalar().unwrap();
    assert_eq!(res, None);

    let res: u64 = read.read_uvarint().unwrap();
    assert_eq!(res, 1024);

    Ok(())
}
