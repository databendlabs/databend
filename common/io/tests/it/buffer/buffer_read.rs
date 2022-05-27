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

use common_io::prelude::*;

#[test]
fn test_buf_read() {
    let mut buffer = BufferReader::new("1 bytes   helloworld".as_bytes());

    buffer.ignore_byte(b'1').unwrap();
    buffer.ignore_white_spaces().unwrap();
    let bs = buffer.buffer();
    assert_eq!(String::from_utf8_lossy(bs), "bytes   helloworld");

    let mut vec = vec![];
    buffer.until(b's', &mut vec).unwrap();
    assert_eq!(String::from_utf8_lossy(buffer.buffer()), "   helloworld");
    assert_eq!(String::from_utf8_lossy(&vec), "bytes".to_string());

    let spaces = buffer.ignore_white_spaces().unwrap();
    assert!(spaces);
    assert_eq!(String::from_utf8_lossy(buffer.buffer()), "helloworld");

    let mut buffer = MemoryReader::new("1 bytes   helloworld".as_bytes().to_vec());

    buffer.ignore_byte(b'1').unwrap();
    buffer.ignore_white_spaces().unwrap();
    let bs = buffer.fill_buf().unwrap();
    assert_eq!(String::from_utf8_lossy(bs), "bytes   helloworld");
}
