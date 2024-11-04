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

use databend_common_io::cursor_ext::*;

#[test]
fn test_read_ext() {
    let mut cursor = Cursor::new("1 bytes   helloworld".as_bytes());

    cursor.ignore_byte(b'1');
    cursor.ignore_white_spaces_or_comments();
    let bs = cursor.remaining_slice();
    assert_eq!(String::from_utf8_lossy(bs), "bytes   helloworld");

    let mut vec = vec![];
    cursor.until(b's', &mut vec);
    assert_eq!(
        String::from_utf8_lossy(cursor.remaining_slice()),
        "   helloworld"
    );
    assert_eq!(String::from_utf8_lossy(&vec), "bytes".to_string());

    let spaces = cursor.ignore_white_spaces_or_comments();
    assert!(spaces);
    assert_eq!(
        String::from_utf8_lossy(cursor.remaining_slice()),
        "helloworld"
    );

    let mut cursor = Cursor::new("1 bytes   helloworld".as_bytes());

    cursor.ignore_byte(b'1');
    cursor.ignore_white_spaces_or_comments();
    let bs = cursor.remaining_slice();
    assert_eq!(String::from_utf8_lossy(bs), "bytes   helloworld");
}
