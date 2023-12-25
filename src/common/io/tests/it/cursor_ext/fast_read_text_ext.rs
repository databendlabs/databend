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

use std::collections::VecDeque;
use std::io::Cursor;

use aho_corasick::AhoCorasick;
use databend_common_exception::Result;
use databend_common_io::cursor_ext::*;

#[test]
fn test_positions() -> Result<()> {
    let cases = vec![
        (r#"abc"#.to_string(), vec![]),
        (r#"'abcdefg'"#.to_string(), vec![0, 8]),
        (r#"'abc\d'e'"#.to_string(), vec![0, 4, 6, 8]),
        (r#"'abc','d\'ef','g\\\'hi'"#.to_string(), vec![
            0, 4, 6, 8, 9, 12, 14, 16, 17, 18, 19, 22,
        ]),
    ];

    let patterns = &["'", "\\"];
    let ac = AhoCorasick::new(patterns).unwrap();
    for (data, expect) in cases {
        let mut positions = VecDeque::new();
        for mat in ac.find_iter(&data) {
            let pos = mat.start();
            positions.push_back(pos);
        }
        assert_eq!(positions, expect)
    }
    Ok(())
}

#[test]
fn test_fast_read_text() -> Result<()> {
    let data = r#"'abc','d\'ef','g\\\'hi'"#.to_string();
    let patterns = &["'", "\\"];
    let ac = AhoCorasick::new(patterns).unwrap();
    let mut positions = VecDeque::new();
    for mat in ac.find_iter(&data) {
        let pos = mat.start();
        positions.push_back(pos);
    }

    let mut reader = Cursor::new(data.as_bytes());
    let expected = vec![
        "abc".as_bytes().to_vec(),
        "d'ef".as_bytes().to_vec(),
        "g\\'hi".as_bytes().to_vec(),
    ];
    let mut res = vec![];
    for i in 0..expected.len() {
        if i > 0 {
            assert!(reader.ignore_byte(b','));
        }
        let mut buf = vec![];
        reader.fast_read_quoted_text(&mut buf, &mut positions)?;
        res.push(buf);
    }
    assert_eq!(res, expected);
    Ok(())
}
