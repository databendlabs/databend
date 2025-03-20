// Copyright 2021 Datafuse Labs
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

const ZZ: u8 = b'0'; // \x00
const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const BS: u8 = b'\\'; // \x5C
const __: u8 = 0xff;

// Lookup table of escape sequences.
// A value of __ means that byte i is not escaped.
// A value of b'x' at index i means that byte i is escaped as "\x" in TSV.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    ZZ, __, __, __, __, __, __, __, BB, TT, NN, __, FF, RR, __, __, // 0
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 1
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

pub fn write_quoted_string(bytes: &[u8], buf: &mut Vec<u8>, quote: u8) {
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if byte == quote {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(quote);
            buf.push(quote);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
}

pub fn write_tsv_escaped_string(bytes: &[u8], buf: &mut Vec<u8>, field_delimiter: u8) {
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = ESCAPE[byte as usize];
        if escape == __ {
            if byte == field_delimiter {
                if start < i {
                    buf.extend_from_slice(&bytes[start..i]);
                }
                buf.push(b'\\');
                buf.push(byte);
                start = i + 1;
            }
        } else {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(b'\\');
            buf.push(escape);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
}

#[cfg(test)]
mod test {
    use crate::field_encoder::helpers::write_json_string;
    use crate::field_encoder::helpers::write_tsv_escaped_string;
    use crate::field_encoder::write_csv_string;

    #[test]
    fn test_escape() {
        {
            let mut buf = vec![];
            write_tsv_escaped_string(b"\0\n\r\t\\\'", &mut buf, b'\t');
            assert_eq!(&buf, b"\\0\\n\\r\\t\\\\\'")
        }

        {
            let mut buf = vec![];
            write_tsv_escaped_string(b"\n123\n456\n", &mut buf, b'\t');
            assert_eq!(&buf, b"\\n123\\n456\\n")
        }

        {
            let mut buf = vec![];
            write_tsv_escaped_string(b"123\n", &mut buf, b'\t');
            assert_eq!(&buf, b"123\\n")
        }

        {
            let mut buf = vec![];
            write_tsv_escaped_string(b"\n123", &mut buf, b'\t');
            assert_eq!(&buf, b"\\n123")
        }

        {
            let mut buf = vec![];
            write_tsv_escaped_string(b"\n,23", &mut buf, b',');
            assert_eq!(&buf, b"\\n\\,23")
        }
    }

    #[test]
    fn test_json_escape() {
        let basic = b"\0\n\r\t\'/\"\\";
        {
            let mut buf = vec![];
            write_json_string(basic, &mut buf, false, true);
            assert_eq!(&buf, b"\\u0000\\n\\r\\t\'\\/\\\"\\\\")
        }

        {
            let mut buf = vec![];
            write_json_string(basic, &mut buf, false, false);
            assert_eq!(&buf, b"\\u0000\\n\\r\\t\'/\\\"\\\\")
        }

        {
            let mut buf = vec![];
            write_json_string(basic, &mut buf, true, true);
            assert_eq!(&buf, b"\\u0000\\n\\r\\t\'\\/\"\"\\\\")
        }

        {
            let mut buf = vec![];
            write_json_string(b"\n123\n456\n", &mut buf, true, true);
            assert_eq!(&buf, b"\\n123\\n456\\n")
        }

        {
            let mut buf = vec![];
            write_json_string(b"123\n", &mut buf, true, true);
            assert_eq!(&buf, b"123\\n")
        }

        {
            let mut buf = vec![];
            write_json_string(b"\n123", &mut buf, true, true);
            assert_eq!(&buf, b"\\n123")
        }

        {
            let mut buf = vec![];
            write_json_string(b"\n123", &mut buf, true, true);
            assert_eq!(&buf, b"\\n123")
        }

        {
            let s = "123\u{2028}\u{2029}abc";
            let mut buf = vec![];
            write_json_string(s.as_bytes(), &mut buf, true, true);
            assert_eq!(&buf, b"123\\u2028\\u2029abc")
        }
    }

    #[test]
    fn test_csv_string() {
        {
            let s = "a\"\nb";
            let mut buf = vec![];
            write_csv_string(s.as_bytes(), &mut buf, b'"');
            assert_eq!(&buf, b"\"a\"\"\nb\"")
        }
    }
}
