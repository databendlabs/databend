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

const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const QU: u8 = b'"'; // \x22
const BS: u8 = b'\\'; // \x5C

const FS: u8 = b'/'; // \x2F
const UU: u8 = b'u'; // \x00...\x1F except the ones above
const E2: u8 = b'u'; // first byte of \u2028 and \u2029
const __: u8 = 0xff;

// Lookup table of escape sequences for JSON.
// A value of __ means that byte i is not escaped.
// A value of b'x' at index i means that byte i is escaped as "\x" in TSV.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
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
    __, __, E2, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

static ESCAPE_WITH_FS: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, FS, // 2
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
    __, __, E2, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

fn write_ascii_control(byte: u8, buf: &mut Vec<u8>) {
    static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
    let bytes = &[
        b'\\',
        b'u',
        b'0',
        b'0',
        HEX_DIGITS[(byte >> 4) as usize],
        HEX_DIGITS[(byte & 0xF) as usize],
    ];
    buf.extend_from_slice(bytes)
}

pub fn write_json_string(
    bytes: &[u8],
    buf: &mut Vec<u8>,
    quote_denormals: bool,
    escape_forward_slashes: bool,
) {
    let mut start = 0;
    let len = bytes.len();
    let table = if escape_forward_slashes {
        ESCAPE_WITH_FS
    } else {
        ESCAPE
    };

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = table[byte as usize];
        if escape == __ {
            continue;
        }

        if escape == QU {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            if quote_denormals {
                buf.push(QU);
            } else {
                buf.push(b'\\');
            }
            buf.push(QU);
            start = i + 1;
        } else if escape == E2 && len - i >= 3 && bytes[i + 1] == 0x80 {
            if bytes[i + 2] == 0xA8 || bytes[i + 2] == 0xA9 {
                if start < i {
                    buf.extend_from_slice(&bytes[start..i]);
                }
                if bytes[i + 2] == 0xA8 {
                    buf.extend_from_slice(b"\\u2028");
                } else {
                    buf.extend_from_slice(b"\\u2029");
                }
                start = i + 3;
            }
        } else {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }

            match escape {
                UU => write_ascii_control(byte, buf),
                _ => {
                    buf.push(b'\\');
                    buf.push(escape);
                }
            }
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
}
