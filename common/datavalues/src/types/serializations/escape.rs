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

const ESCAPES: [u8; 7] = [0x08, 0x0c, b'\n', b'\r', b'\t', b'\0', b'\\'];

fn find_first(values: &[u8], quote: u8) -> usize {
    for (i, v) in values.iter().enumerate() {
        for e in ESCAPES.iter() {
            if v == e {
                return i;
            }
        }
        if *v == quote {
            return i;
        }
    }
    return values.len();
}

pub fn write_escaped_string(values: &[u8], buf: &mut Vec<u8>, quote: u8) {
    let size = values.len();
    let first_pos = find_first(values, quote);
    if first_pos >= size {
        buf.extend_from_slice(values);
    } else {
        buf.extend_from_slice(&values[0..first_pos]);
        for v in &values[first_pos..size] {
            let e = match v {
                0x08 => b'b',
                0x0c => b'f',
                b'\t' => b't',
                b'\n' => b'n',
                b'\r' => b'r',
                b'\\' => b'\\',
                b'\0' => b'0',
                _ => 0u8,
            };
            if e != 0u8 {
                buf.push(b'\\');
                buf.push(e);
            } else if e == quote {
                buf.push(b'\\');
                buf.push(*v);
            } else {
                buf.push(*v);
            }
        }
    }
}
